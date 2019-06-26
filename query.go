package pgsrv

import (
	"context"
	"database/sql/driver"
	"fmt"
	parser "github.com/lfittl/pg_query_go"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"github.com/panoplyio/pgsrv/protocol"
	"io"
)

type Cursor interface{}

type queryCursor struct {
	rows    driver.Rows
	columns []string
	row     []driver.Value
	strings []string
}

func (qc *queryCursor) read(n int, w protocol.MessageWriter) (count int, err error) {
	for count < n || n == 0 {
		err = qc.rows.Next(qc.row)
		if err != nil {
			break
		}

		// convert the values to string
		for i, v := range qc.row {
			qc.strings[i] = fmt.Sprintf("%v", v)
		}

		err = w.Write(protocol.DataRow(qc.strings))
		if err != nil {
			break
		}

		count++
	}

	if err == io.EOF {
		err = nil
	}
	return
}

type commandCursor struct {
	res driver.Result
}

type query struct {
	sess      Session
	transport *protocol.Transport
	queryer   Queryer
	execer    Execer
	sql       string
	ast       *parser.ParsetreeList
	numCols   int
}

func parseQuery(sql string) (*query, error) {
	// parse the query
	ast, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return &query{ast: &ast, sql: sql}, nil
}

func createQuery(sql string, stmts ...nodes.Node) *query {
	ast := parser.ParsetreeList{}
	for _, s := range stmts {
		ast.Statements = append(ast.Statements, s)
	}
	return &query{ast: &ast, sql: sql}
}

func (q *query) withTransport(transport *protocol.Transport) *query {
	q.transport = transport
	return q
}

func (q *query) withQueryer(queryer Queryer) *query {
	q.queryer = queryer
	return q
}

func (q *query) withExecer(execer Execer) *query {
	q.execer = execer
	return q
}

func (q *query) RunAndGetCursors() []Cursor {
	ctx := context.Background()
	ctx = context.WithValue(ctx, sqlCtxKey, q.sql)

	// execute all of the stmts
	for _, stmt := range q.ast.Statements {
		rawStmt, isRaw := stmt.(nodes.RawStmt)
		if isRaw {
			stmt = rawStmt.Stmt
		}

		// determine if it's a query or command
		switch v := stmt.(type) {
		case nodes.PrepareStmt:
			s, ok := q.sess.(*session)
			// only session implementation is capable of storing prepared stmts
			if ok {
				// we just store the statement and don't do anything
				s.storePreparedStatement(&v)
			}
		case nodes.SelectStmt, nodes.VariableShowStmt:
			err = q.Query(ctx, stmt)
		default:
			err = q.Exec(ctx, stmt)
		}

		if err != nil {
			return q.transport.Write(protocol.ErrorResponse(err))
		}
	}
	return nil
}

func (q *query) Query(ctx context.Context, n nodes.Node) (*queryCursor, error) {
	rows, err := q.queryer.Query(ctx, n)
	if err != nil {
		return nil, err
	}

	// build columns from the provided columns list
	cols := rows.Columns()
	types := make([]string, len(cols))
	rowsTypes, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	for i := 0; i < len(types) && ok; i++ {
		types[i] = rowsTypes.ColumnTypeDatabaseTypeName(i)
	}
	return &queryCursor{
		columns: cols,
		row:     make([]driver.Value, len(cols)),
		strings: make([]string, len(cols)),
	}, nil
}

// Run the query using the Server's defined queryer
func (q *query) Run() error {
	// parse the query
	ast, err := parser.Parse(q.sql)
	if err != nil {
		return q.transport.Write(protocol.ErrorResponse(err))
	}

	// add the session to the context, cast to the Session interface just for
	// compile time verification that the interface is implemented.
	ctx := context.Background()
	ctx = context.WithValue(ctx, sessionCtxKey, q.sess)
	ctx = context.WithValue(ctx, sqlCtxKey, q.sql)
	ctx = context.WithValue(ctx, astCtxKey, ast)

	// execute all of the stmts
	for _, stmt := range ast.Statements {
		rawStmt, isRaw := stmt.(nodes.RawStmt)
		if isRaw {
			stmt = rawStmt.Stmt
		}

		// determine if it's a query or command
		switch v := stmt.(type) {
		case nodes.PrepareStmt:
			s, ok := q.sess.(*session)
			// only session implementation is capable of storing prepared stmts
			if ok {
				// we just store the statement and don't do anything
				s.storePreparedStatement(&v)
			}
		case nodes.SelectStmt, nodes.VariableShowStmt:
			err = q.Query(ctx, stmt)
		default:
			err = q.Exec(ctx, stmt)
		}

		if err != nil {
			return q.transport.Write(protocol.ErrorResponse(err))
		}
	}
	return nil
}

func (q *query) Query(ctx context.Context, n nodes.Node) (*queryCursor, error) {
	rows, err := q.queryer.Query(ctx, n)
	if err != nil {
		return q.transport.Write(protocol.ErrorResponse(err))
	}

	// build columns from the provided columns list
	cols := rows.Columns()
	types := make([]string, len(cols))
	rowsTypes, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	for i := 0; i < len(types) && ok; i++ {
		types[i] = rowsTypes.ColumnTypeDatabaseTypeName(i)
	}

	err = q.transport.Write(protocol.RowDescription(cols, types))
	if err != nil {
		return err
	}

	count := 0
	row := make([]driver.Value, len(cols))
	strings := make([]string, len(cols))
	for {
		err = rows.Next(row)
		if err == io.EOF {
			break
		} else if err != nil {
			return q.transport.Write(protocol.ErrorResponse(err))
		}

		// convert the values to string
		for i, v := range row {
			strings[i] = fmt.Sprintf("%v", v)
		}

		err = q.transport.Write(protocol.DataRow(strings))
		if err != nil {
			return err
		}

		count++
	}

	tag := fmt.Sprintf("SELECT %d", count)
	return q.transport.Write(protocol.CommandComplete(tag))
}

func (q *query) Exec(ctx context.Context, n nodes.Node) error {
	res, err := q.execer.Exec(ctx, n)
	if err != nil {
		return q.transport.Write(protocol.ErrorResponse(err))
	}

	t, ok := res.(ResultTag)
	if !ok {
		t = &tagger{res, n}
	}

	tag, err := t.Tag()
	if err != nil {
		return q.transport.Write(protocol.ErrorResponse(err))
	}
	return q.transport.Write(protocol.CommandComplete(tag))
}

// QueryFromContext returns the sql string as saved in the given context
func QueryFromContext(ctx context.Context) string {
	return ctx.Value(sqlCtxKey).(string)
}

// implements the CommandComplete tag according to the spec as described at the
// link below. When there's no suitable tag according to the spec, "UPDATE" is
// used instead.
// https://www.postgresql.org/docs/10/static/protocol-message-formats.html
type tagger struct {
	driver.Result
	Node nodes.Node
}

func (res *tagger) Tag() (tag string, err error) {
	// allow commands to not specify number of rows affected
	skipResults := false
	switch res.Node.(type) {
	case nodes.VariableSetStmt:
		skipResults = true
		kind := res.Node.(nodes.VariableSetStmt).Kind
		switch kind {
		case nodes.VAR_SET_VALUE, nodes.VAR_SET_CURRENT, nodes.VAR_SET_DEFAULT, nodes.VAR_SET_MULTI:
			tag = "SET"
		case nodes.VAR_RESET, nodes.VAR_RESET_ALL:
			tag = "RESET"
		default:
			tag = "???"
		}
	case nodes.InsertStmt:
		// oid in INSERT is not implemented; defaults to 0
		tag = "INSERT 0"
	case nodes.CreateTableAsStmt:
		tag = "SELECT" // follows the spec
	case nodes.DeleteStmt:
		tag = "DELETE"
	case nodes.FetchStmt:
		tag = "FETCH"
	case nodes.CopyStmt:
		tag = "COPY"
	case nodes.VacuumStmt:
		skipResults = true
		tag = "VACUUM"
	case nodes.CreateRoleStmt:
		skipResults = true
		tag = "CREATE ROLE"
	case nodes.ViewStmt:
		skipResults = true
		tag = "CREATE VIEW"
	case nodes.CreateStmt:
		skipResults = true
		tag = "CREATE TABLE"
	case nodes.UpdateStmt:
		tag = "UPDATE"
	default:
		tag = "UPDATE"
	}

	if !skipResults {
		affected, err := res.RowsAffected()
		if err != nil {
			return tag, err
		}
		tag = fmt.Sprintf("%s %d", tag, affected)
	}
	return tag, nil
}
