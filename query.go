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

// Cursor implements ResultTag and returns as a result of a query.
// Cursor holds driver.Rows and allows fetching in batches or in full.
type Cursor struct {
	rows    driver.Rows
	columns []string
	row     []driver.Value
	strings []string
	types   []string
	count   int
	eof     bool
}

// Tag implements ResultTag
func (c *Cursor) Tag() (string, error) {
	return fmt.Sprintf("SELECT %d", c.count), nil
}

// Fetch retrieves next n rows from the saved result and writes a DataRow for every row retrieved.
// Fetch return the amount of rows retrieved and an error if occurred. if n > available rows,
// no error will be returned. if reached EOF, eof flag will be turned on for this Cursor.
func (c *Cursor) Fetch(n int, w protocol.MessageWriter) (count int, err error) {
	for (count < n || n == 0) && !c.eof {
		err = c.rows.Next(c.row)
		if err != nil {
			break
		}

		// convert the values to string
		for i, v := range c.row {
			c.strings[i] = fmt.Sprintf("%v", v)
		}

		err = w.Write(protocol.DataRow(c.strings))
		if err != nil {
			break
		}

		count++
	}
	c.count += count

	if err == io.EOF {
		c.eof = true
		err = nil
	}
	return
}

// CommandResult implements ResultTag and returns as a result of a command.
// Cursor holds a tagger for default tagging.
type CommandResult struct {
	driver.Result
	tagger ResultTag
}

// Tag implements ResultTag
func (cr *CommandResult) Tag() (string, error) {
	return cr.tagger.Tag()
}

type query struct {
	sess    Session
	queryer Queryer
	execer  Execer
	sql     string
	ast     *parser.ParsetreeList
	params  [][]byte
	numCols int
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

func (q *query) withQueryer(queryer Queryer) *query {
	q.queryer = queryer
	return q
}

func (q *query) withExecer(execer Execer) *query {
	q.execer = execer
	return q
}

func (q *query) withParams(params [][]byte) *query {
	q.params = params
	return q
}

func (q *query) Run() (res []ResultTag, err error) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, sqlCtxKey, q.sql)
	if q.params != nil {
		ctx = context.WithValue(ctx, paramsCtxKey, q.params)
	}

	// execute all of the stmts
	for _, stmt := range q.ast.Statements {
		rawStmt, isRaw := stmt.(nodes.RawStmt)
		if isRaw {
			stmt = rawStmt.Stmt
		}

		var r ResultTag
		// determine if it's a query or command
		switch v := stmt.(type) {
		case nodes.PrepareStmt:
			s, ok := q.sess.(*session)
			// only session implementation is capable of storing prepared stmts
			if ok {
				// we just store the statement and don't do anything
				s.storePreparedStatement(&statement{prepareStmt: &v})
			}
		case nodes.SelectStmt, nodes.VariableShowStmt:
			r, err = q.Query(ctx, stmt)
		default:
			r, err = q.Exec(ctx, stmt)
		}

		if err != nil {
			return
		}
		res = append(res, r)
	}
	return
}

func (q *query) Query(ctx context.Context, n nodes.Node) (*Cursor, error) {
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
	return &Cursor{
		columns: cols,
		row:     make([]driver.Value, len(cols)),
		strings: make([]string, len(cols)),
		rows:    rows,
		types:   types,
	}, nil
}

func (q *query) Exec(ctx context.Context, n nodes.Node) (*CommandResult, error) {
	res, err := q.execer.Exec(ctx, n)
	if err != nil {
		return nil, err
	}

	t, ok := res.(ResultTag)
	if !ok {
		t = &tagger{res, n}
	}

	return &CommandResult{
		Result: res,
		tagger: t,
	}, nil
}

// QueryFromContext returns the sql string as saved in the given context
func QueryFromContext(ctx context.Context) string {
	return ctx.Value(sqlCtxKey).(string)
}

// ParamsFromContext returns the raw params array as saved in the given context
func ParamsFromContext(ctx context.Context) [][]byte {
	return ctx.Value(paramsCtxKey).([][]byte)
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
