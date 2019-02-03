package pgsrv

import (
	"context"
	"database/sql/driver"
	"fmt"
	parser "github.com/lfittl/pg_query_go"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"io"
)

type query struct {
	session *session
	sql     string
	numCols int
}

// Run the query using the Server's defined queryer
func (q *query) Run() error {
	// parse the query
	ast, err := parser.Parse(q.sql)
	if err != nil {
		return q.session.Write(errMsg(err))
	}

	// add the session to the context, cast to the Session interface just for
	// compile time verification that the interface is implemented.
	ctx := context.Background()
	ctx = context.WithValue(ctx, sessionCtxKey, Session(q.session))
	ctx = context.WithValue(ctx, sqlCtxKey, q.sql)
	ctx = context.WithValue(ctx, astCtxKey, ast)

	// execute all of the statements
	for _, stmt := range ast.Statements {
		rawStmt, isRaw := stmt.(nodes.RawStmt)
		if isRaw {
			stmt = rawStmt.Stmt
		}

		// determine if it's a query or command
		switch stmt.(type) {
		case nodes.SelectStmt, nodes.VariableShowStmt:
			err = q.Query(ctx, stmt)
		default:
			err = q.Exec(ctx, stmt)
		}

		if err != nil {
			return q.session.Write(errMsg(err))
		}
	}
	return nil
}

func (q *query) Query(ctx context.Context, n nodes.Node) error {
	rows, err := q.session.Query(ctx, n)
	if err != nil {
		return q.session.Write(errMsg(err))
	}

	// build columns from the provided columns list
	cols := rows.Columns()
	types := make([]string, len(cols))
	rowsTypes, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	for i := 0; i < len(types) && ok; i++ {
		types[i] = rowsTypes.ColumnTypeDatabaseTypeName(i)
	}

	err = q.session.Write(rowDescriptionMsg(cols, types))
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
			return q.session.Write(errMsg(err))
		}

		// convert the values to string
		for i, v := range row {
			strings[i] = fmt.Sprintf("%v", v)
		}

		err = q.session.Write(dataRowMsg(strings))
		if err != nil {
			return err
		}

		count++
	}

	tag := fmt.Sprintf("SELECT %d", count)
	return q.session.Write(completeMsg(tag))
}

func (q *query) Exec(ctx context.Context, n nodes.Node) error {
	res, err := q.session.Exec(ctx, n)
	if err != nil {
		return q.session.Write(errMsg(err))
	}

	t, ok := res.(ResultTag)
	if !ok {
		t = &tagger{res, n}
	}

	tag, err := t.Tag()
	if err != nil {
		return q.session.Write(errMsg(err))
	}
	return q.session.Write(completeMsg(tag))
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
