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
		fmt.Printf("%+v\n", err)
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

		// determine if it's a query or command
		switch stmt.(type) {
		case nodes.VariableShowStmt:
			err = q.Query(ctx, stmt)
		case nodes.SelectStmt:
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

func (res *tagger) Tag() (string, error) {
	affected, err := res.RowsAffected()
	if err != nil {
		return "", err
	}

	var tag string
	switch res.Node.(type) {
	// oid in INSERT is not implemented; defaults to 0
	case nodes.InsertStmt:
		tag = "INSERT 0"
	case nodes.CreateTableAsStmt:
		tag = "SELECT" // follows the spec
	case nodes.DeleteStmt:
		tag = "DELETE"
	case nodes.FetchStmt:
		tag = "FETCH"
	case nodes.CopyStmt:
		tag = "COPY"
	case nodes.UpdateStmt:
		tag = "UPDATE"
	default:
		tag = "UPDATE"
	}

	return fmt.Sprintf("%s %d", tag, affected), nil
}
