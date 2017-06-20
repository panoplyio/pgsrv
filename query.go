package pgsrv

import (
    "io"
    "fmt"
    "context"
    "database/sql/driver"
    parser "github.com/lfittl/pg_query_go"
    nodes "github.com/lfittl/pg_query_go/nodes"
)

type query struct {
    session *session
    sql string
    numCols int
}

type column struct {
    name string
}

func (q *query) Query(ctx context.Context, n nodes.Node) error {
    rows, err := q.session.Query(ctx, n)
    if err != nil {
        return q.session.Write(errMsg(err))
    }

    // build columns from the provided columns list
    cols := []*column{}
    for _, col := range rows.Columns() {
        cols = append(cols, &column{col})
    }

    err = q.session.Write(rowDescriptionMsg(cols))
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

        count += 1
    }

    // TODO: implement different tags
    tag := fmt.Sprintf("SELECT %d", count)
    return q.session.Write(completeMsg(tag))

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
    ctx = context.WithValue(ctx, "Session", Session(q.session))
    ctx = context.WithValue(ctx, "SQL", q.sql)

    // execute all of the statements
    for _, stmt := range ast.Statements {
        err = q.Query(ctx, stmt)
        if err != nil {
            return q.session.Write(errMsg(err))
        }
    }

    return nil
}
