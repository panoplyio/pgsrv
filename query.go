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

// implements Execer
func (q *query) Exec(ctx context.Context, n nodes.Node) error {
    res, err := q.session.Exec(ctx, n)
    if err != nil {
        return q.session.Write(errMsg(err))
    }

    var tag string
    tagger, ok := res.(ResultTag)
    if ok {
        // tag is supplied by the results
        tag, err = tagger.Tag()
        if err != nil {
            return q.session.Write(errMsg(err))
        }
    } else {
        // default tag behavior to follow the spec described in CommandComplete:
        // https://www.postgresql.org/docs/10/static/protocol-message-formats.html
        affected, err := res.RowsAffected()
        if err != nil {
            return q.session.Write(errMsg(err))
        }

        switch n.(type) {
        case nodes.CreateTableAsStmt:
            tag = "SELECT" // follows the spec
        case nodes.InsertStmt:
            tag = "INSERT 0" // oid is not implemented; defaults to 0.
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

        tag = fmt.Sprintf("%s %d", tag, affected)
    }

    return q.session.Write(completeMsg(tag))
}

// implements Queryer
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
    ctx = context.WithValue(ctx, "AST", ast)

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
