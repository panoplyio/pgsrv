package pgsrv

import (
    "io"
    "fmt"
    "context"
    "database/sql/driver"
)

type query struct {
    session *session
    sql string
    numCols int
}

type column struct {
    name string
}

// Run the query using the Server's defined queryer
func (q *query) Run() error {
    ctx := context.Background()
    rows, err := q.session.Query(ctx, q.sql)
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
