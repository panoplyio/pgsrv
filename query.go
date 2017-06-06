package postgressrv

import (
    "io"
    "fmt"
    "database/sql/driver"
)

type query struct {
    session Session
    sql string
    numCols int
}

type column struct {
    name string
}

// Run the query using the Server's defined queryer
func (q *query) Run() error {
    rows, err := q.session.Query(q.sql, nil)
    if err != nil {
        return err
    }

    // build columns from the provided columns list
    cols := []*column{}
    for _, col := range rows.Columns() {
        cols = append(cols, &column{col})
    }

    err = q.session.Write(RowDescriptionMsg(cols))
    if err != nil {
        return err
    }

    row := make([]driver.Value, len(cols))
    strings := make([]string, len(cols))
    for {
        err = rows.Next(row)
        if err == io.EOF {
            break
        } else if err != nil {
            return q.session.Write(ErrMsg(err))
        }

        // convert the values to string
        for i, v := range row {
            strings[i] = fmt.Sprintf("%v", v)
        }

        err = q.session.Write(DataRowMsg(strings))
        if err != nil {
            return err
        }
    }

    // TODO: implement different tags
    return q.session.Write(CompleteMsg("SELECT 1"))
}
