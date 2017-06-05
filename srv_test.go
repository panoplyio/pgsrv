package postgressrv

import (
    "io"
    "fmt"
    "net"
    "testing"
    "database/sql"
    "database/sql/driver"

	_ "github.com/lib/pq"
)

func TestOne(t *testing.T) {
    mock := &rows{}
    mock.AddCol("one")
    mock.AddRow("hello")
    mock.AddRow("world")

    s := New(mock)

    go func() {
        ln, err := net.Listen("tcp", ":5432")
        if err != nil {
            t.Fatal(err)
        }
        conn, err := ln.Accept()
        if err != nil {
            t.Fatal(err)
        }

        fmt.Println("RECEIVED CONN")
        err = s.Serve(conn)
        if err != nil {
            t.Fatal(err)
        }
    }()

    db, err := sql.Open("postgres", "user=pqgotest dbname=pqgotest sslmode=disable")
    if err != nil {
        t.Fatal(err)
    }

    rows, err := db.Query("SELECT 1")
    if err != nil {
        t.Fatal(err)
    }

    fmt.Println(rows.Columns())

    for rows.Next() {
        var v string
        err := rows.Scan(&v)
        if err != nil {
            t.Fatal(err)
        }

        fmt.Println(v)
    }
}

type rows struct {
    cols []string
    rows [][]driver.Value
}

func (*rows) Close() error {
    panic("not implemented")
}

func (rows *rows) Columns() []string {
    return rows.cols
}

func (rows *rows) Next(dest []driver.Value) error {
    if len(rows.rows) == 0 {
        return io.EOF
    }

    for i, v := range rows.rows[0] {
        dest[i] = v
    }

    rows.rows = rows.rows[1:]
    return nil
}

func (rows *rows) AddCol(name string) {
    rows.cols = append(rows.cols, name)
}

func (rows *rows) AddRow(v string) {
    var err error
    row := make([]driver.Value, 1)
    row[0], err = driver.String.ConvertValue(v)
    if err != nil {
        panic(err)
    }
    rows.rows = append(rows.rows, row)
}

func (rows *rows) Query(string, []driver.Value) (driver.Rows, error) {
    return rows, nil
}
