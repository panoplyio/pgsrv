package postgressrv

import (
    // "fmt"
    // "net"
    "testing"
    // "database/sql"

	_ "github.com/lib/pq"
)

func TestOne(t *testing.T) {
    // s := New(&MockQueryer{
        // Cols: []Column{&MockColumn{"roi", 0}},
        // Rows: [][]string{
        //     []string{"one"},
        //     []string{"two"},
        // },
    // })

    // go func() {
    //     ln, err := net.Listen("tcp", ":5432")
    //     if err != nil {
    //         t.Fatal(err)
    //     }
    //     conn, err := ln.Accept()
    //     if err != nil {
    //         t.Fatal(err)
    //     }
    //
    //     fmt.Println("RECEIVED CONN")
    //     err = s.Serve(conn)
    //     if err != nil {
    //         t.Fatal(err)
    //     }
    // }()
    //
    // db, err := sql.Open("postgres", "user=pqgotest dbname=pqgotest sslmode=disable")
    // if err != nil {
    //     t.Fatal(err)
    // }
    //
    // rows, err := db.Query("SELECT 1")
    // if err != nil {
    //     t.Fatal(err)
    // }
    //
    // cols, err := rows.Columns()
    // if err != nil {
    //     t.Fatal(err)
    // }
    // t.Logf("ROWS %v", cols)
}

// type MockQueryer struct {
//     Cols []Column
//     Rows [][]string
// }

// func (m *MockQueryer) Query(q Query) error {
//     fmt.Println("QUERY RECEIVED")
//     err := q.WriteColumns(m.Cols...)
//     if err != nil {
//         return err
//     }
//
//     for _, row := range m.Rows {
//         cells := make([][]byte, len(row))
//         for i, cell := range row {
//             cells[i] = []byte(cell)
//         }
//
//         err := q.WriteRow(cells...)
//         if err != nil {
//             return err
//         }
//     }
//
//     return nil
// }

// type MockColumn struct { name string; oid uint }
// func (c *MockColumn) Name() string { return c.name }
// func (c *MockColumn) TypeOid() uint { return c.oid }
