package pgsrv

import (
    "net"
    "context"
    "database/sql/driver"
)

// Queryer is a generic interface for objects capable of parsing and executing
// sql code. The returned Rows object provides the API for reading the row data
// as well as metadata (like Columns, types, etc.)
type Queryer interface {
    Query(ctx context.Context, sql string) (driver.Rows, error)
}

// Session represents a connected client session. It provides the API to set,
// get, delete and accessing all of the session variables. The session should
// added to the context of all queries executed via the "Session" key:
//
//      ctx.Value("Session").(Session)
//
type Session interface {
    Set(k string, v interface{})
    Get(k string) interface{}
    Del(k string)
    All() map[string]interface{}
}

// Server is an interface for objects capable for handling the postgres protocol
// by serving client connections. Each connection is assigned a Session that's
// maintained in-memory until the connection is closed.
type Server interface {
    // Manually serve a connection
    Serve(net.Conn) error // blocks. Run in go-routine.
}
