package pgsrv

import (
    "net"
    "context"
    "database/sql/driver"
)

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

type Server interface {
    // Manually serve a connection
    Serve(net.Conn) error
}
