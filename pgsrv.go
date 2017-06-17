package pgsrv

import (
    "net"
    "context"
    "database/sql/driver"
)

type Queryer interface {
    Query(ctx context.Context, sql string) (driver.Rows, error)
}

type Session interface {

    // Set a session variable
    Set(k string, v interface{})

    // Get a session variable
    Get(k string) interface{}
}

type Server interface {
    // Manually serve a connection
    Serve(net.Conn) error
}
