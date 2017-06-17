package pgsrv

import (
    "net"
    "context"
    "database/sql/driver"
)

type Rows driver.Rows

type Queryer interface {
    Query(ctx context.Context, sql string) (Rows, error)
}

type Session interface {
    Queryer

    Write(m msg) error
    Read() (msg, error)
}

type Server interface {
    Queryer

    // Manually serve a connection
    Serve(net.Conn) error
}
