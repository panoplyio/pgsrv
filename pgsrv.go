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

// Error object that includes a hint text
type ErrHinter interface {
    error
    Hint() string
}

// Error object that includes an error code
// See list of available error codes here:
//      https://www.postgresql.org/docs/10/static/errcodes-appendix.html
type ErrCoder interface {
    error
    Code() string
}
