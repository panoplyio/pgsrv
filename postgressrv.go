package postgressrv

import (
    "net"
    "database/sql/driver"
)

type Session interface {
    driver.Queryer
    Write(m msg) error
    Read() (msg, error)
}

type Server interface {
    driver.Queryer

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
