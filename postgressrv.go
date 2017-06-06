package postgressrv

import (
    "fmt"
    "net"
    "database/sql/driver"
)

var Logf = fmt.Printf
var Errf = fmt.Errorf

type Queryer driver.Queryer

type Session interface {
    Queryer
    Write(m Msg) error
    Read() (Msg, error)
}

type Server interface {
    Queryer

    // Manually serve a connection
    Serve(net.Conn) error
}

// Msg is just an alias for a slice of bytes that exposes common operations on
// Postgres' client-server protocol messages.
// see: https://www.postgresql.org/docs/9.2/static/protocol-message-formats.html
// for postgres specific list of message formats
type Msg []byte

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
