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

    // Manually start serving a connection. This function is called internally
    // by Start(), but can also be called directly
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
