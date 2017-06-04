package postgressrv

import (
    "fmt"
    "net"
)

var Logf = fmt.Printf
var Errf = fmt.Errorf

type Column interface {
    Name() string
    TypeOid() uint
    TypeSize() uint16
    TypeModifier() uint32
}

type Query interface {
    Session() Session
    SQL() string
    WriteColumns(...Column) error
    WriteRow(...[]byte) error
}

type Queryer interface {
    Query(q Query) error
}

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
