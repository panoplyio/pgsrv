package postgressrv

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
    Write(m Msg) error
    Read() (Msg, error)
}

type Server interface {
    // Manually start serving a connection. This function is called internally
    // by Start(), but can also be called directly
    Serve(net.Conn) error
}
