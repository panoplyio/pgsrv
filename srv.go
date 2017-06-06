package postgressrv

import (
    "net"
    "context"
    "database/sql/driver"
)

// implements the Server interface
type server struct {
    queryer driver.QueryerContext
}

func New(queryer driver.QueryerContext) Server {
    return &server{queryer}
}

// implements Queryer
func (s *server) QueryContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Rows, error) {
    return s.queryer.QueryContext(ctx, sql, args)
}

func (s *server) Listen(laddr string) error {
    ln, err := net.Listen("tcp", laddr)
    if err != nil {
        return err
    }

    // Logf("Listening on %s...\n", laddr)
    for {
        conn, err := ln.Accept()
        if err != nil {
            return err
        }

        go s.Serve(conn)
    }
}

func (s *server) Serve(conn net.Conn) error {
    defer conn.Close()

    // Logf("CONNECTED %s\n", conn.RemoteAddr())
    sess := &session{Server: s, Conn: conn}
    err := sess.Serve()
    if err != nil {
        // Logf("ERROR Serve %s: %s\n", conn.RemoteAddr(), err.Error())
    }
    return err
}
