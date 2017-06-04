package postgressrv

import (
    "net"
)

// implements the Server interface
type server struct {
    queryer Queryer
}

func New(queryer Queryer) Server {
    return &server{queryer}
}

// implements Queryer
func (s *server) Query(q Query) error {
    return s.queryer.Query(q)
}

func (s *server) Listen(laddr string) error {
    ln, err := net.Listen("tcp", laddr)
    if err != nil {
        return err
    }

    Logf("Listening on %s...\n", laddr)
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

    Logf("CONNECTED %s\n", conn.RemoteAddr())
    sess := &session{Server: s, Conn: conn}
    err := sess.Serve()
    if err != nil {
        Logf("ERROR Serve %s: %s\n", conn.RemoteAddr(), err.Error())
    }
    return err
}
