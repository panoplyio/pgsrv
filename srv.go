package postgressrv

import (
    "fmt"
    "net"
)

var Logf = fmt.Printf
var Errf = fmt.Errorf

type Server struct {
    Runner func(sql string)
    Handler func(net.Conn) error
}

func (s *Server) Listen(laddr string) error {
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

func (s *Server) Serve(conn net.Conn) {
    defer conn.Close()

    Logf("CONNECTED %s\n", conn.RemoteAddr())
    sess := &Session{Server: s, Conn: conn}
    err := sess.Serve()
    if err != nil {
        Logf("ERROR Serve %s: %s\n", conn.RemoteAddr(), err.Error())
    }
}


type Runner interface {
    // Run(sql string) chan ep.Dataset
}
