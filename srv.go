package pgsrv

import (
	"context"
	"database/sql/driver"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"net"
)

// implements the Server interface
type server struct {
	queryer       Queryer
	authenticator authenticator
}

// New creates a Server object capable of handling postgres client connections.
// It delegates query execution to the provided Queryer. If the provided Queryer
// also implements Execer, the returned server will also be able to handle
// executing SQL commands (see Execer).
//
// If queryer implements passwordProvider interface, a new server will be protected
// with a new md5Authenticator.
func New(queryer Queryer) Server {
	var auth authenticator
	auth = &noPasswordAuthenticator{}
	pp, ok := queryer.(PasswordProvider)
	if ok {
		switch pp.Type() {
		case MD5:
			auth = &md5Authenticator{pp}
		case Plain:
			auth = &clearTextAuthenticator{pp}
		}
	}
	return &server{queryer, auth}
}

// implements Queryer
func (s *server) Query(ctx context.Context, n nodes.Node) (driver.Rows, error) {
	return s.queryer.Query(ctx, n)
}

// implements Execer
func (s *server) Exec(ctx context.Context, n nodes.Node) (driver.Result, error) {
	execer, ok := s.queryer.(Execer)
	if !ok {
		return nil, Unsupported("commands execution. Read-only mode.")
	}

	return execer.Exec(ctx, n)
}

func (s *server) Listen(laddr string) error {
	ln, err := net.Listen("tcp", laddr)
	if err != nil {
		return err
	}

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

	sess := &session{Server: s, Conn: conn}
	err := sess.Serve()
	if err != nil {
		// TODO: Log it?
	}
	return err
}

func (s *server) authenticate(sess *session, args map[string]interface{}) error {
	return s.authenticator.authenticate(sess, args)
}
