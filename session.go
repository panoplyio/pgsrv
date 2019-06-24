package pgsrv

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	"github.com/panoplyio/pgsrv/protocol"
	"io"
	"math/rand"
	"sync"
)

var allSessions sync.Map

// Session represents a single client-connection, and handles all of the
// communications with that client.
//
// see: https://www.postgresql.org/docs/9.2/static/protocol.html
// for postgres protocol and startup handshake process
type session struct {
	Server      *server
	Conn        io.ReadWriteCloser
	Args        map[string]interface{}
	Secret      int32 // used for cancelling requests
	Ctx         context.Context
	CancelFunc  context.CancelFunc
	initialized bool
}

func (s *session) startUp() error {
	handshake := protocol.NewHandshake(s.Conn)
	msg, err := handshake.Init()
	if err != nil {
		return err
	}

	if msg.IsCancel() {
		pid, secret, err := msg.CancelKeyData()
		if err != nil {
			return err
		}

		s, ok := allSessions.Load(pid)
		if !ok || s == nil {
			_, cancelFunc := context.WithCancel(context.Background())
			cancelFunc()
		} else if s.(*session).Secret == secret {
			s.(*session).CancelFunc() // intentionally doesn't report success to frontend
		}

		return nil // disconnect.
	}

	s.Args, err = msg.StartupArgs()
	if err != nil {
		return err
	}

	// handle authentication
	err = s.Server.authenticator.authenticate(handshake, s.Args)
	if err != nil {
		return err
	}

	err = handshake.Write(protocol.ParameterStatus("client_encoding", "utf8"))
	if err != nil {
		return err
	}

	// generate cancellation pid and secret for this session
	s.Secret = rand.Int31()

	pid := rand.Int31()
	for s1, ok := allSessions.Load(pid); ok && s1 != nil; pid++ {
		s1, ok = allSessions.Load(pid)
	}

	allSessions.Store(pid, s)
	defer allSessions.Delete(pid)

	// notify the client of the pid and secret to be passed back when it wishes
	// to interrupt this session
	s.Ctx, s.CancelFunc = context.WithCancel(context.Background())
	err = handshake.Write(protocol.BackendKeyData(pid, s.Secret))
	if err != nil {
		return err
	}
	return nil
}

// Handle a connection session
func (s *session) Serve() error {
	err := s.startUp()
	if err != nil {
		return err
	}

	t := protocol.NewTransport(s.Conn)

	// query-cycle
	for {
		msg, err := t.NextFrontendMessage()
		if err != nil {
			return err
		}

		switch v := msg.(type) {
		case *pgproto3.Terminate:
			s.Conn.Close()
			return nil // client terminated intentionally
		case *pgproto3.Query:
			q := &query{
				transport: t,
				sql:       v.String,
				queryer:   s.Server,
				execer:    s.Server,
			}
			err = q.Run(s)
			if err != nil {
				return err
			}
		case *pgproto3.Describe:
			err = t.Write(protocol.ErrorResponse(fmt.Errorf("not implemented")))
			if err != nil {
				return err
			}
		case *pgproto3.Parse:
			err = t.Write(protocol.ErrorResponse(fmt.Errorf("not implemented")))
			if err != nil {
				return err
			}
		case *pgproto3.Bind:
			err = t.Write(protocol.ErrorResponse(fmt.Errorf("not implemented")))
			if err != nil {
				return err
			}
		case *pgproto3.Execute:
			err = t.Write(protocol.ErrorResponse(fmt.Errorf("not implemented")))
			if err != nil {
				return err
			}
		}
	}
}

func (s *session) Set(k string, v interface{}) { s.Args[k] = v }
func (s *session) Get(k string) interface{}    { return s.Args[k] }
func (s *session) Del(k string)                { delete(s.Args, k) }
func (s *session) All() map[string]interface{} { return s.Args }