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
	Server        *server
	Conn          io.ReadWriteCloser
	Args          map[string]interface{}
	Secret        int32 // used for cancelling requests
	Ctx           context.Context
	CancelFunc    context.CancelFunc
	initialized   bool
	queryer       Queryer
	authenticator authenticator
	statements    map[string]string
}

// Handle a connection session
func (s *session) Serve() error {
	p := protocol.NewProtocol(s.Conn, s.Conn)

	suMsg, err := p.StartUp()
	if err != nil {
		return err
	}

	if suMsg.IsCancel() {
		pid, secret, err := suMsg.CancelKeyData()
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

	s.Args, err = suMsg.StartupArgs()
	if err != nil {
		return err
	}

	// handle authentication
	err = s.Server.authenticator.authenticate(p, s.Args)
	if err != nil {
		return err
	}

	err = p.Write(protocol.ParameterStatus("client_encoding", "utf8"))
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
	err = p.Write(protocol.BackendKeyData(pid, s.Secret))
	if err != nil {
		return err
	}

	// query-cycle
	for {
		msg, err := p.NextFrontendMessage()
		if err != nil {
			return err
		}

		switch msg.(type) {
		case *pgproto3.Terminate:
			s.Conn.Close()
			return nil // client terminated intentionally
		case *pgproto3.Query:
			q := &query{protocol: p, sql: msg.(*pgproto3.Query).String, queryer: s.Server, execer: s.Server}
			err = q.Run(s)
			if err != nil {
				return err
			}
		case *pgproto3.Describe:
			err = p.Write(protocol.ErrorResponse(fmt.Errorf("not implemented")))
			if err != nil {
				return err
			}
		case *pgproto3.Parse:
			err = p.Write(protocol.ErrorResponse(fmt.Errorf("not implemented")))
			if err != nil {
				return err
			}
		case *pgproto3.Bind:
			err = p.Write(protocol.ErrorResponse(fmt.Errorf("not implemented")))
			if err != nil {
				return err
			}
		case *pgproto3.Execute:
			err = p.Write(protocol.ErrorResponse(fmt.Errorf("not implemented")))
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
