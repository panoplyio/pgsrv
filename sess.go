package pgsrv

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgproto3"
	"github.com/jackc/pgx/pgtype"
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
	statements    map[string]*pgx.PreparedStatement
	portals       map[string]*portal
}

// Handle a connection session
func (s *session) Serve() error {
	t := protocol.NewTransport(s.Conn, s.Conn)
	s.statements = map[string]*pgx.PreparedStatement{}
	s.portals = map[string]*portal{}

	suMsg, err := t.StartUp()
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
	err = s.Server.authenticator.authenticate(t, s.Args)
	if err != nil {
		return err
	}

	err = t.Write(protocol.ParameterStatus("client_encoding", "UTF8"))
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
	err = t.Write(protocol.BackendKeyData(pid, s.Secret))
	if err != nil {
		return err
	}

	// query-cycle
	for {
		msg, err := t.NextFrontendMessage()
		if err != nil {
			return err
		}

		var res []protocol.Message
		switch msg.(type) {
		case *pgproto3.Terminate:
			s.Conn.Close()
			return nil // client terminated intentionally
		case *pgproto3.Query:
			q := &query{transport: t, sql: msg.(*pgproto3.Query).String, queryer: s.Server, execer: s.Server}
			err = q.Run(s)
		case *pgproto3.Describe:
			res, err = s.describe(msg.(*pgproto3.Describe))
		case *pgproto3.Parse:
			res, err = s.prepare(msg.(*pgproto3.Parse))
		case *pgproto3.Bind:
			res, err = s.bind(msg.(*pgproto3.Bind))
		case *pgproto3.Execute:
			err = t.Write(protocol.ErrorResponse(fmt.Errorf("not implemented")))
		}
		for _, m := range res {
			err = t.Write(m)
			if err != nil {
				break
			}
		}
		if err != nil {
			return err
		}
	}
}

func (s *session) prepare(parseMsg *pgproto3.Parse) (res []protocol.Message, err error) {
	ps := &pgx.PreparedStatement{
		Name: parseMsg.Name,
		SQL:  parseMsg.Query,
	}
	ps.ParameterOIDs = make([]pgtype.OID, len(parseMsg.ParameterOIDs))
	for i := 0; i < len(parseMsg.ParameterOIDs); i++ {
		ps.ParameterOIDs[i] = pgtype.OID(parseMsg.ParameterOIDs[i])
	}
	s.statements[ps.Name] = ps
	res = append(res, protocol.ParseComplete)
	return
}

func (s *session) describe(describeMsg *pgproto3.Describe) (res []protocol.Message, err error) {
	switch describeMsg.ObjectType {
	case 'S':
		if ps, ok := s.statements[describeMsg.Name]; !ok {
			res = append(res, protocol.ErrorResponse(fmt.Errorf("prepared statement %s not exist", describeMsg.Name)))
		} else {
			res = append(res, protocol.ParameterDescription(ps))
			// TODO: add a RowDescription message. this will require access to the catalog
		}
	case 'P':
		err = fmt.Errorf("unsupported object type '%c'", describeMsg.ObjectType)
	default:
		err = fmt.Errorf("unrecognized object type '%c'", describeMsg.ObjectType)
	}
	return
}

func (s *session) bind(bindMsg *pgproto3.Bind) (res []protocol.Message, err error) {
	ps, exist := s.statements[bindMsg.PreparedStatement]
	if !exist {
		res = append(res, protocol.ErrorResponse(fmt.Errorf("prepared statement %s not exist", bindMsg.PreparedStatement)))
		return
	}
	s.portals[bindMsg.DestinationPortal] = &portal{
		srcPreparedStatement: ps.Name,
		parameters:           bindMsg.Parameters,
	}
	res = append(res, protocol.BindComplete)
	return
}

func (s *session) Set(k string, v interface{}) { s.Args[k] = v }
func (s *session) Get(k string) interface{}    { return s.Args[k] }
func (s *session) Del(k string)                { delete(s.Args, k) }
func (s *session) All() map[string]interface{} { return s.Args }
