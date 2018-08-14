package pgsrv

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"io"
	"math/rand"
	"net"
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
	Conn        net.Conn
	Args        map[string]interface{}
	Secret      int32 // used for cancelling requests
	Ctx         context.Context
	CancelFunc  context.CancelFunc
	initialized bool
}

// implements Queryer
func (s *session) Query(ctx context.Context, n nodes.Node) (driver.Rows, error) {
	return s.Server.Query(ctx, n)
}

// implements Execer
func (s *session) Exec(ctx context.Context, n nodes.Node) (driver.Result, error) {
	return s.Server.Exec(ctx, n)
}

// Handle a connection session
func (s *session) Serve() error {
	// read the initial connection startup message
	msg, err := s.Read()
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

	if msg.IsTLSRequest() {
		// currently we don't support TLS.
		err := s.Write(tlsResponseMsg(false))
		if err != nil {
			return err
		}

		// re-read the full startup message
		msg, err = s.Read()
		if err != nil {
			return err
		}
	}

	v, err := msg.StartupVersion()
	if err != nil {
		return err
	}

	if v != "3.0" {
		return fmt.Errorf("Unsupported protocol version %s", v)
	}

	s.Args, err = msg.StartupArgs()
	if err != nil {
		return err
	}

	s.initialized = true

	// handle authentication.
	// TODO: replace with an actual pre-configured authenticator
	a := &authenticationNoPassword{}
	authResponse, err := a.authenticate()
	if err != nil {
		return s.Write(errMsg(WithSeverity(err, "FATAL")))
	}

	err = s.Write(authResponse)
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
	err = s.Write(keyDataMsg(pid, s.Secret))
	if err != nil {
		return err
	}

	// query-cycle
	for {
		// notify the client that we're ready for more messages.
		err = s.Write(readyMsg())
		if err != nil {
			return err
		}

		msg, err = s.Read()
		if err != nil {
			return err
		}

		if msg.IsTerminate() {
			s.Conn.Close()
			return nil // client terminated intentionally
		}

		if msg.Type() != 'Q' {
			// TODO implement other message types
			// Logf("MESSAGE TYPE = %q\n", msg.Type())
		}

		sql, err := msg.QueryText()
		if err != nil {
			return err
		}

		q := &query{session: s, sql: sql}
		err = q.Run()
		if err != nil {
			return err
		}
	}
}

// Read reads and returns a single message from the connection.
//
// The Postgres protocol supports two types of messages: (1) untyped messages
// are only mostly present during the initial startup process and starts with
// the length of the message, followed by the content. (2) typed messages are
// similar to the untyped messages except that they're prefixed with a
// single-byte type character used to distinguish between the different message
// types (query, prepare, etc.), and are the normal messages used for most
// client-server communications.
//
// This method abstracts away this differentiation, returning the next available
// message whether it's typed or not.
func (s *session) Read() (msg, error) {
	typechar := make([]byte, 1)
	if s.initialized {

		// we've already started up, so all future messages are MUST start with
		// a single-byte type identifier.
		_, err := s.Conn.Read(typechar)
		if err != nil {
			return nil, err
		}
	}

	// read the actual body of the message
	msg, err := s.readBody()
	if err != nil {
		return nil, err
	}

	if typechar[0] != 0 {

		// we have a typed-message, prepend it to the message body by first
		// creating a new message that's 1-byte longer than the body in order to
		// make room in memory for the type byte
		body := msg
		msg = make([]byte, len(body)+1)

		// fixing the type byte at the beginning (position 0) of the new message
		msg[0] = typechar[0]

		// finally append the body to the new message, starting from position 1
		copy(msg[1:], body)
	}

	return newMsg(msg), nil
}

// ReadMsgBody reads the body of the next message in the connection. The body is
// comprised of an Int32 body-length (N), inclusive of the length itself
// followed by N-bytes of the actual body.
func (s *session) readBody() ([]byte, error) {

	// messages starts with an Int32 Length of message contents in bytes,
	// including self.
	lenbytes := make([]byte, 4)
	_, err := io.ReadFull(s.Conn, lenbytes)
	if err != nil {
		return nil, err
	}

	// convert the 4-bytes to int
	length := int(binary.BigEndian.Uint32(lenbytes))

	// read the remaining bytes in the message
	msg := make([]byte, length)
	_, err = io.ReadFull(s.Conn, msg[4:]) // keep 4 bytes for the length
	if err != nil {
		return nil, err
	}

	// append the message content to the length bytes in order to rebuild the
	// original message in its entirety
	copy(msg[:4], lenbytes)
	return msg, nil
}

// WriteMsg writes the provided msg to the client connection
func (s *session) Write(m msg) error {
	_, err := s.Conn.Write(m)
	return err
}

func (s *session) Set(k string, v interface{}) { s.Args[k] = v }
func (s *session) Get(k string) interface{}    { return s.Args[k] }
func (s *session) Del(k string)                { delete(s.Args, k) }
func (s *session) All() map[string]interface{} { return s.Args }
