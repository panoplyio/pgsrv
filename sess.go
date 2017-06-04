package postgressrv

import (
    "io"
    "net"
    "context"
    "math/rand"
    "encoding/binary"
)

var AllSessions = map[int32]*session{}

// Session represents a single client-connection, and handles all of the
// communications with that client.
//
// see: https://www.postgresql.org/docs/9.2/static/protocol.html
// for postgres protocol and startup handshake process
type session struct {
    Server Server
    Conn net.Conn
    Args map[string]string
    Secret int32 // used for cancelling requests
    Ctx context.Context
    CancelFunc context.CancelFunc
    initialized bool
}

func (s *session) Query(q Query) error {
    return s.Server.Query(q)
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

        s := AllSessions[pid]
        if s == nil {
            _, cancelFunc := context.WithCancel(context.Background())
            cancelFunc()
        } else if s.Secret == secret {
            s.CancelFunc() // intentionally doesn't report success to frontend
        }

        return nil // disconnect.
    }

    if msg.IsTLSRequest() {
        // currently we don't support TLS.
        err := s.Write(TLSResponseMsg(false))
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
        return Errf("Unsupported protocol version %s", v)
    }

    s.Args, err = msg.StartupArgs()
    if err != nil {
        return err
    }

    // handle authentication.
    err = s.Write(AuthOKMsg())
    if err != nil {
        return err
    }

    // generate cancellation pid and secret for this session
    s.Secret = rand.Int31()

    pid := rand.Int31()
    for AllSessions[pid] != nil {
        pid += 1
    }

    AllSessions[pid] = s
    defer delete(AllSessions, pid)

    // notify the client of the pid and secret to be passed back when it wishes
    // to interrupt this session
    s.Ctx, s.CancelFunc = context.WithCancel(context.Background())
    err = s.Write(KeyDataMsg(pid, s.Secret))
    if err != nil {
        return err
    }

    // query-cycle
    s.initialized = true
    for {
        // notify the client that we're ready for more messages.
        err = s.Write(ReadyMsg())
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
            Logf("MESSAGE TYPE = %q\n", msg.Type())
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

    return nil
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
func (s *session) Read() (Msg, error) {
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
        msg = make([]byte, len(body) + 1)

        // fixing the type byte at the beginning (position 0) of the new message
        msg[0] = typechar[0]

        // finally append the body to the new message, starting from position 1
        copy(msg[1:], body)
    }

    return NewMsg(msg), nil
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
func (s *session) Write(m Msg) error {
    _, err := s.Conn.Write(m)
    return err
}
