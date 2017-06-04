package postgressrv

import (
    "net"
    "context"
    "math/rand"
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
