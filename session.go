package pgsrv

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	"github.com/jackc/pgx/pgtype"
	parser "github.com/lfittl/pg_query_go"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"github.com/panoplyio/pgsrv/protocol"
	"io"
	"math/rand"
	"strings"
	"sync"
)

var allSessions sync.Map

type portal struct {
	srcPreparedStatement string
	parameters           [][]byte
}

// Session represents a single client-connection, and handles all of the
// communications with that client.
//
// see: https://www.postgresql.org/docs/9.2/static/protocol.html
// for postgres protocol and startup handshake process
type session struct {
	Server       *server
	Conn         io.ReadWriteCloser
	ConnInfo     *pgtype.ConnInfo
	Args         map[string]interface{}
	Secret       int32 // used for cancelling requests
	Ctx          context.Context
	CancelFunc   context.CancelFunc
	initialized  bool
	stmts        map[string]*nodes.PrepareStmt
	pendingStmts map[string]*nodes.PrepareStmt
	portals      map[string]*portal
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

	s.ConnInfo = pgtype.NewConnInfo()
	for k, v := range protocol.TypesOid {
		s.ConnInfo.RegisterDataType(pgtype.DataType{Name: strings.ToLower(k), OID: pgtype.OID(v), Value: &pgtype.GenericText{}})
	}

	return nil
}

// Handle a connection session
func (s *session) Serve() error {
	err := s.startUp()
	if err != nil {
		return err
	}

	s.stmts = map[string]*nodes.PrepareStmt{}
	s.pendingStmts = map[string]*nodes.PrepareStmt{}
	s.portals = map[string]*portal{}
	t := protocol.NewTransport(s.Conn)

	// query-cycle
	for {
		msg, ts, err := t.NextFrontendMessage()
		if err != nil {
			return err
		}

		s.handleTransactionState(ts)
		err = s.handleFrontendMessage(t, msg)
		if err != nil {
			return err
		}
	}
}

func (s *session) handleFrontendMessage(t *protocol.Transport, msg pgproto3.FrontendMessage) (err error) {
	var res []protocol.Message
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
	case *pgproto3.Describe:
		res, err = s.describe(v)
	case *pgproto3.Parse:
		res, err = s.prepare(v)
	case *pgproto3.Bind:
		res, err = s.bind(v)
	case *pgproto3.Sync:
	default:
		res = append(res, protocol.ErrorResponse(Unsupported("message type")))
	}
	for _, m := range res {
		err = t.Write(m)
		if err != nil {
			break
		}
	}
	return
}

func (s *session) handleTransactionState(state protocol.TransactionState) {
	switch state {
	case protocol.InTransaction, protocol.NotInTransaction:
		// these states have no effect on session
		break
	case protocol.TransactionFailed, protocol.TransactionEnded:
		if state == protocol.TransactionEnded {
			for k, v := range s.pendingStmts {
				s.stmts[k] = v
			}
		}
		s.pendingStmts = map[string]*nodes.PrepareStmt{}
		s.portals = map[string]*portal{}
	}
}

func (s *session) prepare(parseMsg *pgproto3.Parse) (res []protocol.Message, err error) {
	var tree parser.ParsetreeList
	tree, err = parser.Parse(parseMsg.Query)
	if err != nil {
		res = append(res, protocol.ErrorResponse(SyntaxError(err.Error())))
		return
	}

	ps := nodes.PrepareStmt{
		Query:    tree.Statements[0],
		Argtypes: nodes.List{Items: make([]nodes.Node, len(parseMsg.ParameterOIDs))},
	}
	for i, p := range parseMsg.ParameterOIDs {
		dt, ok := s.ConnInfo.DataTypeForOID(pgtype.OID(p))
		if !ok {
			res = append(res, protocol.ErrorResponse(fmt.Errorf("cache lookup failed for type %d", p)))
			return
		}
		ps.Argtypes.Items[i] = nodes.TypeName{
			TypeOid: nodes.Oid(p),
			Names: nodes.List{
				Items: []nodes.Node{
					nodes.String{Str: dt.Name},
				},
			},
		}
	}

	if parseMsg.Name == "" {
		ps.Name = nil
	} else {
		ps.Name = &parseMsg.Name
	}
	s.storePreparedStatement(&ps)
	res = append(res, protocol.ParseComplete)
	return
}

func (s *session) storePreparedStatement(ps *nodes.PrepareStmt) {
	name := ""
	if ps.Name != nil {
		name = *ps.Name
	}
	s.pendingStmts[name] = ps
}

func (s *session) describe(describeMsg *pgproto3.Describe) (res []protocol.Message, err error) {
	switch describeMsg.ObjectType {
	case protocol.DescribeStatement:
		if ps, ok := s.stmts[describeMsg.Name]; !ok {
			res = append(res, protocol.ErrorResponse(InvalidSQLStatementName(describeMsg.Name)))
		} else {
			var msg protocol.Message
			msg, err = protocol.ParameterDescription(ps)
			if err != nil {
				return
			}
			res = append(res, msg)
			// TODO: add a RowDescription message. this will require access to the backend
		}
	case protocol.DescribePortal:
		err = Unsupported("object type '%c'", describeMsg.ObjectType)
	default:
		err = ProtocolViolation(fmt.Sprintf("invalid DESCRIBE message subtype '%c'", describeMsg.ObjectType))
	}
	return
}

func (s *session) bind(bindMsg *pgproto3.Bind) (res []protocol.Message, err error) {
	_, exist := s.stmts[bindMsg.PreparedStatement]
	if !exist {
		res = append(res, protocol.ErrorResponse(InvalidSQLStatementName(bindMsg.PreparedStatement)))
		return
	}
	s.portals[bindMsg.DestinationPortal] = &portal{
		srcPreparedStatement: bindMsg.PreparedStatement,
		parameters:           bindMsg.Parameters,
	}
	res = append(res, protocol.BindComplete)
	return
}

func (s *session) Set(k string, v interface{}) { s.Args[k] = v }
func (s *session) Get(k string) interface{}    { return s.Args[k] }
func (s *session) Del(k string)                { delete(s.Args, k) }
func (s *session) All() map[string]interface{} { return s.Args }
