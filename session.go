package pgsrv

import (
	"context"
	"database/sql/driver"
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
	cursor               *cursor
}

type statement struct {
	rawSql      string
	prepareStmt *nodes.PrepareStmt
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
	stmts        map[string]*statement
	pendingStmts map[string]*statement
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

	s.stmts = map[string]*statement{}
	s.pendingStmts = map[string]*statement{}
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
		err = q.Run()
		if err != nil {
			res = append(res, protocol.ParseComplete)
		}
	case *pgproto3.Describe:
		res, err = s.describe(v.ObjectType, v.Name)
	case *pgproto3.Parse:
		res, err = s.prepare(v.Name, v.Query, v.ParameterOIDs)
	case *pgproto3.Bind:
		res, err = s.bind(v.PreparedStatement, v.DestinationPortal, v.Parameters)
	case *pgproto3.Execute:
		res, err = s.execute(v.Portal, v.MaxRows)
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
	case protocol.TransactionFailed, protocol.TransactionEnded:
		if state == protocol.TransactionEnded {
			for k, v := range s.pendingStmts {
				s.stmts[k] = v
			}
		}
		s.pendingStmts = map[string]*statement{}
		s.portals = map[string]*portal{}
	}
}

func (s *session) oidListToNames(list []uint32) ([]string, error) {
	res := make([]string, len(list))
	for i, o := range list {
		dt, ok := s.ConnInfo.DataTypeForOID(pgtype.OID(o))
		if !ok {
			return nil, fmt.Errorf("failed to find type by oid = %d", o)
		}
		res[i] = dt.Name
	}
	return res, nil
}

func (s *session) execute(portalName string, maxRows uint32) (res []protocol.Message, err error) {
	p, ok := s.portals[portalName]
	if !ok {
		res = append(res, protocol.ErrorResponse(fmt.Errorf("portal %s not exist", portalName)))
		return
	}
	if p.cursor != nil {
		p.cursor.Next()
	}
}

func (s *session) storePreparedStatement(stmt *statement) {
	name := ""
	if stmt.prepareStmt.Name != nil {
		name = *stmt.prepareStmt.Name
	}
	s.pendingStmts[name] = stmt
}

func (s *session) prepare(name, sql string, paramOIDs []uint32) (res []protocol.Message, err error) {
	var tree parser.ParsetreeList
	tree, err = parser.Parse(sql)
	if err != nil {
		return
	}

	if len(tree.Statements) > 1 {
		res = append(res, protocol.ErrorResponse(SyntaxError("cannot insert multiple commands into a prepared statement")))
		return
	}

	ps := nodes.PrepareStmt{
		Argtypes: nodes.List{Items: make([]nodes.Node, len(paramOIDs))},
	}
	if len(tree.Statements) == 1 {
		ps.Query = tree.Statements[0]
	}
	for i, p := range paramOIDs {
		dt, ok := s.ConnInfo.DataTypeForOID(pgtype.OID(p))
		if !ok {
			err = fmt.Errorf("unrecognized OID: %d", p)
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

	if name == "" {
		ps.Name = nil
	} else {
		ps.Name = &name
	}
	s.storePreparedStatement(&statement{rawSql: sql, prepareStmt: &ps})
	res = append(res, protocol.ParseComplete)
	return
}

func (s *session) describe(objectType byte, objectName string) (res []protocol.Message, err error) {
	switch objectType {
	case 'S':
		if stmt, ok := s.stmts[objectName]; !ok {
			res = append(res, protocol.ErrorResponse(fmt.Errorf("prepared statement %s not exist", objectName)))
		} else {
			var msg protocol.Message
			msg, err = protocol.ParameterDescription(stmt.prepareStmt)
			if err != nil {
				return
			}
			res = append(res, msg)
			// TODO: add a RowDescription message. this will require access to the catalog
		}
	case 'P':
		err = fmt.Errorf("unsupported object type '%c'", objectType)
	default:
		err = fmt.Errorf("unrecognized object type '%c'", objectType)
	}
	return
}

func (s *session) bind(srcPreparedStmt, dstPortal string, parameters [][]byte) (res []protocol.Message, err error) {
	_, exist := s.stmts[srcPreparedStmt]
	if !exist {
		res = append(res, protocol.ErrorResponse(fmt.Errorf("prepared statement %s not exist", srcPreparedStmt)))
		return
	}
	s.portals[dstPortal] = &portal{
		srcPreparedStatement: srcPreparedStmt,
		parameters:           parameters,
	}
	res = append(res, protocol.BindComplete)
	return
}

func (s *session) Set(k string, v interface{}) { s.Args[k] = v }
func (s *session) Get(k string) interface{}    { return s.Args[k] }
func (s *session) Del(k string)                { delete(s.Args, k) }
func (s *session) All() map[string]interface{} { return s.Args }
