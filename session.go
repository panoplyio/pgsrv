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
	params               [][]byte
	result               ResultTag
}

type statement struct {
	rawSQL      string
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
	s.ConnInfo.RegisterDataType(pgtype.DataType{Name: "text", OID: pgtype.OID(0), Value: &pgtype.GenericText{}})
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

func (s *session) getPreparedStmt(name string) *statement {
	if stmt, ok := s.pendingStmts[name]; ok {
		return stmt
	}
	if stmt, ok := s.stmts[name]; ok {
		return stmt
	}
	return nil
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

func (s *session) handleFrontendMessage(t *protocol.Transport, msg pgproto3.FrontendMessage) (err error) {
	var res []protocol.Message
	switch v := msg.(type) {
	case *pgproto3.Terminate:
		_ = s.Conn.Close()
		return // client terminated intentionally
	case *pgproto3.Query:
		err = s.query(v.String, t)
		if err != nil {
			res = append(res, protocol.ParseComplete)
		}
		// postgres doesn't save unnamed statement after a simple query so we imitate this behaviour
		delete(s.stmts, "")
	case *pgproto3.Describe:
		res, err = s.describe(v.ObjectType, v.Name)
	case *pgproto3.Parse:
		res, err = s.prepare(v.Name, v.Query, v.ParameterOIDs)
	case *pgproto3.Bind:
		res, err = s.bind(v.PreparedStatement, v.DestinationPortal, v.Parameters)
	case *pgproto3.Execute:
		res, err = s.execute(t, v.Portal, v.MaxRows)
	}
	for _, m := range res {
		err = t.Write(m)
		if err != nil {
			break
		}
	}
	return
}

func (s *session) query(sql string, t *protocol.Transport) error {
	q, err := parseQuery(sql)
	if err != nil {
		return err
	}
	results, err := q.withExecer(s.Server).
		withQueryer(s.Server).
		Run()

	for _, res := range results {
		if c, ok := res.(*Cursor); ok {
			err = t.Write(protocol.RowDescription(c.columns, c.types))
			if err != nil {
				return err
			}
			_, err := c.Fetch(0, t)
			if err != nil {
				return err
			}
		}
		tag, err := res.Tag()
		if err == nil {
			err = t.Write(protocol.CommandComplete(tag))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *session) execute(t *protocol.Transport, portalName string, maxRows uint32) (res []protocol.Message, err error) {
	portal, ok := s.portals[portalName]
	if !ok {
		res = append(res, protocol.ErrorResponse(fmt.Errorf("portal %s not exist", portalName)))
		return
	}
	stmt := s.getPreparedStmt(portal.srcPreparedStatement)
	if stmt == nil {
		res = append(res, protocol.ErrorResponse(fmt.Errorf("statement %s not exist", portal.srcPreparedStatement)))
		return
	}
	if portal.result == nil {
		q := createQuery(stmt.rawSQL, stmt.prepareStmt.Query)
		if len(portal.params) > 0 {
			argTypes := make([]nodes.TypeName, len(stmt.prepareStmt.Argtypes.Items))
			for i, at := range stmt.prepareStmt.Argtypes.Items {
				tn, ok := at.(nodes.TypeName)
				if !ok {
					return nil, fmt.Errorf("expected node of type 'TypeName', got %T", at)
				}
				argTypes[i] = tn
			}
			q.withParams(portal.params).withArgTypes(argTypes)
		}
		var results []ResultTag
		results, err = q.withExecer(s.Server).
			withQueryer(s.Server).
			Run()
		if err != nil {
			return
		}

		// prepared statement can have at most 1 command, hence query can produce at most 1 result
		portal.result = results[0]
	}

	if c, ok := portal.result.(*Cursor); ok {
		_, err = c.Fetch(int(maxRows), t)
		if err != nil {
			return
		}
		if !c.eof {
			res = append(res, protocol.PortalSuspended)
			return
		}
	}
	var tag string
	tag, err = portal.result.Tag()
	if err == nil {
		res = append(res, protocol.CommandComplete(tag))
	}
	return
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
			return
		}
		ps.Argtypes.Items[i] = nodes.TypeName{
			TypeOid: nodes.Oid(0),
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
	s.storePreparedStatement(&statement{rawSQL: sql, prepareStmt: &ps})
	res = append(res, protocol.ParseComplete)
	return
}

func (s *session) describe(objectType byte, objectName string) (res []protocol.Message, err error) {
	switch objectType {
	case 'S':
		stmt := s.getPreparedStmt(objectName)
		if stmt == nil {
			res = append(res, protocol.ErrorResponse(fmt.Errorf("prepared statement %s not exist", objectName)))
		} else {
			var msg protocol.Message
			msg, err = protocol.ParameterDescription(stmt.prepareStmt)
			if err != nil {
				return
			}
			res = append(res, msg)
			// TODO: add a real RowDescription message. this will require access to the catalog
			res = append(res, protocol.RowDescription(nil, nil))
		}
	case 'P':
		// TODO: add a real RowDescription message. this will require access to the catalog
		res = append(res, protocol.RowDescription(nil, nil))
	default:
		err = fmt.Errorf("unrecognized object type '%c'", objectType)
	}
	return
}

func (s *session) bind(srcPreparedStmt, dstPortal string, parameters [][]byte) (res []protocol.Message, err error) {
	stmt := s.getPreparedStmt(srcPreparedStmt)
	if stmt == nil {
		res = append(res, protocol.ErrorResponse(fmt.Errorf("prepared statement %s not exist", srcPreparedStmt)))
		return
	}
	s.portals[dstPortal] = &portal{
		srcPreparedStatement: srcPreparedStmt,
		params:               parameters,
	}
	res = append(res, protocol.BindComplete)
	return
}

func (s *session) Set(k string, v interface{}) { s.Args[k] = v }
func (s *session) Get(k string) interface{}    { return s.Args[k] }
func (s *session) Del(k string)                { delete(s.Args, k) }
func (s *session) All() map[string]interface{} { return s.Args }
