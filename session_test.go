package pgsrv

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	"github.com/lfittl/pg_query_go/nodes"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"github.com/panoplyio/pg-stories"
	"github.com/panoplyio/pgsrv/protocol"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var testStmtName = "test_stmt"

func startupSeq() []pg_stories.Step {
	startupMsg := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      make(map[string]string),
	}
	startupMsg.Parameters["user"] = "postgres"

	return []pg_stories.Step{
		&pg_stories.Command{FrontendMessage: &startupMsg},
		&pg_stories.Response{BackendMessage: &pgproto3.Authentication{}},
		&pg_stories.Response{BackendMessage: &pgproto3.ReadyForQuery{}},
	}
}

type mockQueryer struct{}

func (r *mockQueryer) Query(ctx context.Context, n pg_query.Node) (driver.Rows, error) {
	rows := &mockRows{1, 0}
	return rows, nil
}

type mockRows struct {
	rows uint8
	pos  uint8
}

func (r *mockRows) Columns() []string { return []string{"column1"} }
func (r *mockRows) Close() error      { return nil }
func (r *mockRows) Next(dest []driver.Value) error {
	if r.pos >= r.rows {
		return io.EOF
	}

	dest[0] = fmt.Sprintf("row %d", r.pos)
	r.pos = r.pos + 1

	return nil
}

type pgStoryScriptsRunner struct {
	baseFolder string
	init       func() (net.Conn, chan interface{})
}

type unhandledFrontendMessage struct{}

func (unhandledFrontendMessage) Decode(data []byte) error { return nil }
func (unhandledFrontendMessage) Encode(dst []byte) []byte { return nil }
func (unhandledFrontendMessage) Frontend()                {}

func TestSession_handleFrontendMessage(t *testing.T) {
	t.Run("unsupported message type", func(t *testing.T) {
		f, b := net.Pipe()
		frontend, err := pgproto3.NewFrontend(f, nil)
		go func() {
			msg, err := frontend.Receive()
			require.NoError(t, err)
			require.IsType(t, msg, &pgproto3.ErrorResponse{})
			require.Equal(t, "0A000", msg.(*pgproto3.ErrorResponse).Code)
		}()
		require.NoError(t, err)
		transport := protocol.NewTransport(b)
		sess := &session{}
		err = sess.handleFrontendMessage(transport, &unhandledFrontendMessage{})
		require.NoError(t, err)
	})
	t.Run("terminate", func(t *testing.T) {
		f, b := net.Pipe()
		transport := protocol.NewTransport(b)
		sess := &session{Conn: f}
		err := sess.handleFrontendMessage(transport, &pgproto3.Terminate{})
		require.NoError(t, err)
		_, err = f.Read([]byte{})
		require.Equal(t, io.ErrClosedPipe, err)
	})
}

func TestSession_storePreparedStatement(t *testing.T) {
	t.Run("stores provided statement", func(t *testing.T) {
		query := "bar"
		sess := &session{pendingStmts: map[string]*nodes.PrepareStmt{}}
		sess.storePreparedStatement(&nodes.PrepareStmt{
			Name:  &testStmtName,
			Query: nodes.String{Str: query},
		})
		require.NotNil(t, sess.pendingStmts[testStmtName])
		require.Equal(t, query, sess.pendingStmts[testStmtName].Query.(nodes.String).Str)
	})
}

func TestSession_handleTransactionState(t *testing.T) {
	t.Run("TransactionFailed", func(t *testing.T) {
		sess := &session{
			pendingStmts: map[string]*nodes.PrepareStmt{
				testStmtName: {Name: &testStmtName},
			},
			stmts: map[string]*nodes.PrepareStmt{
				testStmtName: {Name: &testStmtName},
			},
			portals: map[string]*portal{
				"": {srcPreparedStatement: testStmtName},
			},
		}
		sess.handleTransactionState(protocol.TransactionFailed)
		require.Empty(t, sess.pendingStmts)
		require.Empty(t, sess.portals)
		require.NotEmpty(t, sess.stmts)
	})
	t.Run("TransactionEnded", func(t *testing.T) {
		sess := &session{
			pendingStmts: map[string]*nodes.PrepareStmt{
				"2": {Name: &testStmtName},
			},
			stmts: map[string]*nodes.PrepareStmt{
				"1": {Name: &testStmtName},
			},
			portals: map[string]*portal{
				"": {srcPreparedStatement: testStmtName},
			},
		}
		sess.handleTransactionState(protocol.TransactionEnded)
		require.Empty(t, sess.pendingStmts)
		require.Empty(t, sess.portals)
		require.NotEmpty(t, sess.stmts)
		require.Len(t, sess.stmts, 2)
	})
	t.Run("InTransaction", func(t *testing.T) {
		sess := &session{
			pendingStmts: map[string]*nodes.PrepareStmt{
				"2": {Name: &testStmtName},
			},
			stmts: map[string]*nodes.PrepareStmt{
				"1": {Name: &testStmtName},
			},
			portals: map[string]*portal{
				"": {srcPreparedStatement: testStmtName},
			},
		}
		sess.handleTransactionState(protocol.InTransaction)
		require.Len(t, sess.pendingStmts, 1)
		require.Len(t, sess.stmts, 1)
		require.Len(t, sess.portals, 1)
	})
	t.Run("NotInTransaction", func(t *testing.T) {
		sess := &session{
			pendingStmts: map[string]*nodes.PrepareStmt{
				"2": {Name: &testStmtName},
			},
			stmts: map[string]*nodes.PrepareStmt{
				"1": {Name: &testStmtName},
			},
			portals: map[string]*portal{
				"": {srcPreparedStatement: testStmtName},
			},
		}
		sess.handleTransactionState(protocol.NotInTransaction)
		require.Len(t, sess.pendingStmts, 1)
		require.Len(t, sess.stmts, 1)
		require.Len(t, sess.portals, 1)
	})
}

func TestSession_prepare(t *testing.T) {
	t.Run("parses and stores statements from parse messages", func(t *testing.T) {
		query := "SELECT 1"
		sess := &session{pendingStmts: map[string]*nodes.PrepareStmt{}}
		msgs, err := sess.prepare(&pgproto3.Parse{
			Name:  testStmtName,
			Query: query,
		})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		require.Equal(t, protocol.Message(protocol.ParseComplete), msgs[0])
		require.NotNil(t, sess.pendingStmts[testStmtName])
	})
	t.Run("fails to parse invalid statements", func(t *testing.T) {
		testStmtName := "test"
		query := "invalid"
		sess := &session{pendingStmts: map[string]*nodes.PrepareStmt{}}
		msgs, err := sess.prepare(&pgproto3.Parse{
			Name:  testStmtName,
			Query: query,
		})
		require.Error(t, err)
		require.Len(t, msgs, 0)
		require.Nil(t, sess.pendingStmts[testStmtName])
	})
}

func TestSession_bind(t *testing.T) {
	query := "SELECT 1"
	t.Run("binds a portal", func(t *testing.T) {
		sess := &session{pendingStmts: map[string]*nodes.PrepareStmt{}, portals: map[string]*portal{}}
		sess.storePreparedStatement(&nodes.PrepareStmt{
			Name:  &testStmtName,
			Query: nodes.String{Str: query},
		})
		// temporary hack for the test. transaction logic will be implemented on the next PR
		sess.stmts = sess.pendingStmts
		msgs, err := sess.bind(&pgproto3.Bind{
			PreparedStatement: testStmtName,
		})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		require.Equal(t, protocol.Message(protocol.BindComplete), msgs[0])
		require.Len(t, sess.portals, 1)
		require.NotNil(t, sess.portals[""])
		require.Equal(t, testStmtName, sess.portals[""].srcPreparedStatement)
	})
	t.Run("fails if statement not found", func(t *testing.T) {
		sess := &session{pendingStmts: map[string]*nodes.PrepareStmt{}, portals: map[string]*portal{}}
		sess.storePreparedStatement(&nodes.PrepareStmt{
			Name:  &testStmtName,
			Query: nodes.String{Str: query},
		})
		sess.stmts = sess.pendingStmts
		msgs, err := sess.bind(&pgproto3.Bind{
			PreparedStatement: "other",
		})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		require.True(t, msgs[0].IsError())
		require.Len(t, sess.portals, 0)
	})
}

func TestSession_describe(t *testing.T) {
	query := "SELECT 1"
	sess := &session{pendingStmts: map[string]*nodes.PrepareStmt{}, portals: map[string]*portal{}}
	sess.storePreparedStatement(&nodes.PrepareStmt{
		Name:  &testStmtName,
		Query: nodes.String{Str: query},
		Argtypes: nodes.List{
			Items: []nodes.Node{
				nodes.TypeName{
					TypeOid: 16,
					Names: nodes.List{
						Items: []nodes.Node{
							nodes.String{Str: "bool"},
						},
					},
				},
			},
		},
	})
	t.Run("parameter description of prepared statement", func(t *testing.T) {
		// temporary hack for the test. transaction logic will be implemented on the next PR
		sess.stmts = sess.pendingStmts
		msgs, err := sess.describe(&pgproto3.Describe{
			ObjectType: protocol.DescribeStatement,
			Name:       testStmtName,
		})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		msg := pgproto3.ParameterDescription{}
		err = msg.Decode(msgs[0][5:])
		require.NoError(t, err)
		require.Len(t, msg.ParameterOIDs, 1)
		require.Equal(t, uint32(16), msg.ParameterOIDs[0])
	})
}

func (p *pgStoryScriptsRunner) testStory(t *testing.T, story *pg_stories.Story) {
	conn, killStory := p.init()
	frontend, err := pgproto3.NewFrontend(conn, conn)
	if err != nil {
		require.NoError(t, err)
	}

	story.Frontend = frontend
	story.Filter = filterStartupMessages
	timer := time.NewTimer(time.Second * 2)
	go func() {
		t := <-timer.C
		killStory <- t
	}()
	err = story.Run(t, killStory)
	if err != nil {
		require.NoError(t, err)
	}
}

func (p *pgStoryScriptsRunner) run(t *testing.T) {
	currentDirPath, err := os.Getwd()
	if err != nil {
		require.NoError(t, err)
	}

	dataTestPath := filepath.Join(currentDirPath, TestDataFolder)
	err = filepath.Walk(dataTestPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			require.NoError(t, err)
		}

		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}

		t.Run(info.Name(), func(t *testing.T) {
			storyBuilder := pg_stories.NewBuilder(file, startupSeq()...)
			for {
				story, name, err := storyBuilder.ParseNext()
				if err != nil {
					require.NoError(t, err)
				}
				if story == nil {
					break
				}
				t.Run(name, func(t *testing.T) {
					p.testStory(t, story)
				})
			}
		})

		return nil
	})

	if err != nil {
		require.NoError(t, err)
	}
}

func filterStartupMessages(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.ParameterStatus:
		return false
	case *pgproto3.BackendKeyData:
		return false
	case *pgproto3.NotificationResponse:
		return false
	}
	return true
}

const TestDataFolder = "testdata"

func TestSession_Serve(t *testing.T) {
	t.Skip("extended query flow is still under development so we skip the tests")

	currentDirPath, err := os.Getwd()
	if err != nil {
		require.NoError(t, err)
	}

	dataTestPath := filepath.Join(currentDirPath, TestDataFolder)

	runner := &pgStoryScriptsRunner{
		baseFolder: dataTestPath,
		init: func() (net.Conn, chan interface{}) {
			f, b := net.Pipe()
			srv := server{
				authenticator: &noPasswordAuthenticator{},
				queryer:       &mockQueryer{},
			}

			killStory := make(chan interface{})

			sess := &session{Conn: b, Server: &srv}
			go func() {
				err = sess.Serve()
				if err != nil {
					killStory <- err
					require.NoError(t, err)
				}
			}()
			return f, killStory
		},
	}
	runner.run(t)
}

func TestRealServer(t *testing.T) {
	t.Skip("used for local development as baseline testing against postgres server")

	currentDirPath, err := os.Getwd()
	if err != nil {
		require.NoError(t, err)
	}

	dataTestPath := filepath.Join(currentDirPath, TestDataFolder)

	runner := &pgStoryScriptsRunner{
		baseFolder: dataTestPath,
		init: func() (net.Conn, chan interface{}) {

			conn, err := net.Dial("tcp", "127.0.0.1:5432")
			if err != nil {
				require.NoError(t, err)
				return nil, nil
			}

			killStory := make(chan interface{})

			return conn, killStory
		},
	}

	runner.run(t)
}

type mockConn struct {
	b *bytes.Buffer
}

func (m *mockConn) Read(p []byte) (n int, err error) {
	return m.b.Read(p)
}

func (m *mockConn) Write(p []byte) (n int, err error) {
	return m.b.Write(p)
}

func (m *mockConn) Close() error {
	return nil
}

func TestSession_startUp(t *testing.T) {
	srv := server{
		authenticator: &noPasswordAuthenticator{},
		queryer:       &mockQueryer{},
	}
	buf := bytes.NewBuffer([]byte{})
	t.Run("protocol version 3.0", func(t *testing.T) {
		s := session{Server: &srv, Conn: &mockConn{b: buf}}
		buf.Write([]byte{
			0, 0, 0, 8, // length
			0, 3, 0, 0, // 3.0
		})
		err := s.startUp()
		require.NoError(t, err)

		reader, err := pgproto3.NewFrontend(buf, nil)
		require.NoError(t, err)

		msg, err := reader.Receive()
		require.NoError(t, err)
		require.IsType(t, &pgproto3.Authentication{}, msg)

		msg, err = reader.Receive()
		require.NoError(t, err)
		require.IsType(t, &pgproto3.ParameterStatus{}, msg)
		require.Equal(t, "client_encoding", msg.(*pgproto3.ParameterStatus).Name)
		require.Equal(t, "utf8", msg.(*pgproto3.ParameterStatus).Value)

		msg, err = reader.Receive()
		require.NoError(t, err)
		require.IsType(t, &pgproto3.BackendKeyData{}, msg)
	})

	t.Run("cancel", func(t *testing.T) {
		canceled := false
		s := session{Server: &srv, Secret: 123, Conn: &mockConn{b: buf}, CancelFunc: func() {
			canceled = true
		}}
		allSessions.Store(int32(1), &s)
		cancelMessage := make([]byte, 16)
		binary.BigEndian.PutUint32(cancelMessage[0:4], 16)
		binary.BigEndian.PutUint32(cancelMessage[4:8], 80877102)
		binary.BigEndian.PutUint32(cancelMessage[8:12], uint32(1))
		binary.BigEndian.PutUint32(cancelMessage[12:16], uint32(123))
		_, err := buf.Write(cancelMessage)
		require.NoError(t, err)

		_ = s.startUp()
		require.NoError(t, err)
		require.Equal(t, true, canceled)
	})
}
