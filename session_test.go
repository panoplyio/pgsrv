package pgsrv

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	"github.com/lfittl/pg_query_go/nodes"
	"github.com/panoplyio/pg-stories"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	//t.Skip("extended query flow is still under development so we skip the tests")

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
