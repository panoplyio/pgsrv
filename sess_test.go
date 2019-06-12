package pgsrv

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	"github.com/lfittl/pg_query_go/nodes"
	"github.com/panoplyio/pg-stories"
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
		t.Fatal(err)
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
		t.Fatal(err)
	}
}

func (p *pgStoryScriptsRunner) run(t *testing.T) {
	currentDirPath, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	dataTestPath := filepath.Join(currentDirPath, TestDataFolder)
	err = filepath.Walk(dataTestPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
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
					t.Fatal(err)
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
		t.Fatal(err)
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
	// extended query flow is still under development so we skip the tests
	t.Skip()

	currentDirPath, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
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
					t.Fatal(err)
				}
			}()
			return f, killStory
		},
	}

	runner.run(t)

}

func TestRealServer(t *testing.T) {
	// this test is for baseline testing for developer against local postgres server
	t.Skip()

	currentDirPath, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	dataTestPath := filepath.Join(currentDirPath, TestDataFolder)

	runner := &pgStoryScriptsRunner{
		baseFolder: dataTestPath,
		init: func() (net.Conn, chan interface{}) {

			conn, err := net.Dial("tcp", "127.0.0.1:5432")
			if err != nil {
				t.Fatal(err)
				return nil, nil
			}

			killStory := make(chan interface{})

			return conn, killStory
		},
	}

	runner.run(t)

}
