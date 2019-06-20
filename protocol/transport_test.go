package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	pgstories "github.com/panoplyio/pg-stories"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"testing"
	"time"
)

func TestProtocol_StartUp(t *testing.T) {
	t.Run("supported protocol version", func(t *testing.T) {
		buf := bytes.Buffer{}
		comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
		p := &Transport{W: comm, R: comm}

		_, err := comm.Write([]byte{
			0, 0, 0, 8, // length
			0, 3, 0, 0, // 3.0
			0, 0, 0, 0,
		})
		require.NoError(t, err)

		err = comm.Flush()
		require.NoError(t, err)

		_, err = p.StartUp()
		require.NoError(t, err)
	})

	t.Run("unsupported protocol version", func(t *testing.T) {
		buf := bytes.Buffer{}
		comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
		p := &Transport{W: comm, R: comm}

		_, err := comm.Write([]byte{
			0, 0, 0, 8, // length
			0, 2, 0, 0, // 2.0
			0, 0, 0, 0,
		})
		require.NoError(t, err)

		err = comm.Flush()
		require.NoError(t, err)

		_, err = p.StartUp()
		require.Error(t, err, "expected error of unsupported version. got none")
	})
}

func runStory(t *testing.T, conn io.ReadWriter, steps []pgstories.Step) error {
	frontend, err := pgproto3.NewFrontend(conn, conn)
	if err != nil {
		return err
	}

	story := &pgstories.Story{
		Steps:    steps,
		Frontend: frontend,
	}

	sigKill := make(chan interface{})
	timer := time.NewTimer(time.Second * 2)
	go func() {
		<-timer.C
		sigKill <- fmt.Errorf("timeout")
	}()

	err = story.Run(t, sigKill)
	if err != nil {
		timer.Stop()
	}
	return err
}

func TestProtocol_Read(t *testing.T) {
	t.Run("standard message flow", func(t *testing.T) {
		f, b := net.Pipe()

		frontend, err := pgproto3.NewFrontend(f, f)
		require.NoError(t, err)

		transport := NewTransport(b, b)
		transport.initialized = true

		msg := make(chan Message)
		go func() {
			m, err := transport.NextMessage()
			require.NoError(t, err)

			msg <- m
		}()

		m, err := frontend.Receive()
		require.NoError(t, err)
		require.IsType(t, &pgproto3.ReadyForQuery{}, m, "expected protocol to send ReadyForQuery message")

		_, err = f.Write([]byte{'Q', 0, 0, 0, 4})
		require.NoError(t, err)

		res := <-msg
		require.Equalf(t, byte('Q'), res.Type(), "expected protocol to identify sent message as type 'Q'. actual: %c", res.Type())

		require.Nil(t, transport.transaction, "expected protocol not to start transaction")
	})

	t.Run("extended query message flow", func(t *testing.T) {
		t.Run("starts transaction", func(t *testing.T) {
			f, b := net.Pipe()

			transport := NewTransport(b, b)
			transport.initialized = true

			go func() {
				for {
					_, err := transport.NextMessage()
					require.NoError(t, err)
				}
			}()

			err := runStory(t, f, []pgstories.Step{
				&pgstories.Response{BackendMessage: &pgproto3.ReadyForQuery{}},
				&pgstories.Command{FrontendMessage: &pgproto3.Parse{}},
				&pgstories.Command{FrontendMessage: &pgproto3.Bind{}},
			})
			require.NoError(t, err)

			require.NotNil(t, transport.transaction, "expected protocol to start transaction")
		})

		t.Run("ends transaction", func(t *testing.T) {
			f, b := net.Pipe()

			transport := NewTransport(b, b)
			transport.initialized = true

			go func() {
				for {
					m, err := transport.NextMessage()
					require.NoError(t, err)

					switch m.Type() {
					case Parse:
						err = transport.Write(ParseComplete)
					case Bind:
						err = transport.Write(BindComplete)
					}
					require.NoError(t, err)
				}
			}()

			err := runStory(t, f, []pgstories.Step{
				&pgstories.Response{BackendMessage: &pgproto3.ReadyForQuery{}},
				&pgstories.Command{FrontendMessage: &pgproto3.Parse{}},
				&pgstories.Command{FrontendMessage: &pgproto3.Bind{}},
				&pgstories.Command{FrontendMessage: &pgproto3.Sync{}},
				&pgstories.Response{BackendMessage: &pgproto3.ParseComplete{}},
				&pgstories.Response{BackendMessage: &pgproto3.BindComplete{}},
				&pgstories.Response{BackendMessage: &pgproto3.ReadyForQuery{}},
			})

			require.NoError(t, err)

			require.Nil(t, transport.transaction, "expected protocol to end transaction")
		})
	})
}
