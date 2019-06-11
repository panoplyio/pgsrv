package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	pgstories "github.com/avivklas/pg-stories"
	"github.com/jackc/pgx/pgproto3"
	"io"
	"net"
	"testing"
	"time"
)

func TestProtocol_StartUp(t *testing.T) {

	t.Run("supported protocol version", func(t *testing.T) {

		buf := bytes.Buffer{}
		comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
		p := &Protocol{W: comm, R: comm}

		if _, err := comm.Write([]byte{0, 0, 0, 8, 0, 3, 0, 0, 0, 0, 0, 0}); err != nil {
			t.Fatal(err)
		}
		if err := comm.Flush(); err != nil {
			t.Fatal(err)
		}

		if _, err := p.StartUp(); err != nil {
			t.Fatal(err)
		}

	})

	t.Run("unsupported protocol version", func(t *testing.T) {

		buf := bytes.Buffer{}
		comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
		p := &Protocol{W: comm, R: comm}

		if _, err := comm.Write([]byte{0, 0, 0, 8, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
			t.Fatal(err)
		}
		if err := comm.Flush(); err != nil {
			t.Fatal(err)
		}

		if _, err := p.StartUp(); err == nil {
			t.Fatal(fmt.Errorf("expected error of unsupporting version. got none"))
		}

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
		if err != nil {
			t.Fatal(err)
		}

		p := NewProtocol(b, b)
		p.initialized = true

		msg := make(chan Message)
		go func() {
			if m, err := p.Read(); err != nil {
				t.Fatal(err)
				return
			} else {
				msg <- m
			}
		}()

		if m, err := frontend.Receive(); err != nil {
			t.Fatal(err)
		} else {
			if _, ok := m.(*pgproto3.ReadyForQuery); !ok {
				t.Fatal("expected protocol to send ReadyForQuery message")
			}
		}

		if _, err := f.Write([]byte{'Q', 0, 0, 0, 4}); err != nil {
			t.Fatal(err)
		}

		res := <-msg

		if res.Type() != 'Q' {
			t.Fatalf("expected protocol to identify sent message as type 'Q'. actual: %c", res.Type())
		}

		if p.transaction != nil {
			t.Fatal("expected protocol not to start transaction")
		}

	})

	t.Run("extended query message flow", func(t *testing.T) {

		t.Run("starts transaction", func(t *testing.T) {

			f, b := net.Pipe()

			p := NewProtocol(b, b)
			p.initialized = true

			go func() {
				for {
					if _, err := p.Read(); err != nil {
						t.Fatal(err)
						return
					}
				}
			}()

			err := runStory(t, f, []pgstories.Step{
				&pgstories.Response{&pgproto3.ReadyForQuery{}},
				&pgstories.Command{&pgproto3.Parse{}},
				&pgstories.Command{&pgproto3.Bind{}},
			})

			if err != nil {
				t.Fatal(err)
			}

			if p.transaction == nil {
				t.Fatal("expected protocol to start transaction")
			}

		})

		t.Run("ends transaction", func(t *testing.T) {

			f, b := net.Pipe()

			p := NewProtocol(b, b)
			p.initialized = true

			go func() {
				for {
					if m, err := p.Read(); err != nil {
						t.Fatal(err)
						return
					} else {
						switch m.Type() {
						case Parse:
							err = p.Write(ParseComplete())
						case Bind:
							err = p.Write(BindComplete())
						}
						if err != nil {
							t.Fatal(err)
						}
					}
				}
			}()

			err := runStory(t, f, []pgstories.Step{
				&pgstories.Response{&pgproto3.ReadyForQuery{}},
				&pgstories.Command{&pgproto3.Parse{}},
				&pgstories.Command{&pgproto3.Bind{}},
				&pgstories.Command{&pgproto3.Sync{}},
				&pgstories.Response{&pgproto3.ParseComplete{}},
				&pgstories.Response{&pgproto3.BindComplete{}},
				&pgstories.Response{&pgproto3.ReadyForQuery{}},
			})

			if err != nil {
				t.Fatal(err)
			}

			if p.transaction != nil {
				t.Fatal("expected protocol to end transaction")
			}

		})

	})

}
