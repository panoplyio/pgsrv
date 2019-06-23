package protocol

import (
	"github.com/jackc/pgx/pgproto3"
	"io"
)

// NewHandler creates a protocol
func NewHandler(rw io.ReadWriter) *Handler {
	b, _ := pgproto3.NewBackend(rw, nil)
	return &Handler{
		w: rw,
		r: b,
	}
}

// Handler manages the underlying wire protocol between backend and frontend.
type Handler struct {
	w           io.Writer
	r           *pgproto3.Backend
	transaction *transaction
}

func (t *Handler) beginTransaction() {
	t.transaction = &transaction{handler: t, in: []pgproto3.FrontendMessage{}, out: []Message{}}
}

func (t *Handler) endTransaction() (err error) {
	err = t.transaction.flush()
	t.transaction = nil
	return
}

// NextFrontendMessage reads and returns a single message from the connection when available.
// if within a transaction, the transaction will read from the connection,
// otherwise a ReadyForQuery message will first be sent to the frontend and then reading
// a single message from the connection will happen
//
// NextFrontendMessage expects to be called only after a call to Handshake without an error response
// otherwise, an error is returned
func (t *Handler) NextFrontendMessage() (msg pgproto3.FrontendMessage, err error) {
	if t.transaction != nil {
		msg, err = t.transaction.NextFrontendMessage()
	} else {
		// when not in transaction, client waits for ReadyForQuery before sending next message
		err = t.Write(ReadyForQuery)
		if err != nil {
			return
		}
		msg, err = t.readFrontendMessage()
	}
	if err != nil {
		return
	}

	if t.transaction == nil {
		switch msg.(type) {
		case *pgproto3.Parse, *pgproto3.Bind, *pgproto3.Describe:
			t.beginTransaction()
		}
	} else {
		switch msg.(type) {
		case *pgproto3.Query, *pgproto3.Sync:
			err = t.endTransaction()
		}
	}

	return
}

func (t *Handler) readFrontendMessage() (pgproto3.FrontendMessage, error) {
	return t.r.Receive()
}

// Write writes the provided message to the client connection
func (t *Handler) Write(m Message) error {
	if t.transaction != nil {
		return t.transaction.Write(m)
	}
	return t.write(m)
}

func (t *Handler) write(m Message) error {
	_, err := t.w.Write(m)
	return err
}
