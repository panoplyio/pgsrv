package protocol

import (
	"github.com/jackc/pgx/pgproto3"
	"io"
)

// TransactionStatus is used as a return with every message read for commit and rollback implementation
type TransactionStatus int

const (
	// TransactionEnded states that the current transaction has finished and has to commit
	TransactionEnded TransactionStatus = 1 + iota
	// TransactionEnded states that the current transaction has failed and has to roll-back
	TransactionFailed
)

// NewTransport creates a Transport
func NewTransport(rw io.ReadWriter) *Transport {
	b, _ := pgproto3.NewBackend(rw, nil)
	return &Transport{
		w: rw,
		r: b,
	}
}

// Transport manages the underlying wire protocol between backend and frontend.
type Transport struct {
	w           io.Writer
	r           *pgproto3.Backend
	transaction *transaction
}

func (t *Transport) beginTransaction() {
	t.transaction = &transaction{transport: t}
}

func (t *Transport) endTransaction() (err error) {
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
func (t *Transport) NextFrontendMessage() (msg pgproto3.FrontendMessage, ts TransactionStatus, err error) {
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
			if t.transaction.hasError() {
				ts = TransactionFailed
			} else {
				ts = TransactionEnded
			}
			err = t.endTransaction()
		}
	}

	return
}

func (t *Transport) readFrontendMessage() (pgproto3.FrontendMessage, error) {
	return t.r.Receive()
}

// Write writes the provided message to the client connection
func (t *Transport) Write(m Message) error {
	if t.transaction != nil {
		return t.transaction.Write(m)
	}
	return t.write(m)
}

func (t *Transport) write(m Message) error {
	_, err := t.w.Write(m)
	return err
}
