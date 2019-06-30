package protocol

import (
	"github.com/jackc/pgx/pgproto3"
	"io"
)

// TransactionState is used as a return with every message read for commit and rollback implementation
type TransactionState int

const (
	// NotInTransaction states that transaction is not active and operations should auto-commit
	NotInTransaction TransactionState = 1 + iota
	// InTransaction states that transaction is active and operations should not commit
	InTransaction
	// TransactionEnded states that the current transaction has finished and has to commit
	TransactionEnded
	// TransactionFailed states that the current transaction has failed and has to roll-back
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

func (t *Transport) affectTransaction(msg pgproto3.FrontendMessage) (ts TransactionState, err error) {
	if t.transaction == nil {
		switch msg.(type) {
		case *pgproto3.Parse, *pgproto3.Bind, *pgproto3.Describe:
			t.beginTransaction()
			ts = InTransaction
		default:
			ts = NotInTransaction
		}
	} else {
		if t.transaction.hasError() {
			ts = TransactionFailed
		}
		switch msg.(type) {
		case *pgproto3.Query, *pgproto3.Sync:
			err = t.endTransaction()
			if err != nil {
				ts = TransactionFailed
			} else if ts == 0 {
				ts = TransactionEnded
			}
		default:
			if ts == 0 {
				ts = InTransaction
			}
		}
	}
	return
}

// NextFrontendMessage reads and returns a single message from the connection when available.
// if within a transaction, the transaction will read from the connection,
// otherwise a ReadyForQuery message will first be sent to the frontend and then reading
// a single message from the connection will happen
//
// NextFrontendMessage expects to be called only after a call to Handshake without an error response
// otherwise, an error is returned
func (t *Transport) NextFrontendMessage() (msg pgproto3.FrontendMessage, ts TransactionState, err error) {
	if t.transaction == nil {
		// when not in transaction, client waits for ReadyForQuery before sending next message
		err = t.Write(ReadyForQuery)
		if err != nil {
			return
		}
		msg, err = t.readFrontendMessage()
	} else {
		msg, err = t.transaction.NextFrontendMessage()
	}
	if err != nil {
		return
	}

	ts, err = t.affectTransaction(msg)
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
