package protocol

import (
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	"io"
)

// NewTransport creates a protocol
func NewTransport(r io.Reader, w io.Writer) *Transport {
	return &Transport{
		w: w,
		r: newReader(r),
	}
}

// Transport manages the underlying wire protocol between backend and frontend.
type Transport struct {
	w           io.Writer
	r           *reader
	initialized bool
	transaction *transaction
}

// StartUp handles the very first messages exchange between frontend and backend of new session
func (t *Transport) StartUp() (msg Message, err error) {
	// read the initial connection startup message
	msg, err = t.r.readRawMessage()
	if err != nil {
		return nil, err
	}

	if msg.IsCancel() {
		return msg, nil
	}

	// ssl request. see: SSLRequest in https://www.postgresql.org/docs/current/protocol-message-formats.html
	if msg.IsTLSRequest() {
		// currently we don't support TLS.
		err = t.Write(TLSResponse(false))
		if err != nil {
			return nil, err
		}

		msg, err = t.r.readRawMessage()
		if err != nil {
			return nil, err
		}
	}

	v, err := msg.StartupVersion()
	if err != nil {
		return nil, err
	}
	if v != "3.0" {
		return nil, fmt.Errorf("unsupported protocol version %s", v)
	}

	t.initialized = true

	return msg, nil
}

func (t *Transport) beginTransaction() {
	t.transaction = &transaction{transport: t, in: []pgproto3.FrontendMessage{}, out: []Message{}}
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
// NextFrontendMessage expects to be called only after a call to StartUp without an error response
// otherwise, an error is returned
func (t *Transport) NextFrontendMessage() (msg pgproto3.FrontendMessage, err error) {
	if !t.initialized {
		err = fmt.Errorf("transport not yet initialized")
		return
	}
	if t.transaction != nil {
		msg, err = t.transaction.NextFrontendMessage()
	} else {
		// when not in transaction, client waits for ReadyForQuery before sending next message
		err = t.Write(ReadyForQuery)
		if err != nil {
			return
		}
		msg, err = t.r.readFrontendMessage()
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

func (t *Transport) readFrontendMessage() (pgproto3.FrontendMessage, error) {
	return t.r.readFrontendMessage()
}

// Read reads and returns a single message from the connection.
func (t *Transport) Read() (msg Message, err error) {
	if t.initialized {
		return t.r.readTypedMessage()
	}
	return t.r.readRawMessage()
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

func newReader(r io.Reader) *reader {
	return &reader{r: r}
}

type reader struct {
	r           io.Reader
	frontReader *pgproto3.Backend
}

func (r *reader) readTypedMessage() (Message, error) {
	msgType := Message(make([]byte, 1))
	_, err := r.r.Read(msgType)
	if err != nil {
		return nil, err
	}

	body, err := r.readRawMessage()
	if err != nil {
		return nil, err
	}
	return append(msgType, body...), nil
}

// readRawMessage reads un-typed message in the connection. The message is
// comprised of an Int32 body-length (N), inclusive of the length itself
// followed by N-bytes of the actual body.
func (r *reader) readRawMessage() (Message, error) {
	// messages starts with an Int32 Length of message contents in bytes,
	// including self.
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(r.r, lenBytes)
	if err != nil {
		return nil, err
	}

	// convert the 4-bytes to int
	length := int(binary.BigEndian.Uint32(lenBytes))

	// read the remaining bytes in the message
	msg := make([]byte, length)
	_, err = io.ReadFull(r.r, msg[4:]) // keep 4 bytes for the length
	if err != nil {
		return nil, err
	}

	// append the message content to the length bytes in order to rebuild the
	// original message in its entirety
	copy(msg[:4], lenBytes)
	return msg, nil
}

// readFrontendMessage reads and returns a single decoded typed message from the connection.
func (r *reader) readFrontendMessage() (msg pgproto3.FrontendMessage, err error) {
	if r.frontReader == nil {
		r.frontReader, err = pgproto3.NewBackend(r.r, nil)
		if err != nil {
			return
		}
	}
	return r.frontReader.Receive()
}
