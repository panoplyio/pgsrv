package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// NewTransport creates a protocol
func NewTransport(r io.Reader, w io.Writer) *Transport {
	return &Transport{
		R: r,
		W: w,
	}
}

// Transport manages the underlying wire protocol between backend and frontend.
type Transport struct {
	R           io.Reader
	W           io.Writer
	initialized bool
	transaction *transaction
}

// StartUp handles the very first messages exchange between frontend and backend of new session
func (t *Transport) StartUp() (Message, error) {
	// read the initial connection startup message
	msg, err := t.Read()
	if err != nil {
		return nil, err
	}

	if msg.IsCancel() {
		return msg, nil
	}

	if msg.IsTLSRequest() {
		// currently we don't support TLS.
		err := t.Write(TLSResponse(false))
		if err != nil {
			return nil, err
		}

		msg, err = t.Read()
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
	t.transaction = &transaction{transport: t, in: []Message{}, out: []Message{}}
}

func (t *Transport) endTransaction() (err error) {
	err = t.transaction.flush()
	t.transaction = nil
	return
}

// NextMessage reads and returns a single message from the connection when available.
// if within a transaction, the transaction will read from the connection,
// otherwise a ReadyForQuery message will first be sent to the frontend and then reading
// a single message from the connection will happen
//
// NextMessage expects to be called only after a call to StartUp without an error response
// otherwise, an error is returned
func (t *Transport) NextMessage() (msg Message, err error) {
	if !t.initialized {
		err = fmt.Errorf("transport not yet initialized")
		return
	}
	if t.transaction != nil {
		msg, err = t.transaction.Read()
	} else {
		// when not in transaction, client waits for ReadyForQuery before sending next message
		err = t.Write(ReadyForQuery)
		if err != nil {
			return
		}
		msg, err = t.Read()
	}
	if err != nil {
		return
	}

	if msg.CreatesTransaction() && t.transaction == nil {
		t.beginTransaction()
	} else if msg.EndsTransaction() && t.transaction != nil {
		err = t.endTransaction()
	}

	return
}

// Read reads and returns a single message from the connection.
func (t *Transport) Read() (Message, error) {
	typeChar := make([]byte, 1)

	if t.initialized {
		// we've already started up, so all future messages are MUST start with
		// a single-byte type identifier.
		_, err := t.R.Read(typeChar)
		if err != nil {
			return nil, err
		}
	}
	// read the actual body of the message
	msg, err := t.readBody()
	if err != nil {
		return nil, err
	}

	if typeChar[0] != 0 {

		// we have a typed-message, prepend it to the message body by first
		// creating a new message that's 1-byte longer than the body in order to
		// make room in memory for the type byte
		body := msg
		msg = make([]byte, len(body)+1)

		// fixing the type byte at the beginning (position 0) of the new message
		msg[0] = typeChar[0]

		// finally append the body to the new message, starting from position 1
		copy(msg[1:], body)
	}

	return Message(msg), nil
}

// readBody reads the body of the next message in the connection. The body is
// comprised of an Int32 body-length (N), inclusive of the length itself
// followed by N-bytes of the actual body.
func (t *Transport) readBody() ([]byte, error) {
	// messages starts with an Int32 Length of message contents in bytes,
	// including self.
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(t.R, lenBytes)
	if err != nil {
		return nil, err
	}

	// convert the 4-bytes to int
	length := int(binary.BigEndian.Uint32(lenBytes))

	// read the remaining bytes in the message
	msg := make([]byte, length)
	_, err = io.ReadFull(t.R, msg[4:]) // keep 4 bytes for the length
	if err != nil {
		return nil, err
	}

	// append the message content to the length bytes in order to rebuild the
	// original message in its entirety
	copy(msg[:4], lenBytes)
	return msg, nil
}

// Write writes the provided message to the client connection
func (t *Transport) Write(m Message) error {
	if t.transaction != nil {
		return t.transaction.Write(m)
	}
	return t.write(m)
}

func (t *Transport) write(m Message) error {
	_, err := t.W.Write(m)
	return err
}
