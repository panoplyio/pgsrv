package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Protocol struct {
	R           io.Reader
	W           io.Writer
	initialized bool
	transaction *transaction
}

func (p *Protocol) StartUp() (Message, error) {
	// read the initial connection startup message
	msg, err := p.read()
	if err != nil {
		return nil, err
	}

	if msg.IsCancel() {
		return msg, nil
	}

	if msg.IsTLSRequest() {
		// currently we don't support TLS.
		err := p.Write(TlsResponseMsg(false))
		if err != nil {
			return nil, err
		}

		msg, err = p.Read()
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

	p.initialized = true

	return msg, nil
}

func (p *Protocol) beginTransaction() {
	p.transaction = &transaction{p: p, in: []Message{}, out: []Message{}}
}

func (p *Protocol) endTransaction() (err error) {
	err = p.transaction.flush()
	p.transaction = nil
	return
}

// Read reads and returns a single message from the connection.
//
// The Postgres protocol supports two types of messages: (1) untyped messages
// are only mostly present during the initial startup process and starts with
// the length of the message, followed by the content. (2) typed messages are
// similar to the untyped messages except that they're prefixed with a
// single-byte type character used to distinguish between the different message
// types (query, prepare, etc.), and are the normal messages used for most
// client-server communications.
//
// This method abstracts away this differentiation, returning the next available
// message whether it's typed or not.
func (p *Protocol) Read() (msg Message, err error) {
	if p.transaction != nil {
		msg, err = p.transaction.Read()
	} else {
		if p.initialized {
			err = p.Write(ReadyForQuery())
			if err != nil {
				return
			}
		}
		msg, err = p.read()
	}
	if err != nil {
		return
	}

	if msg.CreatesTransaction() && p.transaction == nil {
		p.beginTransaction()
	} else if msg.EndsTransaction() && p.transaction != nil {
		err = p.endTransaction()
	}

	return
}

func (p *Protocol) read() (Message, error) {
	typeChar := make([]byte, 1)

	if p.initialized {
		// we've already started up, so all future messages are MUST start with
		// a single-byte type identifier.
		_, err := p.R.Read(typeChar)
		if err != nil {
			return nil, err
		}
	}
	// read the actual body of the message
	msg, err := p.readBody()
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

// ReadMsgBody reads the body of the next message in the connection. The body is
// comprised of an Int32 body-length (N), inclusive of the length itself
// followed by N-bytes of the actual body.
func (p *Protocol) readBody() ([]byte, error) {

	// messages starts with an Int32 Length of message contents in bytes,
	// including self.
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(p.R, lenBytes)
	if err != nil {
		return nil, err
	}

	// convert the 4-bytes to int
	length := int(binary.BigEndian.Uint32(lenBytes))

	// read the remaining bytes in the message
	msg := make([]byte, length)
	_, err = io.ReadFull(p.R, msg[4:]) // keep 4 bytes for the length
	if err != nil {
		return nil, err
	}

	// append the message content to the length bytes in order to rebuild the
	// original message in its entirety
	copy(msg[:4], lenBytes)
	return msg, nil
}

// Write writes the provided protocol to the client connection
func (p *Protocol) Write(m Message) error {
	if p.transaction != nil {
		return p.transaction.Write(m)
	}
	return p.write(m)
}

func (p *Protocol) write(m Message) error {
	_, err := p.W.Write(m)
	return err
}
