package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

func NewHandshake(rw io.ReadWriter) *Handshake {
	return &Handshake{rw: rw}
}

// Handshake handles the very first message passing of the protocol
type Handshake struct {
	rw     io.ReadWriter
	passed bool
}

// Write implements MessageReadWriter
func (h *Handshake) Write(m Message) error {
	_, err := h.rw.Write(m)
	return err
}

// Read implements MessageReadWriter
func (h *Handshake) Read() (Message, error) {
	if h.passed {
		return h.readTypedMessage()
	}
	return h.readRawMessage()
}

// Init receives and validates the very first message from the frontend per session.
// it may send message back to the frontend if needed to skip SSL request since it
// currently not supported.
//
// once done, Init must not be called again, or error will be returned.
func (h *Handshake) Init() (res Message, err error) {
	if h.passed {
		err = fmt.Errorf("handshake already passed")
		return
	}

	// read the initial connection startup message
	res, err = h.readRawMessage()
	if err != nil {
		return nil, err
	}

	if res.IsCancel() {
		return res, nil
	}

	// ssl request. see: SSLRequest in https://www.postgresql.org/docs/current/protocol-message-formats.html
	if res.IsTLSRequest() {
		// currently we don't support TLS.
		_, err = h.rw.Write(TLSResponse(false))
		if err != nil {
			return nil, err
		}

		res, err = h.readRawMessage()
		if err != nil {
			return nil, err
		}
	}

	v, err := res.StartupVersion()
	if err != nil {
		return nil, err
	}
	if v != "3.0" {
		return nil, fmt.Errorf("unsupported protocol version %s", v)
	}

	h.passed = true

	return res, nil

}

func (h *Handshake) readTypedMessage() (Message, error) {
	msgType := Message(make([]byte, 1))
	_, err := h.rw.Read(msgType)
	if err != nil {
		return nil, err
	}

	body, err := h.readRawMessage()
	if err != nil {
		return nil, err
	}
	return append(msgType, body...), nil
}

// readRawMessage reads un-typed message in the connection. The message is
// comprised of an Int32 body-length (N), inclusive of the length itself
// followed by N-bytes of the actual body.
func (h *Handshake) readRawMessage() ([]byte, error) {
	// messages starts with an Int32 Length of message contents in bytes,
	// including self.
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(h.rw, lenBytes)
	if err != nil {
		return nil, err
	}

	// convert the 4-bytes to int
	length := int(binary.BigEndian.Uint32(lenBytes))

	// read the remaining bytes in the message
	res := make([]byte, length)
	_, err = io.ReadFull(h.rw, res[4:]) // keep 4 bytes for the length
	if err != nil {
		return nil, err
	}

	// append the message content to the length bytes in order to rebuild the
	// original message in its entirety
	copy(res[:4], lenBytes)
	return res, nil
}
