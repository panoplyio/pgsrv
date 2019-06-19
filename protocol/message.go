package protocol

// frontend message types
const (
	Query     = 'Q'
	Terminate = 'X'
	Parse     = 'P'
	Bind      = 'B'
	Describe  = 'D'
	Execute   = 'E'
	Sync      = 'S'
)

// Message is just an alias for a slice of bytes that exposes common operations on
// Postgres' client-server protocol messages.
// see: https://www.postgresql.org/docs/current/protocol-message-formats.html
// for postgres specific list of message formats
type Message []byte

// Type returns a string (single-char) representing the message type. The full
// list of available types is available in the aforementioned documentation.
func (m Message) Type() byte {
	var b byte
	if len(m) > 0 {
		b = m[0]
	}
	return b
}

// MessageReadWriter describes objects that handle client-server communication.
// Objects implementing this interface are used by logic operations to send Message
// objects to frontend and receive Message back from it
type MessageReadWriter interface {
	Write(m Message) error
	Read() (Message, error)
}
