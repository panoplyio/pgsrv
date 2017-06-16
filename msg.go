package pgsrv

// Msg is just an alias for a slice of bytes that exposes common operations on
// Postgres' client-server protocol messages.
// see: https://www.postgresql.org/docs/9.2/static/protocol-message-formats.html
// for postgres specific list of message formats
type msg []byte

// Type returns a string (single-char) representing the message type. The full
// list of available types is available in the aforementioned documentation.
func (m msg) Type() byte {
    var b byte
    if len(m) > 0 {
        b = m[0]
    }
    return b
}

func newMsg(b []byte) msg {
    return msg(b)
}
