package postgressrv

// Type returns a string (single-char) representing the message type. The full
// list of available types is available in the aforementioned documentation.
func (m Msg) Type() byte {
    var b byte
    if len(m) > 0 {
        b = m[0]
    }
    return b
}

func NewMsg(b []byte) Msg {
    return Msg(b)
}
