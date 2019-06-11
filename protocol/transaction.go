package protocol

// transaction represents a sequence of frontend and backend messages
// that apply only on commit. the purpose of transaction is to support
// extended query flow.
type transaction struct {
	p   *Protocol
	in  []Message
	out []Message
}

// Read uses Protocol to read the next message into the transaction's incoming messages buffer
func (t *transaction) Read() (msg Message, err error) {
	if msg, err = t.p.read(); err == nil {
		t.in = append(t.in, msg)
	}
	return
}

// Write writes the provided message into the transaction's outgoing messages buffer
func (t *transaction) Write(msg Message) error {
	t.out = append(t.out, msg)
	return nil
}

func (t *transaction) flush() (err error) {
	for len(t.out) > 0 {
		m := t.out[0]
		if len(t.out) > 1 {
			t.out = t.out[1:]
		} else {
			t.out = nil
		}
		err = t.p.write(m)
		if err != nil {
			break
		}
	}
	return
}
