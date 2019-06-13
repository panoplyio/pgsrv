package protocol

// transaction represents a sequence of frontend and backend messages
// that apply only on commit. the purpose of transaction is to support
// extended query flow.
type transaction struct {
	p   *Protocol
	in  []Message // TODO: asses if we need it after implementation of prepared statements and portals is done
	out []Message // TODO: add size limit
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
		err = t.p.write(t.out[0])
		if err != nil {
			break
		}
		t.out = t.out[1:]
	}
	return
}
