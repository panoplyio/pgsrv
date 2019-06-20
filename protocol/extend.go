package protocol

// ParseComplete is sent when backend parsed a prepared statement successfully
var ParseComplete = []byte{'1', 0, 0, 0, 4}

// BindComplete is sent when backend prepared a portal and finished planning the query
var BindComplete = []byte{'2', 0, 0, 0, 4}

// CreatesTransaction tells weather this is a frontend message that should start/continue a transaction
func (m *Message) CreatesTransaction() bool {
	return m.Type() == Parse || m.Type() == Bind
}

// EndsTransaction tells weather this is a frontend message that should end the current transaction
func (m *Message) EndsTransaction() bool {
	return m.Type() == Query || m.Type() == Sync
}

// transaction represents a sequence of frontend and backend messages
// that apply only on commit. the purpose of transaction is to support
// extended query flow.
type transaction struct {
	transport *Transport
	in        []Message // TODO: asses if we need it after implementation of prepared statements and portals is done
	out       []Message // TODO: add size limit
}

// Read uses Transport to read the next message into the transaction's incoming messages buffer
func (t *transaction) Read() (msg Message, err error) {
	if msg, err = t.transport.Read(); err == nil {
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
		err = t.transport.write(t.out[0])
		if err != nil {
			break
		}
		t.out = t.out[1:]
	}
	return
}
