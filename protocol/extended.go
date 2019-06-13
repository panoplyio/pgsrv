package protocol

// ParseComplete is sent when backend parsed a prepared statement successfully.
var ParseComplete = []byte{'1', 0, 0, 0, 4}

// BindComplete is sent when backend prepared a portal and finished planning the query.
var BindComplete = []byte{'2', 0, 0, 0, 4}

// CreatesTransaction tells weather this is a frontend message that should start/continue a transaction
func (m *Message) CreatesTransaction() bool {
	return m.Type() == Parse || m.Type() == Bind
}

// EndsTransaction tells weather this is a frontend message that should end the current transaction
func (m *Message) EndsTransaction() bool {
	return m.Type() == Query || m.Type() == Sync
}
