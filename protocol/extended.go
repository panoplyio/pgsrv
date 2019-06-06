package protocol

func (m *Message) CreatesTransaction() bool {
	return m.Type() == Parse || m.Type() == Bind
}

func (m *Message) EndsTransaction() bool {
	return m.Type() == Query || m.Type() == Sync
}

// ParseComplete is sent when backend parsed a prepared statement successfully.
func ParseComplete() Message {
	return []byte{'1'}
}

// BindComplete is sent when backend prepared a portal and finished planning the query.
func BindComplete() Message {
	return []byte{'2'}
}
