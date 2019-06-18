package protocol

import (
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgio"
)

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

// ParameterDescription is sent when backend received Describe message from frontend
// with ObjectType = 'S' - requesting to describe prepared statement with a provided name
func ParameterDescription(ps *pgx.PreparedStatement) Message {
	res := []byte{'t'}
	sp := len(res)
	res = pgio.AppendInt32(res, -1)

	res = pgio.AppendUint16(res, uint16(len(ps.ParameterOIDs)))
	for _, oid := range ps.ParameterOIDs {
		res = pgio.AppendUint32(res, uint32(oid))
	}

	pgio.SetInt32(res[sp:], int32(len(res[sp:])))

	return Message(res)
}
