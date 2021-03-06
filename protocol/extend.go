package protocol

import (
	"github.com/jackc/pgx/pgio"
	"github.com/jackc/pgx/pgproto3"
	nodes "github.com/lfittl/pg_query_go/nodes"
)

// ParseComplete is sent when backend parsed a prepared statement successfully
var ParseComplete = []byte{'1', 0, 0, 0, 4}

// BindComplete is sent when backend prepared a portal and finished planning the query
var BindComplete = []byte{'2', 0, 0, 0, 4}

// Describe message object types
const (
	DescribeStatement = 'S'
	DescribePortal    = 'P'
)

// ParameterDescription is sent when backend received Describe message from frontend
// with ObjectType = 'S' - requesting to describe prepared statement with a provided name
func ParameterDescription(ps *nodes.PrepareStmt) (Message, error) {
	res := []byte{'t'}
	sp := len(res)
	res = pgio.AppendInt32(res, -1)

	res = pgio.AppendUint16(res, uint16(len(ps.Argtypes.Items)))
	for _, v := range ps.Argtypes.Items {
		res = pgio.AppendUint32(res, uint32(v.(nodes.TypeName).TypeOid))
	}

	pgio.SetInt32(res[sp:], int32(len(res[sp:])))

	return Message(res), nil
}

// transaction represents a sequence of frontend and backend messages
// that apply only on commit. the purpose of transaction is to support
// extended query flow.
type transaction struct {
	transport *Transport
	in        []pgproto3.FrontendMessage // TODO: asses if we need it after implementation of prepared statements and portals is done
	out       []Message                  // TODO: add size limit
}

// NextFrontendMessage uses Transport to read the next message into the transaction's incoming messages buffer
func (t *transaction) NextFrontendMessage() (msg pgproto3.FrontendMessage, err error) {
	if msg, err = t.transport.readFrontendMessage(); err == nil {
		t.in = append(t.in, msg)
	}
	return
}

// Write writes the provided message into the transaction's outgoing messages buffer
func (t *transaction) Write(msg Message) error {
	if t.hasError() {
		return nil
	}
	t.out = append(t.out, msg)
	return nil
}

func (t *transaction) hasError() bool {
	return len(t.out) > 0 && t.out[len(t.out)-1].IsError()
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
