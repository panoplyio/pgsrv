package protocol

import (
	"bufio"
	"bytes"
	"github.com/jackc/pgx/pgproto3"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransaction_Read(t *testing.T) {
	buf := bytes.Buffer{}
	comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
	transport := NewTransport(comm, comm)
	transport.initialized = true
	trans := &transaction{transport: transport, in: []pgproto3.FrontendMessage{}, out: []Message{}}

	parseMsg := (&pgproto3.Parse{}).Encode([]byte{})
	_, err := comm.Write(parseMsg)
	require.NoError(t, err)

	err = comm.Flush()
	require.NoError(t, err)

	m, err := trans.NextFrontendMessage()
	require.NoError(t, err)
	require.NotNil(t, m,
		"expected to receive message from transaction. got nil")

	require.Equalf(t, 1, len(trans.in),
		"expected exactly 1 message in transaction incoming buffer. actual: %d", len(trans.in))

	require.IsTypef(t, &pgproto3.Parse{}, trans.in[0],
		"expected type of the only message in transaction incomming buffer to be %T. actual: %T", &pgproto3.Parse{}, trans.in[0])

	require.Equalf(t, 0, len(trans.out),
		"expected no message to exist in transaction's outgoing message buffer. actual buffer length: %d", len(trans.out))

	err = trans.Write(CommandComplete(""))
	require.NoError(t, err)

	require.Equalf(t, 1, len(trans.out),
		"expected exactly one message in transaction's outgoind message buffer. actual messages count: %d", len(trans.out))
}
