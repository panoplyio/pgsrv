package protocol

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransaction_Read(t *testing.T) {
	buf := bytes.Buffer{}
	comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
	p := &Transport{W: comm, R: comm, initialized: true}
	trans := &transaction{transport: p, in: []Message{}, out: []Message{}}

	_, err := comm.Write([]byte{'P', 0, 0, 0, 4})
	require.NoError(t, err)

	err = comm.Flush()
	require.NoError(t, err)

	m, err := trans.Read()
	require.NoError(t, err)
	require.NotNil(t, m,
		"expected to receive message from transaction. got nil")

	require.Equalf(t, 1, len(trans.in),
		"expected exactly 1 message in transaction incoming buffer. actual: %d", len(trans.in))

	require.Equalf(t, byte('P'), trans.in[0].Type(),
		"expected type of the only message in transaction incoming buffer to be 'P'. actual: %c", trans.in[0].Type())

	require.Equalf(t, 0, len(trans.out),
		"expected no message to exist in transaction's outgoing message buffer. actual buffer length: %d", len(trans.out))

	err = trans.Write(CommandComplete(""))
	require.NoError(t, err)

	require.Equalf(t, 1, len(trans.out),
		"expected exactly one message in transaction's outgoind message buffer. actual messages count: %d", len(trans.out))
}
