package protocol

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReadyMsg(t *testing.T) {
	msg := ReadyForQuery
	require.Equal(t, []byte{'Z', 0, 0, 0, 5, 'I'}, []byte(msg))
}

func TestCompleteMsg(t *testing.T) {
	msg := CommandComplete("meh")
	expectedMsg := []byte{
		'C',        // type
		0, 0, 0, 8, // size
		109, 101, 104, // meh in bytes
		0, // null terminator
	}

	require.Equal(t, expectedMsg, []byte(msg))
}
