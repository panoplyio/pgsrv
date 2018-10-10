package pgsrv

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReadyMsg(t *testing.T) {
	msg := readyMsg()
	require.Equal(t, []byte{'Z', 0, 0, 0, 5, 'I'}, []byte(msg))
}

func TestCompleteMsg(t *testing.T) {
	msg := completeMsg("meh")
	expectedMsg := []byte{
		'C',        // type
		0, 0, 0, 8, // size
		109, 101, 104, // meh in bytes
		0, // null terminator
	}

	require.Equal(t, expectedMsg, []byte(msg))
}
