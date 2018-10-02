package pgsrv

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReadyMsg(t *testing.T) {
	msg := readyMsg()
	require.Equal(t, []byte{'Z', 0, 0, 0, 5, 'I'}, []byte(msg))
}
