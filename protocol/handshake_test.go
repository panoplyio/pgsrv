package protocol

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHandshake_Init(t *testing.T) {
	t.Run("supported protocol version", func(t *testing.T) {
		buf := bytes.Buffer{}
		comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
		handshake := NewHandshake(comm)

		_, err := comm.Write([]byte{
			0, 0, 0, 8, // length
			0, 3, 0, 0, // 3.0
		})
		require.NoError(t, err)

		err = comm.Flush()
		require.NoError(t, err)

		_, err = handshake.Init()
		require.NoError(t, err)
		require.Equal(t, true, handshake.passed)
	})

	t.Run("unsupported protocol version", func(t *testing.T) {
		buf := bytes.Buffer{}
		comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
		handshake := NewHandshake(comm)

		_, err := comm.Write([]byte{
			0, 0, 0, 8, // length
			0, 2, 0, 0, // 2.0
		})
		require.NoError(t, err)

		err = comm.Flush()
		require.NoError(t, err)

		_, err = handshake.Init()
		require.Error(t, err, "expected error of unsupported version. got none")
	})

	t.Run("call init twice returns an error", func(t *testing.T) {
		buf := bytes.Buffer{}
		comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
		handshake := NewHandshake(comm)

		_, err := comm.Write([]byte{
			0, 0, 0, 8, // length
			0, 3, 0, 0, // 3.0
		})
		require.NoError(t, err)

		err = comm.Flush()
		require.NoError(t, err)

		_, err = handshake.Init()
		require.NoError(t, err)

		_, err = handshake.Init()
		require.Error(t, err, "expected second call to handshake.Init() to return an error")
	})
}
