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

func TestQueryText(t *testing.T) {
	t.Run("not a Q", func(t *testing.T) {
		bs := []byte{'p', 0, 0, 0, 5}
		msg := newMsg(bs)

		res, err := msg.QueryText()
		require.EqualError(t, err, "Not a query message: 'p'")
		require.Equal(t, "", res)
	})

	t.Run("Q with a string", func(t *testing.T) {
		bs := []byte{'Q', 0, 0, 0, 11}
		bs = append(bs, []byte("thing")...)
		msg := newMsg(bs)

		res, err := msg.QueryText()
		require.NoError(t, err)
		require.Equal(t, "thing", res)
	})

	t.Run("Q with an empty string", func(t *testing.T) {
		bs := []byte{'Q', 0, 0, 0, 5}
		bs = append(bs, []byte("")...)
		msg := newMsg(bs)

		res, err := msg.QueryText()
		require.NoError(t, err)
		require.Equal(t, "", res)

	})
}
