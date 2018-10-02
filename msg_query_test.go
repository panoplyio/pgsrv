package pgsrv

import (
	"github.com/stretchr/testify/require"
	"testing"
)

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
