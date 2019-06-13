package protocol

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewMsg(t *testing.T) {
	bs := []byte{'p', 0, 0, 0, 5}
	actualMessage := Message(bs)
	expectedMessage := Message{'p', 0, 0, 0, 5}

	require.Equal(t, expectedMessage, actualMessage)
}

func TestType(t *testing.T) {
	t.Run("empty message", func(t *testing.T) {
		m := Message{}
		mt := m.Type()
		expectedType := byte(0)

		require.Equal(t, expectedType, mt)
	})

	t.Run("regular message", func(t *testing.T) {
		m := Message{'p', 0, 0, 0, 5}
		mt := m.Type()
		expectedType := byte('p')

		require.Equal(t, expectedType, mt)
	})
}
