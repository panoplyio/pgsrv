package pgsrv

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFromErr(t *testing.T) {
	t.Run("already *err", func(t *testing.T) {
		err := fmt.Errorf("this is an error")
		expectedErr := fromErr(err)

		actualErr := fromErr(expectedErr)
		require.Equal(t, expectedErr, actualErr)
	})

	t.Run("all interfaces", func(t *testing.T) {
		e := &mockErr{}
		actualErr := fromErr(e)

		require.Equal(t, "BAD", actualErr.Severity())
		require.Equal(t, "13", actualErr.Code())
		require.Equal(t, "This is bad", actualErr.Error())
		require.Equal(t, "Some detail", actualErr.Detail())
		require.Equal(t, "A hint", actualErr.Hint())
		require.Equal(t, 42, actualErr.Position())
	})
}

type mockErr struct{}

func (*mockErr) Severity() string { return "BAD" }
func (*mockErr) Code() string     { return "13" }
func (*mockErr) Error() string    { return "This is bad" }
func (*mockErr) Detail() string   { return "Some detail" }
func (*mockErr) Hint() string     { return "A hint" }
func (*mockErr) Position() int    { return 42 }
