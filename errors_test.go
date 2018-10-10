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

func TestUnrecognized(t *testing.T) {
	e := Unrecognized("thing %s", "meh").(*err)
	require.Equal(t, "42000", e.Code())
	require.Equal(t, -1, e.Position())
	require.Equal(t, "unrecognized thing meh", e.Error())
}

func TestInvalid(t *testing.T) {
	e := Invalid("thing %s", "meh").(*err)
	require.Equal(t, "42000", e.Code())
	require.Equal(t, -1, e.Position())
	require.Equal(t, "invalid thing meh", e.Error())
}

func TestDisallowed(t *testing.T) {
	e := Disallowed("thing %s", "meh").(*err)
	require.Equal(t, "42000", e.Code())
	require.Equal(t, -1, e.Position())
	require.Equal(t, "disallowed thing meh", e.Error())
}

func TestUnsupported(t *testing.T) {
	e := Unsupported("thing %s", "meh").(*err)
	require.Equal(t, "0A000", e.Code())
	require.Equal(t, -1, e.Position())
	require.Equal(t, "unsupported thing meh", e.Error())
}

func TestWithSeverity(t *testing.T) {
	t.Run("error is nil", func(t *testing.T) {
		err := WithSeverity(nil, "thing")
		require.Nil(t, err)
	})

	t.Run("real error", func(t *testing.T) {
		e := &mockErr{}
		es := WithSeverity(e, "minor")
		require.NotNil(t, es)
		require.Equal(t, "minor", es.(*err).Severity())
	})
}

func TestWithDetail(t *testing.T) {
	t.Run("error is nil", func(t *testing.T) {
		err := WithDetail(nil, "thing")
		require.Nil(t, err)
	})

	t.Run("real error", func(t *testing.T) {
		e := &mockErr{}
		es := WithDetail(e, "some details")
		require.NotNil(t, es)
		require.Equal(t, "some details", es.(*err).Detail())
	})
}

func TestWithHint(t *testing.T) {
	t.Run("error is nil", func(t *testing.T) {
		err := WithHint(nil, "this is a hint")
		require.Nil(t, err)
	})

	t.Run("real error", func(t *testing.T) {
		e := &mockErr{}
		es := WithHint(e, "hint!")
		require.NotNil(t, es)
		require.Equal(t, "hint!", es.(*err).Hint())
	})
}

func TestWithPosition(t *testing.T) {
	t.Run("error is nil", func(t *testing.T) {
		err := WithPosition(nil, 13)
		require.Nil(t, err)
	})

	t.Run("real error", func(t *testing.T) {
		e := fmt.Errorf("this is a regular error")
		es := WithPosition(e, 13)
		require.NotNil(t, es)
		require.Equal(t, 13, es.(*err).Position())
	})
}

type mockErr struct{}

func (*mockErr) Severity() string { return "BAD" }
func (*mockErr) Code() string     { return "13" }
func (*mockErr) Error() string    { return "This is bad" }
func (*mockErr) Detail() string   { return "Some detail" }
func (*mockErr) Hint() string     { return "A hint" }
func (*mockErr) Position() int    { return 42 }
