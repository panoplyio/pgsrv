package pgsrv

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStartupVersion(t *testing.T) {
	t.Run("typed message", func(t *testing.T) {
		m := &msg{'p', 0, 0, 0, 5}
		_, err := m.StartupVersion()

		require.Error(t, err)
	})

	t.Run("untyped message", func(t *testing.T) {
		m := &msg{
			0, 0, 0, 8,
			4, 210, 22, 47,
		}
		version, err := m.StartupVersion()

		require.NoError(t, err)
		require.Equal(t, "1234.5679", version)
	})
}
