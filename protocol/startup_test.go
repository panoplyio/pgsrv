package protocol

import (
	"encoding/binary"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStartupVersion(t *testing.T) {
	t.Run("typed message", func(t *testing.T) {
		m := &Message{'p', 0, 0, 0, 5}
		_, err := m.StartupVersion()

		require.EqualError(t, err, "expected untyped startup message, got: 'p'")
	})

	t.Run("untyped message", func(t *testing.T) {
		m := &Message{
			0, 0, 0, 8,
			4, 210, 22, 47,
		}
		version, err := m.StartupVersion()

		require.NoError(t, err)
		require.Equal(t, "1234.5679", version)
	})
}

func TestStartupArgs(t *testing.T) {
	t.Run("typed message", func(t *testing.T) {
		m := &Message{'p', 0, 0, 0, 5}
		_, err := m.StartupArgs()

		require.EqualError(t, err, "expected untyped startup message, got: 'p'")
	})

	t.Run("untyped message", func(t *testing.T) {
		// an original message sent by psql client
		m := &Message{
			0, 0, 0, 84, 0, 3, 0, 0, 117, 115,
			101, 114, 0, 112, 111, 115, 116, 103, 114, 101,
			115, 0, 100, 97, 116, 97, 98, 97, 115, 101,
			0, 112, 111, 115, 116, 103, 114, 101, 115, 0,
			97, 112, 112, 108, 105, 99, 97, 116, 105, 111,
			110, 95, 110, 97, 109, 101, 0, 112, 115, 113,
			108, 0, 99, 108, 105, 101, 110, 116, 95, 101,
			110, 99, 111, 100, 105, 110, 103, 0, 85, 84,
			70, 56, 0, 0,
		}

		args, err := m.StartupArgs()
		expectedArgs := map[string]interface{}{
			"user":             "postgres",
			"database":         "postgres",
			"application_name": "psql",
			"client_encoding":  "UTF8",
		}

		require.NoError(t, err)
		require.Equal(t, expectedArgs, args)
	})

	t.Run("untyped message, no params", func(t *testing.T) {
		m := &Message{
			0, 0, 0, 9,
			1, 2, 3, 4,
			5, // this is an extra byte without a null terminator
		}

		args, err := m.StartupArgs()
		expectedArgs := make(map[string]interface{})

		require.NoError(t, err)
		require.Equal(t, expectedArgs, args)
	})
}

func TestIsTLSRequest(t *testing.T) {
	t.Run("tls", func(t *testing.T) {
		// an actual message with version 1234.5679
		m := &Message{0, 0, 0, 8, 4, 210, 22, 47}

		isTLS := m.IsTLSRequest()
		require.True(t, isTLS)
	})

	t.Run("not tls", func(t *testing.T) {
		// an actual message with version 1234.5679 with modified last byte
		m := &Message{0, 0, 0, 8, 4, 210, 22, 42}

		isTLS := m.IsTLSRequest()
		require.False(t, isTLS)
	})
}

func TestIsTerminate(t *testing.T) {
	t.Run("terminate", func(t *testing.T) {
		m := &Message{Terminate, 0, 0, 0, 5}

		isTerminate := m.IsTerminate()
		require.True(t, isTerminate)
	})

	t.Run("not terminate", func(t *testing.T) {
		m := &Message{'x', 0, 0, 0, 5}

		isTerminate := m.IsTerminate()
		require.False(t, isTerminate)
	})
}

func TestTlsResponseMsg(t *testing.T) {
	t.Run("supported", func(t *testing.T) {
		m := TLSResponse(true)

		require.Equal(t, Message{'S'}, m)
	})

	t.Run("not supported", func(t *testing.T) {
		m := TLSResponse(false)

		require.Equal(t, Message{'N'}, m)
	})
}

func TestKeyDataMsg(t *testing.T) {
	m := BackendKeyData(1325119140, 942490198)
	expectedMessage := Message{75, 0, 0, 0, 12, 78, 251, 182, 164, 56, 45, 66, 86}

	require.Equal(t, expectedMessage, m)
}

func TestIsCancel(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		// an actual message with version 1234.5678
		m := &Message{0, 0, 0, 8, 4, 210, 22, 46}

		isCancel := m.IsCancel()
		require.True(t, isCancel)
	})

	t.Run("not cancel", func(t *testing.T) {
		// an actual message with version 1234.5678 with modified last byte
		m := &Message{0, 0, 0, 8, 4, 210, 22, 42}

		isCancel := m.IsCancel()
		require.False(t, isCancel)
	})
}

func TestCancelKeyData(t *testing.T) {
	t.Run("not a cancel message", func(t *testing.T) {
		m := &Message{1, 2, 3, 4, 5, 6, 7, 8}
		_, _, err := m.CancelKeyData()

		require.EqualError(t, err, "not a cancel message")
	})

	t.Run("cancel message", func(t *testing.T) {
		m := Message{
			0, 0, 0, 16, 4, 210, 22, 46, // 1234.5678
			0, 0, 0, 0, // pid
			0, 0, 0, 0, // secret
		}

		expectedPid := uint32(1)
		expectedSecret := uint32(2)

		binary.BigEndian.PutUint32(m[8:12], expectedPid)
		binary.BigEndian.PutUint32(m[12:16], expectedSecret)

		pid, secret, err := m.CancelKeyData()

		require.NoError(t, err)
		require.Equal(t, int32(expectedPid), pid)
		require.Equal(t, int32(expectedSecret), secret)
	})
}

func TestParameterStatus(t *testing.T) {
	m := ParameterStatus("client_encoding", "utf8")
	expectedMessage := Message{
		'S',
		0, 0, 0, 25,
		'c', 'l', 'i', 'e', 'n', 't', '_', 'e', 'n', 'c', 'o', 'd', 'i', 'n', 'g', 0,
		'u', 't', 'f', '8', 0,
	}

	require.Equal(t, expectedMessage, m)
}
