package pgsrv

import (
	"bytes"
	"crypto/md5"
	"github.com/stretchr/testify/require"
	"testing"
)

var authOKMessage = msg{'R', 0, 0, 0, 8, 0, 0, 0, 0}
var fatalMarker = []byte{
	83, 70, 65, 84, 65, 76,
}

func TestAuthOKMsg(t *testing.T) {
	actualResult := authOKMsg()
	expectedResult := authOKMessage

	require.Equal(t, expectedResult, actualResult)
}

func TestNoPassword_authenticate(t *testing.T) {
	rw := &mockMessageReadWriter{output: []msg{}}
	args := map[string]interface{}{
		"user": "this-is-user",
	}

	np := &noPasswordAuthenticator{}
	ok, err := np.authenticate(rw, args)

	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, []msg{authOKMessage}, rw.messages)
}

func TestAuthenticationClearText_authenticate(t *testing.T) {
	passwordRequest := msg{
		'R',
		0, 0, 0, 8, // length
		0, 0, 0, 3, // clear text auth type
	}
	passwordMessage := msg{
		'p',
		0, 0, 0, 8,
		109, 101, 104, 0, // 'meh'
	}
	rw := &mockMessageReadWriter{output: []msg{passwordMessage}}
	args := map[string]interface{}{
		"user": "this-is-user",
	}
	pp := &constantPasswordProvider{password: []byte("meh")}

	a := &clearTextAuthenticator{pp}

	t.Run("valid password", func(t *testing.T) {
		defer rw.Reset()
		ok, err := a.authenticate(rw, args)

		require.True(t, ok)
		require.NoError(t, err)
		expectedMessages := []msg{
			passwordRequest,
			authOKMessage,
		}
		require.Equal(t, expectedMessages, rw.messages)
	})

	t.Run("invalid password", func(t *testing.T) {
		defer rw.Reset()
		pp.password = []byte("shtoot")
		ok, err := a.authenticate(rw, args)

		require.False(t, ok)
		require.Equal(t, passwordRequest, rw.messages[0])
		require.True(t, bytes.Contains(rw.messages[1], fatalMarker))
		require.NoError(t, err)
	})

	t.Run("invalid message type", func(t *testing.T) {
		defer rw.Reset()
		rw = &mockMessageReadWriter{output: []msg{
			{'q', 0, 0, 0, 5, 1},
		}}
		ok, err := a.authenticate(rw, args)

		require.False(t, ok)
		require.Equal(t, passwordRequest, rw.messages[0])
		require.True(t, bytes.Contains(rw.messages[1], fatalMarker))
		require.NoError(t, err)
	})
}

func TestAuthenticationMD5_authenticate(t *testing.T) {
	passwordRequest := msg{
		'R',
		0, 0, 0, 12, // length
		0, 0, 0, 5, // md5 auth type
	}
	rw := &mockMD5MessageReadWriter{
		user: "postgres",
		pass: []byte("test"),
		salt: []byte{},
	}
	args := map[string]interface{}{
		"user": "postgres",
	}
	pp := &md5ConstantPasswordProvider{password: []byte("test")}

	a := &md5Authenticator{pp}

	t.Run("valid password", func(t *testing.T) {
		defer rw.Reset()
		ok, err := a.authenticate(rw, args)

		require.True(t, ok)
		require.NoError(t, err)
		require.True(t, bytes.Contains(rw.messages[0], passwordRequest))
		require.Equal(t, authOKMessage, rw.messages[1])
	})

	t.Run("invalid password", func(t *testing.T) {
		defer rw.Read()
		pp.password = []byte("shtoot")
		ok, err := a.authenticate(rw, args)

		require.False(t, ok)
		require.True(t, bytes.Contains(rw.messages[0], passwordRequest))
		require.True(t, bytes.Contains(rw.messages[1], fatalMarker))
		require.NoError(t, err)
	})

	t.Run("invalid message type", func(t *testing.T) {
		defer rw.Reset()
		rw := &mockMessageReadWriter{output: []msg{
			{'q', 0, 0, 0, 5, 1},
		}}
		ok, err := a.authenticate(rw, args)

		require.False(t, ok)
		require.True(t, bytes.Contains(rw.messages[0], passwordRequest))
		require.True(t, bytes.Contains(rw.messages[1], fatalMarker))
		require.NoError(t, err)
	})
}

func TestHashWithSalt(t *testing.T) {
	user := "postgres"
	pass := []byte("test")
	salt := []byte{196, 53, 49, 235}
	hash := md5.Sum(append(pass, []byte(user)...))

	// actual hash received from psql using the above variables
	expectedHash := []byte{
		109, 100, 53, 97, 97, 51, 102, 56, 98, 56,
		55, 97, 57, 51, 52, 97, 52, 53, 48, 52,
		52, 101, 49, 102, 98, 50, 100, 57, 48, 55,
		48, 99, 98, 56, 48,
	}

	actualHash := hashWithSalt(hash[:], salt)
	require.Equal(t, expectedHash, actualHash)
}

func TestGetRandomSalt(t *testing.T) {
	var lastSalt []byte
	for i := 0; i < 100; i++ {
		salt := getRandomSalt()
		require.Equal(t, len(salt), 4)
		require.NotEqual(t, lastSalt, salt)
		lastSalt = salt
	}
}

func TestExtractPassword(t *testing.T) {
	t.Run("regular password", func(t *testing.T) {
		passwordMessage := msg{
			'p',
			0, 0, 0, 9,
			42, 42, 42, 42,
			0,
		}

		expectedResult := []byte{42, 42, 42, 42}
		actualResult := extractPassword(passwordMessage)
		require.Equal(t, expectedResult, actualResult)
	})

	t.Run("empty password", func(t *testing.T) {
		passwordMessage := msg{
			'p',
			0, 0, 0, 5,
			0,
		}

		expectedResult := []byte{}
		actualResult := extractPassword(passwordMessage)
		require.Equal(t, expectedResult, actualResult)
	})
}

// mockMessageReadWriter implements messageReadWriter and outputs the provided output
// message by message, looped.
type mockMessageReadWriter struct {
	output        []msg
	currentOutput int
	messages      []msg
}

func (rw *mockMessageReadWriter) Read() (msg, error) {
	return rw.output[rw.currentOutput%len(rw.output)], nil
}

func (rw *mockMessageReadWriter) Write(m msg) error {
	rw.messages = append(rw.messages, m)
	return nil
}

func (rw *mockMessageReadWriter) Reset() {
	rw.messages = make([]msg, 0)
}

// mockMD5MessageReadWriter implements messageReadWriter and outputs password
// hashed with the salt received in Write() method
type mockMD5MessageReadWriter struct {
	user     string
	pass     []byte
	salt     []byte
	messages []msg
}

func (rw *mockMD5MessageReadWriter) Read() (msg, error) {
	message := msg{
		'p',
		0, 0, 0, 25,
	}
	hash := md5.Sum(append(rw.pass, []byte(rw.user)...))
	message = append(message, hashWithSalt(hash[:], rw.salt)...)
	message = append(message, 0)
	return message, nil
}

func (rw *mockMD5MessageReadWriter) Write(m msg) error {
	rw.salt = m[9:len(m)]
	rw.messages = append(rw.messages, m)
	return nil
}

func (rw *mockMD5MessageReadWriter) Reset() {
	rw.messages = make([]msg, 0)
}
