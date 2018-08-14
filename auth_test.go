package pgsrv

import (
	"crypto/md5"
	"github.com/stretchr/testify/require"
	"testing"
)

var authOKMessage = msg{'R', 0, 0, 0, 8, 0, 0, 0, 0}

func TestAuthOKMsg(t *testing.T) {
	actualResult := authOKMsg()
	expectedResult := authOKMessage

	require.Equal(t, expectedResult, actualResult)
}

func TestNoPassword_authenticate(t *testing.T) {
	np := &noPasswordAuthenticator{}
	actualResult, err := np.authenticate()
	expectedResult := authOKMessage

	require.NoError(t, err)
	require.Equal(t, actualResult, expectedResult)
}

func TestAuthenticationClearText_authenticate(t *testing.T) {
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

	a := &clearTextAuthenticator{rw, args, pp}

	t.Run("valid password", func(t *testing.T) {
		expectedResult := authOKMessage
		actualResult, err := a.authenticate()

		require.NoError(t, err)
		require.Equal(t, expectedResult, actualResult)
	})

	t.Run("invalid password", func(t *testing.T) {
		pp.password = []byte("shtoot")
		_, err := a.authenticate()

		require.EqualError(t, err,
			"Password does not match for user \"this-is-user\"")
	})

	t.Run("invalid message type", func(t *testing.T) {
		a.rw = &mockMessageReadWriter{output: []msg{
			{'q', 0, 0, 0, 5, 1},
		}}
		_, err := a.authenticate()

		require.EqualError(t, err,
			"expected password response, got message type q")
	})
}

func TestAuthenticationMD5_authenticate(t *testing.T) {
	rw := &mockMD5MessageReadWriter{
		user: "postgres",
		pass: []byte("test"),
		salt: []byte{},
	}
	args := map[string]interface{}{
		"user": "postgres",
	}
	pp := &md5ConstantPasswordProvider{password: []byte("test")}

	a := &md5Authenticator{rw, args, pp}

	t.Run("valid password", func(t *testing.T) {
		expectedResult := authOKMessage
		actualResult, err := a.authenticate()

		require.NoError(t, err)
		require.Equal(t, expectedResult, actualResult)
	})

	t.Run("invalid password", func(t *testing.T) {
		pp.password = []byte("shtoot")
		_, err := a.authenticate()

		require.EqualError(t, err,
			"Password does not match for user \"postgres\"")
	})

	t.Run("invalid message type", func(t *testing.T) {
		a.rw = &mockMessageReadWriter{output: []msg{
			{'q', 0, 0, 0, 5, 1},
		}}
		_, err := a.authenticate()

		require.EqualError(t, err,
			"expected password response, got message type q")
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
}

func (rw *mockMessageReadWriter) Read() (msg, error) {
	return rw.output[rw.currentOutput%len(rw.output)], nil
}

func (rw *mockMessageReadWriter) Write(m msg) error { return nil }

// mockMD5MessageReadWriter implements messageReadWriter and outputs password
// hashed with the salt received in Write() method
type mockMD5MessageReadWriter struct {
	user string
	pass []byte
	salt []byte
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
	return nil
}
