package pgsrv

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNoPassword_authenticate(t *testing.T) {
	np := &noPassword{}
	actualResult := np.authenticate()
	expectedResult := msg{'R', 0, 0, 0, 8, 0, 0, 0, 0}
	require.Equal(t, actualResult, expectedResult)
}
