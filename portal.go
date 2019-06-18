package pgsrv

type portal struct {
	srcPreparedStatement string
	parameters           [][]byte
}
