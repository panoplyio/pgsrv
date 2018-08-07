package pgsrv

type authenticator interface {
	authenticate() msg
}

type noPassword struct{}

func (*noPassword) authenticate() msg {
	return msg{'R', 0, 0, 0, 8, 0, 0, 0, 0}
}
