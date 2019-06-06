package protocol

type transaction struct {
	p   *Protocol
	in  []Message
	out []Message
}

func (t *transaction) Read() (msg Message, err error) {
	if msg, err = t.p.read(); err == nil {
		t.in = append(t.in, msg)
	}
	return
}

func (t *transaction) Write(msg Message) error {
	t.out = append(t.out, msg)
	return nil
}

func (t *transaction) flush() (err error) {
	for len(t.out) > 0 {
		m := t.out[0]
		if len(t.out) > 1 {
			t.out = t.out[1:]
		} else {
			t.out = nil
		}
		err = t.p.write(m)
		if err != nil {
			break
		}
	}
	return
}
