package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func TestTransaction_Read(t *testing.T) {

	buf := bytes.Buffer{}
	comm := bufio.NewReadWriter(bufio.NewReader(&buf), bufio.NewWriter(&buf))
	p := &Protocol{W: comm, R: comm, initialized: true}
	trans := &transaction{p: p, in: []Message{}, out: []Message{}}

	if _, err := comm.Write([]byte{'P', 0, 0, 0, 4}); err != nil {
		t.Fatal(err)
	}

	if err := comm.Flush(); err != nil {
		t.Fatal(err)
	}

	if m, err := trans.Read(); err != nil {
		t.Fatal(err)
	} else if m == nil {
		t.Fatal(fmt.Errorf("expected exactly 1 message in transaction incomming buffer. actual: %d", len(trans.in)))
	}

	if len(trans.in) != 1 {
		t.Fatal(fmt.Errorf("expected exactly 1 message in transaction incomming buffer. actual: %d", len(trans.in)))
	}

	if trans.in[0].Type() != 'P' {
		t.Fatal(fmt.Errorf("expected type of the only message in transaction incomming buffer to be 'P'. actual: %c", trans.in[0].Type()))
	}

	if len(trans.out) != 0 {
		t.Fatal(fmt.Errorf("expected no message to exist in transaction's outgoind message buffer. actual buffer length: %d", len(trans.out)))
	}

	if err := trans.Write(CommandComplete("")); err != nil {
		t.Fatal(err)
	}

	if len(trans.out) != 1 {
		t.Fatal(fmt.Errorf("expected exactly one message in transaction's outgoind message buffer. actual messages count: %d", len(trans.out)))
	}

}
