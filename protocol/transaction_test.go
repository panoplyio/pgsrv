package protocol

import (
	"testing"
)

func BenchmarkChan(b *testing.B) {
	c := make(chan int, b.N)
	for n := 0; n < b.N; n++ {
		c <- n
	}
	for n := 0; n < b.N; n++ {
		<-c
	}
}

func BenchmarkQueue(b *testing.B) {
	var q []int
	for n := 0; n < b.N; n++ {
		q = append(q, n)
	}
	for n := 0; n < b.N; n++ {
		_, q = q[0], q[1:]
	}
}
