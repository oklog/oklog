package main

import (
	"io"
	"testing"
)

func TestBufferedScanner(t *testing.T) {
	sb := newSliceBuffer(3)
	bs := bufferedScanner{
		buf: sb,
	}
	exp := []string{"hi", "ho", "yi", "yo"}
	pr, pw := io.Pipe()
	go bs.Consume(pr)
	for _, e := range exp {
		pw.Write([]byte(e + "\n"))
	}

	for i, e := range exp {
		if i >= 3 {
			break
		}
		ok := bs.Scan()
		if !ok {
			t.Errorf("Buffer scan should return a value")
		}
		if bs.Text() != e {
			t.Errorf("Buffer should return a specific value [%s] but received [%s]", e, bs.Text())
		}
		//	}
	}
}
