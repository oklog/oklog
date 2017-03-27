package main

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBufferedForwarderRemaining(t *testing.T) {
	onwardWriter := bytes.NewBufferString("")
	msgCount := 10
	bufferSize := 5
	bf := newBufferedForwarder(5, onwardWriter, "pfx ")
	bf.add(fmt.Sprintf("%02d", 0))
	if bf.remaining != 1 {
		t.Errorf("remaining should be 1")
	}
	bf.process()
	if bf.remaining != 0 {
		t.Errorf("remaining should be 0")
	}
	for i := 0; i < msgCount; i++ {
		bf.add(fmt.Sprintf("%02d", i))
	}
	if bf.remaining != int64(bufferSize) {
		t.Errorf("remaining should be %d", bufferSize)
	}

}

func TestBufferedForwarder(t *testing.T) {
	onwardWriter := bytes.NewBufferString("")
	msgCount := 10
	bufferSize := 5
	prefix := "pfx "
	bf := newBufferedForwarder(5, onwardWriter, prefix)
	for i := 0; i < msgCount; i++ {
		bf.add(fmt.Sprintf("%03d", i))
	}
	for i := 0; i < msgCount; i++ {
		done, remaining := bf.process()
		if done || !remaining {
			break
		}
	}
	s := onwardWriter.String()
	if len(s) != (3+len(prefix)+1)*bufferSize {
		t.Errorf("Expected to receive 5 messages")
	}
	t.Logf("%s", s)
	t.Logf("%d", len(s))
}
