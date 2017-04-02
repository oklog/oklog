package main

import (
	"fmt"
	"testing"
)

func TestSliceBuffer(t *testing.T) {
	testSliceBuffer(t)
}

func BenchmarkSliceBuffer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testSliceBuffer(b)
	}
}

func testSliceBuffer(t testing.TB) {
	var b buffer
	bufferSize := 5
	b = newSliceBuffer(bufferSize)
	testBuffer(t, b, bufferSize)
}

func testBuffer(t testing.TB, b buffer, bufferSize int) {
	msgCount := 10
	b.Put(fmt.Sprintf("%02d", 0))
	b.Get()
	for i := 0; i < msgCount; i++ {
		b.Put(fmt.Sprintf("%02d", i))
	}
	for i := 0; i < bufferSize; i++ {
		t.Logf(b.Get())
	}
}
