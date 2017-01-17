package ioext_test

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/oklog/oklog/pkg/ioext"
)

func TestOffsetReader(t *testing.T) {
	t.Parallel()

	r := ioext.OffsetReader(strings.NewReader("foobar"), 3)

	data, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := string(data), "bar"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
