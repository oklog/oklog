package record

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestDynamicReader(t *testing.T) {
	for _, c := range []struct {
		name  string
		err   error
		input string
		exp   []string
	}{
		{
			name:  "basic",
			input: "topic_A foo1\ntopic_B foo2\ntopic_A foo2\n",
			exp:   []string{"topic_A foo1\n", "topic_B foo2\n", "topic_A foo2\n"},
		}, {
			name:  "no-final-newline",
			input: "topic_A foo1\ntopic_B foo2\ntopic_A foo2",
			exp:   []string{"topic_A foo1\n", "topic_B foo2\n"},
		}, {
			name:  "no-topic",
			input: "topic_A foo1\nabcdef\ntopic_B foo2\n",
			exp:   []string{"topic_A foo1\n"},
			err:   ErrIllegalTopicName,
		}, {
			name:  "empty-record",
			input: "topic_A foo1\n\ntopic_B foo2\n",
			exp:   []string{"topic_A foo1\n"},
			err:   ErrIllegalTopicName,
		}, {
			name:  "bad-topic",
			input: "to~pic foo1\n",
			err:   ErrIllegalTopicName,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			var res []string
			var rec []byte
			var err error

			r := NewDynamicReader(bytes.NewBufferString(c.input))
			for {
				rec, err = r()
				if err != nil {
					break
				}
				res = append(res, string(rec))
			}
			if err != io.EOF && err != c.err {
				t.Fatalf("unexpected error: want %q, got %q", c.err, err)
			}
			if !reflect.DeepEqual(c.exp, res) {
				t.Fatalf("unexpected records: want (%v), got (%v)", c.exp, res)
			}
		})
	}
}
func TestStaticReader(t *testing.T) {
	for _, c := range []struct {
		name  string
		err   error
		input string
		exp   []string
	}{
		{
			name:  "basic",
			input: "foo1\nfoo2\nfoo3\n",
			exp:   []string{"topic_A foo1\n", "topic_A foo2\n", "topic_A foo3\n"},
		},
		{
			name:  "no-newline",
			input: "foo1\nfoo2\nfoo3",
			exp:   []string{"topic_A foo1\n", "topic_A foo2\n"},
		},
		{
			name:  "empty-record",
			input: "foo1\n\nfoo2\n",
			exp:   []string{"topic_A foo1\n", "topic_A \n", "topic_A foo2\n"},
			err:   ErrIllegalTopicName,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			var res []string
			var rec []byte
			var err error

			r := StaticReaderFactory([]byte("topic_A"))(bytes.NewBufferString(c.input))
			for {
				rec, err = r()
				if err != nil {
					break
				}
				res = append(res, string(rec))
			}
			if err != io.EOF && err != c.err {
				t.Fatalf("unexpected error: want %q, got %q", c.err, err)
			}
			if !reflect.DeepEqual(c.exp, res) {
				t.Fatalf("unexpected records: want (%v), got (%v)", c.exp, res)
			}
		})
	}
}
