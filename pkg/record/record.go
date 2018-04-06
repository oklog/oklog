package record

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

// ErrIllegalTopicName is returned if a topic's character sequence is invalid.
var ErrIllegalTopicName = errors.New("illegal topic name")

// Reader emits records.
// It returns io.EOF if the underlying record source has no more data.
type Reader func() (record []byte, err error)

// RecordReaderFactory returns a new RecordReader backed by the given reader.
type ReaderFactory func(io.Reader) Reader

// NewDynamicReader returns a record reader that expects each input record from r
// to start with a space-delimited topic identifier.
func NewDynamicReader(r io.Reader) Reader {
	br := bufio.NewReader(r)

	return func() ([]byte, error) {
		l, err := br.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		// Validate that a correct topic prefix exists.
		if i := bytes.IndexByte(l, ' '); i < 0 || !IsValidTopic(l[:i]) {
			return nil, ErrIllegalTopicName
		}
		return l, nil
	}
}

// StaticReaderFactory returns a RecordReaderFactory that prefixes all records
// with the given bytes as topic.
// Callers must ensure the topic identifier is valid.
func StaticReaderFactory(topic []byte) ReaderFactory {
	topic = append(topic, ' ')

	return func(r io.Reader) Reader {
		br := bufio.NewReader(r)

		return func() ([]byte, error) {
			l, err := br.ReadBytes('\n')
			if err != nil {
				return nil, err
			}
			return append(topic, l...), nil
		}
	}
}

// IsValidTopic ensures that the a topic only contains allowed characters. It implements
// the regex [a-zA-Z0-9][a-zA-Z0-9_-]*
// It takes a byte slice to avoid memory allocations at the caller.
func IsValidTopic(b []byte) bool {
	for i, c := range b {
		if i > 0 && (c == '_' || c == '-') {
			continue
		}
		if ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') {
			continue
		}
		return false
	}
	return len(b) > 0
}
