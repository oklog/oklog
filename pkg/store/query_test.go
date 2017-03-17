package store

import (
	"reflect"
	"testing"
	"time"

	"github.com/oklog/ulid"
)

func TestQueryResultMerge(t *testing.T) {
	t.Skip("TODO(pb)")
}

func TestULIDOrTimeParse(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]ulidOrTime{
		"01BB6RQR190000000000000000": {
			ulid.MustParse("01BB6RQR190000000000000000"),
			mustParseRFC3339("2017-03-14T16:59:40.585+01:00"),
		},
		"2017-03-14T17:02:42.211235645+01:00": {
			ulid.MustParse("01BB6RX9D30000000000000000"),
			mustParseRFC3339("2017-03-14T17:02:42.211235645+01:00"),
		},
	} {
		t.Run(input, func(t *testing.T) {
			var have ulidOrTime
			if err := have.Parse(input); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, have) {
				t.Fatalf("want %+v, have %+v", want, have)
			}
		})
	}
}

func mustParseRFC3339(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}
