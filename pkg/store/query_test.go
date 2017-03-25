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
		"01BC3NABW20000000000000000": {
			ulid.MustParse("01BC3NABW20000000000000000"),
			mustParseRFC3339("2017-03-25T21:17:54.946Z"), // truncated
		},
		"01BC3NABW21234567890123456": {
			ulid.MustParse("01BC3NABW21234567890123456"),
			mustParseRFC3339("2017-03-25T21:17:54.946Z"), // truncated
		},
		"2017-03-25T21:18:20.05731891Z": {
			ulid.MustParse("01BC3NB4CS0000000000000000"),
			mustParseRFC3339("2017-03-25T21:18:20.05731891Z"),
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
	t, err := time.ParseInLocation(time.RFC3339Nano, s, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}
