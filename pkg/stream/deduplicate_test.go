package stream

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid"
)

func Benchmark_Deduplicate(t *testing.B) {

	var (
		in     = make(chan []byte)
		tick   = make(chan time.Time)
		ticker = func(time.Duration) *time.Ticker { return &time.Ticker{C: tick} }
		window = 10 * time.Second
		out    = Deduplicate(in, window, ticker)
		count  = 900000000
		rec    []byte
		record string = `[FREEGO_WORK ] {"module":"gateway","level":"error","client":"","token":"","traceid":"-","rpcid":"0","tracetag":0,"servicefrom":17,"serviceto":0,"ipfrom":"","ipto":"","timestamp":"2018-02-10T12:27:55+08:00","interval":0,"file":"handler.go","line":293,"cnt":"调用服务时返回错误: {\"data\":{\"battery_buzz_light\":false},\"errors\":[{\"code\":50004,\"message\":\"内部微服务调用失败\"}]}","consoleinfo":{"phone":"18837058255","userid":"cn_admin_users_1518079483_c0eed8c2-bbe0-46da-bbcb-010d2988624a","client":"-"}}`
	)
	go func() {
		for i := 0; i < count; i++ {
			//time.Sleep(time.Millisecond)
			rec = []byte(fmt.Sprintf("%s %s", ulid.MustNew(ulid.Timestamp(time.Now()), nil).String(), record))
			in <- rec
		}
	}()
	// We haven't ticked the ticker yet, so no records can emerge.
	for i := 0; i < count; i++ {
		select {
		case record := <-out:
			t.Fatalf("unexpected record: %q", record)
		default:
			// good
		}
	}
}

func TestDeduplicate(t *testing.T) {
	t.Parallel()

	var (
		in     = make(chan []byte)
		tick   = make(chan time.Time)
		ticker = func(time.Duration) *time.Ticker { return &time.Ticker{C: tick} }
		window = time.Second
		out    = Deduplicate(in, window, ticker)
		t0     = time.Now()
		t1     = t0.Add(1 * window)
		t2     = t0.Add(2 * window)
	)

	var (
		rec1  = []byte(fmt.Sprintf("%s Aaaa", ulid.MustNew(ulid.Timestamp(t0), nil).String()))
		rec2  = []byte(fmt.Sprintf("%s Bbb1", ulid.MustNew(ulid.Timestamp(t1), nil).String()))
		rec2b = []byte(fmt.Sprintf("%s Bbb2", ulid.MustNew(ulid.Timestamp(t1), nil).String()))
		rec3  = []byte(fmt.Sprintf("%s Cccc", ulid.MustNew(ulid.Timestamp(t2), nil).String()))
	)

	// Out-of-order and duplicate inserts.
	in <- rec3
	in <- rec1
	in <- rec3
	in <- rec2
	in <- rec2b // ReplaceOrInsert overwrites
	in <- rec1

	// We haven't ticked the ticker yet, so no records can emerge.
	select {
	case record := <-out:
		t.Fatalf("unexpected record: %q", record)
	default:
		// good
	}

	// Tick up just past one window. We should get the first record.
	tick <- t0.Add(time.Duration(1.5 * float64(window)))

	// Check the first.
	select {
	case record := <-out:
		if want, have := rec1, record; bytes.Compare(want, have) != 0 {
			t.Errorf("first record: want %q, have %q", want, have)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first record")
	}

	// We shouldn't get any other record yet.
	select {
	case record := <-out:
		t.Fatalf("unexpected record: %q", record)
	default:
		// good
	}

	// Tick way past everything. We should get precisely two more records.
	tick <- t2.Add(10 * window)

	// Check the second.
	select {
	case record := <-out:
		if want, have := rec2b, record; bytes.Compare(want, have) != 0 {
			t.Errorf("second record: want %q, have %q", want, have)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for second record")
	}

	// Check the third.
	select {
	case record := <-out:
		if want, have := rec3, record; bytes.Compare(want, have) != 0 {
			t.Errorf("third record: want %q, have %q", want, have)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for third record")
	}

	// No more records.
	select {
	case record := <-out:
		t.Fatalf("unexpected record: %q", record)
	default:
		// good
	}

	// Close the in chan, wait for the close on the out chan.
	close(in)
	select {
	case record, ok := <-out:
		if ok {
			t.Fatalf("unexpected record: %q", record)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for chan close")
	}
}
