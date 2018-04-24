package index

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/google/codesearch/regexp"
	"github.com/oklog/ulid"

	"github.com/google/codesearch/index"
	"github.com/oklog/oklog/pkg/event"
	"github.com/oklog/oklog/pkg/fs"
	"github.com/oklog/oklog/pkg/record"
	"github.com/oklog/oklog/pkg/store"
)

func TestIndex2(t *testing.T) {
	if err := os.MkdirAll("testdir/store", 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("testdir/index", 0777); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("testdir")

	// Write plain log file to store with ULIDs prefixed.
	var low, high ulid.ULID
	start := ulid.Now()
	{
		of, err := os.Create("testdir/store/convert")
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open("testdata/log.txt")
		if err != nil {
			t.Fatal(err)
		}
		bfw := bufio.NewWriter(of)
		read := record.NewReader(f)
		randr := rand.New(rand.NewSource(0))
		for i := 0; ; i++ {
			r, err := read.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			id := ulid.MustNew(start+(uint64(i)*1000), randr)
			fmt.Fprintf(bfw, "%s %s", id, r)
			if i == 0 {
				low = id
			} else {
				high = id
			}
		}
		if err := bfw.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := of.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename("testdir/store/convert", fmt.Sprintf("testdir/store/%s-%s.flushed", low, high)); err != nil {
			t.Fatal(err)
		}
	}

	st, err := store.NewFileLog(
		fs.NewRealFilesystem(),
		"testdir/store",
		100*1024*1024,
		1024*1024,
		event.LogReporter{Logger: log.NewNopLogger()},
	)
	if err != nil {
		t.Fatal(err)
	}

	ix, err := NewIndex(fs.NewRealFilesystem(), "testdir/index", "testdir/store", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Sync(); err != nil {
		t.Fatal(err)
	}

	begin := time.Now()

	res, err := ix.Query(record.QueryParams{
		From:  record.ULIDOrTime{ULID: low},
		To:    record.ULIDOrTime{ULID: high},
		Q:     `ac\.uk.+endeavour-logo`,
		Regex: true,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(res)

	b, err := ioutil.ReadAll(res.Records)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(b))
	fmt.Println("index query took:", time.Since(begin))

	begin = time.Now()

	res2, err := st.Query(record.QueryParams{
		From:  record.ULIDOrTime{ULID: low},
		To:    record.ULIDOrTime{ULID: high},
		Q:     `ac\.uk.+endeavour-logo`,
		Regex: true,
	}, false)

	b, err = ioutil.ReadAll(res2.Records)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(b))

	fmt.Println("store query took:", time.Since(begin))
}

func TestIndex(t *testing.T) {
	f, err := os.Create("test.index")
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter()
	w.Reset(f)

	w.Add(1, 20, []byte("I will join Google in May"))
	w.Add(2, 21, []byte("I will join Facebook in August"))

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := NewReader("test.index")
	if err != nil {
		t.Fatal(err)
	}
	re, err := regexp.Compile("(Fac|Goo|Daa).* Au.+ust")
	if err != nil {
		t.Fatal(err)
	}
	q := index.RegexpQuery(re.Syntax)

	bm, err := r.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(bm.ToArray())

	// r.Query(&index.)

}
