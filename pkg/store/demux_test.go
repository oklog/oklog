package store

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"

	"github.com/oklog/oklog/pkg/fs"
)

func TestDemux(t *testing.T) {

}

func BenchmarkDemux(b *testing.B) {
	for name, newFsys := range map[string]func() fs.Filesystem{
		"virtual": fs.NewVirtualFilesystem,
		"real":    fs.NewRealFilesystem,
	} {
		b.Run(name, func(b *testing.B) {
			fsys := newFsys()
			// HACK(fabxc): package fs does not provide RemoveAll. Call plain os.RemoveAll
			// to cleanup on the real filesystem.
			defer os.RemoveAll("benchrun")

			if err := fsys.MkdirAll("benchrun"); err != nil {
				b.Fatal(err)
			}
			reporter := LogReporter{log.NewLogfmtLogger(os.Stderr)}
			d := NewDemuxer(nil, NewFileTopicLogs(fsys, "benchrun", 128*1024*1024, 1024*1024, nil), reporter)
			rec := ""
			for i := 0; i < 256; i++ {
				rec += "A"
			}
			f, err := fsys.Create("benchrun/benchseg.flushed")
			if err != nil {
				b.Fatal(err)
			}
			const numTopics = 10
			bufw := bufio.NewWriter(f) // don't obfuscate CPU profile with syscalls.
			for i := 0; i < b.N; i++ {
				id := ulid.MustNew(uint64(i), nil)
				topic := fmt.Sprintf("topic-%d", i%numTopics)
				fmt.Fprintln(bufw, id, topic, rec)
			}
			bufw.Flush()
			f.Close()

			s, err := newFileReadSegment(fsys, "benchrun/benchseg.flushed")
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			b.ReportAllocs()

			if err := d.demux(s); err != nil {
				b.Fatal(err)
			}
		})
	}
}
