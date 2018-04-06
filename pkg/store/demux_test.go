package store

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/oklog/ulid"

	"github.com/oklog/oklog/pkg/fs"
)

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
			const numTopics = 10
			logs := map[string]Log{}

			for i := 0; i < numTopics; i++ {
				t := fmt.Sprintf("topic-%d", i)
				log, err := NewFileLog(fsys, filepath.Join("benchrun", t), 128*1024*1024, 1024*1024, nil)
				if err != nil {
					b.Fatal(err)
				}
				logs[t] = log
			}
			d := NewDemuxer(nil, func(t string) (WriteSegment, error) {
				return logs[t].Create()
			})
			rec := ""
			for i := 0; i < 256; i++ {
				rec += "A"
			}
			f, err := fsys.Create("benchrun/benchseg.flushed")
			if err != nil {
				b.Fatal(err)
			}
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
