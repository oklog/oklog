package fs

import "testing"

func TestLock(t *testing.T) {
	for _, testcase := range []struct {
		name    string
		filesys Filesystem
	}{
		{"virtual", NewVirtualFilesystem()},
		{"real", NewRealFilesystem(false)},
	} {
		lock := "TESTLOCK"
		t.Run(testcase.name, func(t *testing.T) {
			r, existed, err := testcase.filesys.Lock(lock)
			if err != nil {
				t.Fatalf("initial claim: %v", err)
			}
			if existed {
				t.Fatal("initial claim: lock file already exists")
			}
			if _, existed, _ = testcase.filesys.Lock(lock); !existed {
				t.Fatal("second claim: want existed true, have false")
			}
			if err := r.Release(); err != nil {
				t.Fatalf("initial release: %v", err)
			}
			if err := r.Release(); err == nil {
				t.Fatal("second release: want error, have none")
			}
		})
		if testcase.filesys.Exists(lock) {
			t.Errorf("%s: %s still exists", testcase.name, lock)
		}
		testcase.filesys.Remove(lock)
	}
}
