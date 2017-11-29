package fs

import "testing"

func TestLock(t *testing.T) {
	t.Parallel()

	lock := "TESTLOCK"
	for name, filesys := range map[string]Filesystem{
		"virtual": NewVirtualFilesystem(),
		"real":    NewRealFilesystem(),
	} {
		t.Run(name, func(t *testing.T) {
			r, existed, err := filesys.Lock(lock)
			if err != nil {
				t.Fatalf("initial claim: %v", err)
			}
			if existed {
				t.Fatal("initial claim: lock file already exists")
			}
			if _, existed, _ = filesys.Lock(lock); !existed {
				t.Fatal("second claim: want existed true, have false")
			}
			if err := r.Release(); err != nil {
				t.Fatalf("initial release: %v", err)
			}
			if err := r.Release(); err == nil {
				t.Fatal("second release: want error, have none")
			}
		})
		if filesys.Exists(lock) {
			t.Errorf("%s: %s still exists", name, lock)
		}
		filesys.Remove(lock)
	}
}
