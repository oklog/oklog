package fs

import "testing"

func TestLock(t *testing.T) {
	var (
		fs   = NewVirtualFilesystem()
		path = "test-lock"
	)
	if err := ClaimLock(fs, path); err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	if err := ClaimLock(fs, path); err == nil {
		t.Fatal("double-claim: expected error, got none")
	}
	if err := ReleaseLock(fs, path); err != nil {
		t.Fatalf("initial release: %v", err)
	}
	if err := ReleaseLock(fs, path); err == nil {
		t.Fatalf("double-release: expected error, got none")
	}
}
