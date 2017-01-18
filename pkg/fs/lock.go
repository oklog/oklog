package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

const lockFilename = "LOCK"

// ClaimLock on the specified path.
// Successful claims must eventually be released.
func ClaimLock(fs Filesystem, path string) error {
	lockFile := filepath.Join(path, lockFilename)
	if fs.Exists(lockFile) {
		return errors.Errorf("%s already exists; another process is running, or the file is stale", lockFile)
	}
	f, err := fs.Create(lockFile)
	if err != nil {
		return errors.Wrapf(err, "creating %s", lockFile)
	}
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		return errors.Wrapf(err, "writing PID to %s", lockFile)
	}
	if err := f.Close(); err != nil {
		return errors.Wrapf(err, "closing %s", lockFile)
	}
	return nil
}

// ReleaseLock on the specified path.
// Fails if the lock wasn't previously claimed by the same process.
func ReleaseLock(fs Filesystem, path string) error {
	lockFile := filepath.Join(path, lockFilename)
	if !fs.Exists(lockFile) {
		return errors.Errorf("%s doesn't exist", lockFile)
	}
	f, err := fs.Open(lockFile)
	if err != nil {
		return errors.Wrapf(err, "opening %s", lockFile)
	}
	buf, err := ioutil.ReadAll(f)
	f.Close() // we're done with it from here forward
	if err != nil {
		return errors.Wrapf(err, "reading %s", lockFile)
	}
	var (
		lockPid = strings.TrimSpace(string(buf))
		myPid   = strconv.Itoa(os.Getpid())
	)
	switch {
	case lockPid == myPid:
		fs.Remove(lockFile)
		return nil
	case lockPid == "": // weird! but we can just proceed as normal
		fs.Remove(lockFile)
		return nil
	default:
		return errors.Errorf("%s owned by %s, but we are %s", lockFile, lockPid, myPid)
	}
}
