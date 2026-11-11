package tail

import (
	"os"
	"syscall"
)

// readNames lists dir with one getdents64 call. The kernel holds the
// directory's lock for the whole call, and a rename takes the same lock, so
// the listing is one moment of the directory: a rename lands before the call
// or after it, never in the middle. When one call did not return it all - the
// buffer too small, or a call cut short by a signal - the directory is listed
// again with a larger buffer.
func readNames(dir string) ([]string, error) {
	size := 8 << 10
	for range 8 {
		names, whole, err := readNamesOnce(dir, size)
		if err != nil || whole {
			return names, err
		}
		size = min(2*size, 1<<20)
	}
	return nil, errMoved
}

func readNamesOnce(dir string, size int) (names []string, whole bool, err error) {
	fd, err := syscall.Open(dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, false, &os.PathError{Op: "open", Path: dir, Err: err}
	}
	defer func() { _ = syscall.Close(fd) }()
	buf := make([]byte, size)
	n, err := getdents(fd, buf)
	if err != nil {
		return nil, false, &os.PathError{Op: "getdents", Path: dir, Err: err}
	}
	_, _, names = syscall.ParseDirent(buf[:n], -1, nil)
	if n == 0 {
		return names, true, nil
	}
	// The first call had it all only if the next one finds nothing more.
	m, err := getdents(fd, buf)
	if err != nil {
		return nil, false, &os.PathError{Op: "getdents", Path: dir, Err: err}
	}
	return names, m == 0, nil
}

func getdents(fd int, buf []byte) (int, error) {
	for {
		n, err := syscall.Getdents(fd, buf)
		if err != syscall.EINTR {
			return n, err
		}
	}
}
