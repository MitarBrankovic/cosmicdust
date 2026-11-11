package tail

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Plans made while a writer rotates as fast as it can: every plan must hold
// one file after another, whatever moment of a rotation its listing caught.
// Before the directory was listed in one call and checked again after the
// open, 1-5 % of the plans skipped a file here.
func TestPlansWhileRotating(t *testing.T) {
	if testing.Short() {
		t.Skip("races a writer for a second in all")
	}
	for _, pause := range []time.Duration{0, 200 * time.Microsecond} {
		t.Run(fmt.Sprint("pause=", pause), func(t *testing.T) { plansWhileRotating(t, pause) })
	}
}

func plansWhileRotating(t *testing.T, pause time.Duration) {
	path := filepath.Join(t.TempDir(), "c-json.log")
	const keep, perFile = 20, 3
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var rotations atomic.Int64
	failed := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		seq := 0
		for !stop.Load() {
			var b bytes.Buffer
			for range perFile {
				seq++
				fmt.Fprintf(&b, "%d\n", seq)
			}
			if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
				failed <- err
				return
			}
			// As dockerd rotates: the oldest goes, the others move one name
			// up, oldest first, then the live file becomes .1 and a new one
			// is created.
			_ = os.Remove(fmt.Sprintf("%s.%d", path, keep))
			for i := keep - 1; i >= 1; i-- {
				_ = os.Rename(fmt.Sprintf("%s.%d", path, i), fmt.Sprintf("%s.%d", path, i+1))
			}
			if err := os.Rename(path, path+".1"); err != nil {
				failed <- err
				return
			}
			if err := os.WriteFile(path, nil, 0o644); err != nil {
				failed <- err
				return
			}
			rotations.Add(1)
			if pause > 0 {
				time.Sleep(pause)
			}
		}
	}()

	pl := plan{path: path, buf: newReadBuf(4096), stats: &Stats{}}
	first := func(f *openFile) (int, bool) {
		var b [32]byte
		n, _ := f.f.ReadAt(b[:], 0)
		i := bytes.IndexByte(b[:n], '\n')
		if i <= 0 {
			return 0, false // the live file, still empty
		}
		v, err := strconv.Atoi(string(b[:i]))
		return v, err == nil
	}
	plans, moved, skipped := 0, 0, 0
	for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); {
		of, err := openAt(path+".3", false, pl.buf)
		if err != nil {
			continue // in the middle of a rotation
		}
		p := of.positionAt(0)
		files, err := pl.open(&p, true)
		of.close()
		if errors.Is(err, errMoved) || errors.Is(err, fs.ErrNotExist) {
			moved++
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		plans++
		prev := -1
		for _, f := range files {
			v, ok := first(f)
			if !ok {
				continue
			}
			if prev >= 0 && v != prev+perFile {
				skipped++
				break
			}
			prev = v
		}
		closeAll(files)
	}
	stop.Store(true)
	<-done
	select {
	case err := <-failed:
		t.Fatal(err)
	default:
	}
	t.Logf("%d plans, %d gave up after rotations, %d rotations", plans, moved, rotations.Load())
	if skipped > 0 {
		t.Fatalf("%d of %d plans skipped a file", skipped, plans)
	}
}
