package tail

import (
	"bufio"
	"bytes"
	"errors"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

// Docker rotates a json-file log by renaming: <id>-json.log becomes .log.1,
// the old .1 becomes .2, and a new file with a new inode appears under the
// old name. A descriptor keeps pointing at the renamed file, so the rule is:
// read the old inode to its end, then move on. Following the name alone loses
// the old file's tail; following the inode alone sticks to a file that will
// not grow again. The same machinery resumes after a restart: a position
// names an inode, a fingerprint and an offset, and if that file has since
// become .1 or .2 it is read from the offset, then every newer file in order
// (150 rotations without a line lost or doubled).

// openFile is one log file being read, identified by the inode it had when it
// was opened; path is its name then and may have changed since.
type openFile struct {
	f      *os.File
	buf    *readBuf
	path   string
	inode  uint64
	offset int64 // next unread byte
	live   bool  // false for a rotated file still to be worked through

	head   uint64 // fingerprint of the first line, once there is one
	headOK bool
}

// headLimit caps how much of the first line goes into the fingerprint.
const headLimit = 1024

// fingerprint hashes the file's first line, at most headLimit bytes. The
// kernel gives a freed inode to the next file created, so an inode alone does
// not identify a file across a restart: the first version resumed in the wrong
// file. The first line never changes once written, and in json-file
// it carries a time in nanoseconds. It is read once per file with pread, so
// the reader is not disturbed.
func (o *openFile) fingerprint() (uint64, bool) {
	if o.headOK {
		return o.head, true
	}
	var buf [headLimit]byte
	n, err := o.f.ReadAt(buf[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, false
	}
	end := bytes.IndexByte(buf[:n], '\n')
	switch {
	case end >= 0:
		end++
	case n == headLimit:
		end = headLimit
	default:
		return 0, false // no complete first line yet
	}
	h := fnv.New64a()
	_, _ = h.Write(buf[:end])
	o.head, o.headOK = h.Sum64(), true
	return o.head, true
}

// is reports whether this file is the one position p points into.
func (o *openFile) is(p *cosmicdust.Position) bool {
	if o.inode != p.Inode {
		return false
	}
	if p.Head == 0 {
		return true // a position without a fingerprint: the inode is all there is
	}
	h, ok := o.fingerprint()
	return ok && h == p.Head
}

func (o *openFile) positionAt(off int64) cosmicdust.Position {
	p := cosmicdust.Position{Path: o.path, Inode: o.inode, Offset: off}
	if h, ok := o.fingerprint(); ok {
		p.Head = h
	}
	return p
}

func (o *openFile) position() cosmicdust.Position { return o.positionAt(o.offset) }

// seek moves the next read to off; the descriptor follows when the file gets
// the read buffer.
func (o *openFile) seek(off int64) {
	o.buf.drop(o)
	o.offset = off
}

// readBuf is the read buffer the files of one Source share: the tailer reads
// one file at a time, and what it reads is copied out (into the reader's arena)
// before it reads the next. A buffer per file kept 16 KiB per container, most
// of a quiet container's footprint.
type readBuf struct {
	r     *bufio.Reader
	owner *openFile // whose data r holds
}

func newReadBuf(size int) *readBuf { return &readBuf{r: bufio.NewReaderSize(nil, size)} }

// reader hands o the buffer. If another file had it, o's descriptor is moved
// to o.offset first: what the buffer held of o before is gone.
func (b *readBuf) reader(o *openFile) (*bufio.Reader, error) {
	if b.owner != o {
		if _, err := o.f.Seek(o.offset, io.SeekStart); err != nil {
			return nil, err
		}
		b.r.Reset(o.f)
		b.owner = o
	}
	return b.r, nil
}

// drop forgets what the buffer holds of o.
func (b *readBuf) drop(o *openFile) {
	if b.owner == o {
		b.owner = nil
		b.r.Reset(nil)
	}
}

// close closes o's descriptor.
func (o *openFile) close() {
	o.buf.drop(o)
	_ = o.f.Close()
}

func (o *openFile) size() int64 {
	fi, err := o.f.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}

func inodeOf(fi fs.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Ino
	}
	return 0
}

func openAt(path string, live bool, buf *readBuf) (*openFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &openFile{f: f, buf: buf, path: path, inode: inodeOf(fi), live: live}, nil
}

func closeAll(files []*openFile) {
	for _, f := range files {
		if f != nil {
			f.close()
		}
	}
}

// rotatedFile is a sibling of the live file left by a rotation.
type rotatedFile struct {
	path string
	age  int64 // larger is older
}

// rotatedFiles lists the rotated siblings of path, oldest first, as one
// moment of their directory (readNames). Docker names them path.1, path.2, ...
// (larger is older); the kubelet names them path.<YYYYMMDD-hhmmss> (earlier is
// older). Compressed ones cannot be read in place and are only counted.
func rotatedFiles(path string) (files []rotatedFile, compressed int, err error) {
	dir, base := filepath.Split(path)
	names, err := readNames(filepath.Clean(dir))
	if err != nil {
		return nil, 0, err
	}
	for _, name := range names {
		suffix, ok := strings.CutPrefix(name, base+".")
		if !ok {
			continue
		}
		if strings.HasSuffix(suffix, ".gz") {
			compressed++
			continue
		}
		if n, err := strconv.Atoi(suffix); err == nil {
			files = append(files, rotatedFile{path: path + "." + suffix, age: int64(n)})
			continue
		}
		if t, err := time.Parse("20060102-150405", suffix); err == nil {
			files = append(files, rotatedFile{path: path + "." + suffix, age: -t.Unix()})
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].age > files[j].age })
	return files, compressed, nil
}

// sameNames reports whether two listings name the same files.
func sameNames(a, b []rotatedFile) bool {
	return slices.EqualFunc(a, b, func(x, y rotatedFile) bool { return x.path == y.path })
}

var errMoved = errors.New("log files moved while being opened")

// plan decides which files to read, oldest first, and where to start in the
// first. They are all opened before it returns: an open descriptor pins the
// inode, so a rotation while older files are worked through cannot pull one
// away.
//
//   - resume nil: a container seen for the first time; every file still on
//     disk is read, oldest first.
//   - resume in the live file: from its offset.
//   - resume in a rotated file: from its offset, then every newer file.
//   - resume in a file rotated out of existence: everything still on disk,
//     and the loss is counted unless drained says that file had been read to
//     its end anyway.
//
// The files are opened by the names a listing gave, and a rotation may rename
// them in between: one rotation renames every file, one at a time. The files
// opened count only if they were the log's files at one moment - the
// directory, listed again, has the same names, and every name still has the
// inode opened under it - otherwise the plan starts over. Name by name checks
// missed a listing taken in the middle of a rotation: the position's file
// already under its new name, the next one still under its old name, which the
// rest of the rotation gave to the file after it. Opened by that name, the
// file in between was skipped whole (411 lines in the kill -9 test).
type plan struct {
	path     string
	buf      *readBuf
	stats    *Stats
	warnLost func(resume *cosmicdust.Position, remaining int)

	// listed, in tests, runs between the listing of the rotated files and
	// the opens: where a rotation makes the listing stale.
	listed func()
}

func (pl plan) open(resume *cosmicdust.Position, drained bool) ([]*openFile, error) {
	for range 5 {
		files, err := pl.try(resume, drained)
		if errors.Is(err, errMoved) {
			continue
		}
		return files, err
	}
	return nil, errMoved
}

func (pl plan) try(resume *cosmicdust.Position, drained bool) ([]*openFile, error) {
	olds, compressed, err := rotatedFiles(pl.path)
	if err != nil {
		return nil, err
	}
	if pl.listed != nil {
		pl.listed()
	}
	live, err := openAt(pl.path, true, pl.buf)
	if err != nil {
		return nil, err // not created yet, or between a rename and its create
	}
	files := make([]*openFile, 0, len(olds)+1)
	for _, old := range olds {
		of, err := openAt(old.path, false, pl.buf)
		if err != nil {
			closeAll(append(files, live))
			if errors.Is(err, fs.ErrNotExist) {
				return nil, errMoved // renamed or deleted since the listing
			}
			return nil, err
		}
		files = append(files, of)
	}
	files = append(files, live)
	if err := pl.still(olds, files); err != nil {
		closeAll(files)
		return nil, err
	}
	if compressed > 0 {
		pl.stats.compressedSkipped.Add(int64(compressed))
	}
	if resume == nil {
		return files, nil // without a position, everything on disk is new
	}

	if live.is(resume) {
		off := resume.Offset
		if off > live.size() {
			// Shorter than where we stopped: truncated while we were away.
			pl.stats.truncations.Add(1)
			off = 0
		}
		live.seek(off)
		closeAll(files[:len(files)-1])
		return files[len(files)-1:], nil
	}
	from := slices.IndexFunc(files[:len(files)-1], func(f *openFile) bool { return f.is(resume) })
	if from >= 0 {
		files[from].seek(resume.Offset)
	} else {
		// The position's file has been rotated out of existence. Everything
		// still on disk is newer, so all of it is read: what is lost is only
		// what was in the deleted files.
		from = 0
		if !drained {
			pl.stats.lostFiles.Add(1)
			if pl.warnLost != nil {
				pl.warnLost(resume, len(files)-1)
			}
		}
	}
	closeAll(files[:from]) // older than the position: read already, or never ours
	return files[from:], nil
}

// still reports, as errMoved, a rotation since olds was listed: the files
// opened are the log's files at one moment only if the directory, listed
// again, has the same names and every name still has the inode opened under
// it. A file renamed away never comes back to a name it left, so each name had
// its inode all the time between the open and the check, the second listing
// included; and that listing is one moment of the directory.
func (pl plan) still(olds []rotatedFile, files []*openFile) error {
	again, _, err := rotatedFiles(pl.path)
	if err != nil {
		return err
	}
	if !sameNames(again, olds) {
		return errMoved
	}
	for _, f := range files {
		fi, err := os.Stat(f.path)
		if err != nil || inodeOf(fi) != f.inode {
			return errMoved
		}
	}
	return nil
}
