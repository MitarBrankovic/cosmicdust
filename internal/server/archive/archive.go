// Package archive moves received files from the inbox into the archive,
// split by application: a node sends one file per hour of its own, with the
// rows of every application on it, and the archive keeps
//
//	<table>/app=<app>/dt=<day>/hour=<hour>/<node>-<name>
//
// so that retention per application deletes whole directories, and a query
// for one application reads only its files. The split costs the control plane,
// not the node: an edge node would otherwise write a small file per
// application every few seconds.
package archive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/internal/server/receiver"
	"github.com/c12s/cosmicdust/internal/store"
	"github.com/c12s/cosmicdust/pkg/api"
)

// Archiver works through the received files the ledger lists as waiting, in
// the order they arrived.
type Archiver struct {
	inbox, root, quarantine string
	ledger                  *ledger.Ledger
	archived                func() // wakes the indexer
	log                     *slog.Logger
	wake                    chan struct{}
	batch                   int

	compactAfter, compactEvery time.Duration
	lastCompaction             time.Time

	Stats Stats
}

// Stats counts what the archiver did.
type Stats struct {
	Files, Rows, Outputs, Failures, Missing, Quarantined atomic.Int64
	Compactions, Compacted                               atomic.Int64 // hours merged; files merged away
}

// New returns an archiver from inbox into root. Received files that are not
// Parquet files of the tables' schemas go to quarantine instead.
func New(inbox, root, quarantine string, l *ledger.Ledger, archived func(), log *slog.Logger) *Archiver {
	if archived == nil {
		archived = func() {}
	}
	return &Archiver{inbox: inbox, root: root, quarantine: quarantine, ledger: l, archived: archived, log: log,
		wake: make(chan struct{}, 1), batch: 4096}
}

// Wake tells the archiver a file arrived.
func (a *Archiver) Wake() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// Run archives until ctx ends. A file that cannot be archived now - a full
// disk, say - is tried again after a pause; the files behind it wait, so that
// they are archived in the order they arrived.
func (a *Archiver) Run(ctx context.Context) {
	pause := time.Second
	for {
		failed := false
		for _, file := range a.ledger.PendingArchive() {
			if ctx.Err() != nil {
				return
			}
			if err := a.Archive(file); err != nil {
				a.Stats.Failures.Add(1)
				a.log.Warn("cannot archive a received file; trying again", "file", file, "err", err, "in", pause)
				failed = true
				break
			}
		}
		if failed {
			// The pause holds also when files keep arriving: a full disk is
			// not fixed by trying again at every file.
			select {
			case <-ctx.Done():
				return
			case <-time.After(pause):
			}
			pause = min(2*pause, time.Minute)
			continue
		}
		pause = time.Second
		a.compactDue(time.Now())
		select {
		case <-ctx.Done():
			return
		case <-a.wake:
		case <-time.After(time.Minute):
		}
	}
}

// Archive splits one received file into the archive, records it and removes
// it from the inbox. Done again after a crash, it writes the same files.
func (a *Archiver) Archive(file string) error {
	node, table, partition, name, err := receiver.ParsePath(file)
	if err != nil {
		return err
	}
	src := filepath.Join(a.inbox, filepath.FromSlash(file))
	out := node + "-" + name
	var outputs []string
	var rows int64
	switch table {
	case api.TableLogs:
		outputs, rows, err = split[store.LogRow](src, a.root, table, partition, out, a.batch, 1<<20)
	case api.TableEvents:
		outputs, rows, err = split[store.EventRow](src, a.root, table, partition, out, a.batch, 64<<10)
	default:
		err = fmt.Errorf("unknown table %q", table)
	}
	var bad *BadFileError
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Recorded but gone from the inbox: nothing to archive. It does not
		// happen unless the inbox is changed by hand.
		a.Stats.Missing.Add(1)
		a.log.Error("a received file is missing from the inbox", "file", file)
		outputs, err = nil, nil
	case errors.As(err, &bad):
		// Trying again would not help, and would hold up every file behind
		// it: the file goes to quarantine, for a person to look at.
		dst := filepath.Join(a.quarantine, filepath.FromSlash(file))
		if merr := os.MkdirAll(filepath.Dir(dst), 0o755); merr != nil {
			return merr
		}
		if merr := os.Rename(src, dst); merr != nil {
			return merr
		}
		a.Stats.Quarantined.Add(1)
		a.log.Error("a received file is not a Parquet file of its table; quarantined", "file", file, "err", err)
		outputs, err = nil, nil
	}
	if err != nil {
		return err
	}
	if err := a.ledger.Archived(file, outputs); err != nil {
		return err
	}
	if err := os.Remove(src); err != nil && !errors.Is(err, fs.ErrNotExist) {
		a.log.Warn("cannot remove an archived file from the inbox", "file", file, "err", err)
	}
	a.Stats.Files.Add(1)
	a.Stats.Rows.Add(rows)
	a.Stats.Outputs.Add(int64(len(outputs)))
	a.archived()
	return nil
}

// Path is an archive file's path, relative to the archive's root.
func Path(table, app, partition, name string) string {
	return table + "/app=" + EscapeApp(app) + "/" + partition + "/" + name
}

// unknownApp stands for rows without an application.
const unknownApp = "_unknown"

// EscapeApp makes an application name a directory name.
func EscapeApp(app string) string {
	if app == "" {
		return unknownApp
	}
	return url.PathEscape(app)
}

// UnescapeApp is the application a directory name stands for.
func UnescapeApp(dir string) string {
	if dir == unknownApp {
		return ""
	}
	if app, err := url.PathUnescape(dir); err == nil {
		return app
	}
	return dir
}

// ParsePath splits an archive path into table, application, partition and
// name.
func ParsePath(rel string) (table, app, partition, name string, err error) {
	parts := strings.Split(rel, "/")
	if len(parts) != 5 || !strings.HasPrefix(parts[1], "app=") {
		return "", "", "", "", fmt.Errorf("not an archive path: %q", rel)
	}
	return parts[0], UnescapeApp(strings.TrimPrefix(parts[1], "app=")), parts[2] + "/" + parts[3], parts[4], nil
}

// outFile is one application's part of a received file, being written.
type outFile[T Row] struct {
	rel, path, tmp string
	f              *os.File
	w              *parquet.GenericWriter[T]
}

func split[T Row](src, root, table, partition, name string, batch, page int) (outputs []string, rows int64, err error) {
	r, err := openRows[T](src)
	if err != nil {
		return nil, 0, err
	}
	defer r.close()

	outs := map[string]*outFile[T]{}
	defer func() {
		if err != nil {
			for _, o := range outs {
				_ = o.f.Close()
				_ = os.Remove(o.tmp)
			}
		}
	}()
	buf := make([]T, batch)
	groups := map[string][]T{} // the batch's rows by application, in their order
	for {
		n, rerr := r.read(buf)
		for app := range groups {
			groups[app] = groups[app][:0]
		}
		for i := range buf[:n] {
			app := AppOf(&buf[i])
			groups[app] = append(groups[app], buf[i])
		}
		for app, g := range groups {
			if len(g) == 0 {
				continue
			}
			o := outs[app]
			if o == nil {
				if o, err = create[T](root, Path(table, app, partition, name), page); err != nil {
					return nil, 0, err
				}
				outs[app] = o
			}
			if _, err = o.w.Write(g); err != nil {
				return nil, 0, fmt.Errorf("write %s: %w", o.rel, err)
			}
		}
		rows += int64(n)
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				return nil, 0, rerr
			}
			break
		}
	}
	for _, o := range outs {
		if err = o.commit(); err != nil {
			return nil, 0, err
		}
		outputs = append(outputs, o.rel)
	}
	slices.Sort(outputs)
	return outputs, rows, nil
}

// archiveCodec is the archive's compression: zstd at its default level. The
// archive is kept for weeks, and the control plane has the CPU the node, which
// uses the fastest level, does not.
var archiveCodec = &zstd.Codec{Level: zstd.SpeedDefault}

func create[T Row](root, rel string, page int, opts ...parquet.WriterOption) (*outFile[T], error) {
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	opts = append([]parquet.WriterOption{parquet.Compression(archiveCodec), parquet.PageBufferSize(page),
		parquet.CreatedBy("cosmicdust-server", "", "")}, opts...)
	w := parquet.NewGenericWriter[T](f, opts...)
	return &outFile[T]{rel: rel, path: path, tmp: path + ".tmp", f: f, w: w}, nil
}

// commit finishes the file: footer, fsync, rename into place, fsync of the
// directory. Only then does it exist under its name.
func (o *outFile[T]) commit() error {
	if err := o.w.Close(); err != nil {
		return err
	}
	if err := o.f.Sync(); err != nil {
		return err
	}
	if err := o.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(o.tmp, o.path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(o.path))
}

// ReadRows calls fn with the rows of a Parquet file, up to chunk at a time;
// the slice is reused between calls. A file it cannot read is a
// *BadFileError.
func ReadRows[T Row](path string, chunk int, fn func([]T) error) error {
	r, err := openRows[T](path)
	if err != nil {
		return err
	}
	defer r.close()
	buf := make([]T, chunk)
	for {
		n, err := r.read(buf)
		if n > 0 {
			if ferr := fn(buf[:n]); ferr != nil {
				return ferr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// BadFileError is a file that is not a Parquet file of its table's schema:
// trying it again does not help.
type BadFileError struct {
	Path string
	Err  error
}

func (e *BadFileError) Error() string { return e.Path + ": " + e.Err.Error() }
func (e *BadFileError) Unwrap() error { return e.Err }

// rowFile reads the rows of one Parquet file. parquet-go panics on a file it
// cannot read; here that is a *BadFileError, so that one bad file a node sent
// cannot stop the service.
type rowFile[T Row] struct {
	path string
	f    *os.File
	r    *parquet.GenericReader[T]
}

func openRows[T Row](path string) (rf *rowFile[T], err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		_ = f.Close()
		return nil, &BadFileError{Path: path, Err: err}
	}
	defer func() {
		if p := recover(); p != nil {
			_ = f.Close()
			rf, err = nil, &BadFileError{Path: path, Err: fmt.Errorf("%v", p)}
		}
	}()
	return &rowFile[T]{path: path, f: f, r: parquet.NewGenericReader[T](pf)}, nil
}

// read reads rows into buf, and io.EOF at the end.
func (rf *rowFile[T]) read(buf []T) (n int, err error) {
	defer func() {
		if p := recover(); p != nil {
			n, err = 0, &BadFileError{Path: rf.path, Err: fmt.Errorf("%v", p)}
		}
	}()
	n, err = rf.r.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		err = &BadFileError{Path: rf.path, Err: err}
	}
	return n, err
}

func (rf *rowFile[T]) close() {
	_ = rf.r.Close()
	_ = rf.f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// Row is either kind of row of the files nodes send.
type Row interface {
	store.LogRow | store.EventRow
}

// AppOf is the application of a row.
func AppOf[T Row](r *T) string {
	switch v := any(r).(type) {
	case *store.LogRow:
		return v.App
	case *store.EventRow:
		return v.App
	}
	return ""
}
