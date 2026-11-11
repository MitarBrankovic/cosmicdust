package receiver

import (
	"bytes"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/internal/server/ledger"
	"github.com/c12s/cosmicdust/pkg/api"
)

func ref(name string, data []byte) *api.FileRef {
	return &api.FileRef{Node: "n1", Table: "logs", Partition: "dt=2026-10-29/hour=08", Name: name,
		Crc32C: crc32.Checksum(data, castagnoli), Size: int64(len(data))}
}

// send delivers data from offset in chunks of size, as one upload attempt,
// and returns the last ack.
func send(r *Receiver, fr *api.FileRef, data []byte, upload uint64, from, size int) *api.Ack {
	for off := from; ; off += size {
		end := min(off+size, len(data))
		ack := r.Chunk(&api.Chunk{File: fr, Upload: upload, Offset: int64(off), Data: data[off:end], Last: end == len(data)})
		if !ack.Ok || ack.Duplicate || ack.Stored || end == len(data) {
			return ack
		}
	}
}

type fixture struct {
	dir    string
	ledger *ledger.Ledger
	r      *Receiver
	stored int
}

func open(t *testing.T, dir string) *fixture {
	t.Helper()
	fx := &fixture{dir: dir}
	var err error
	if fx.ledger, err = ledger.Open(filepath.Join(dir, "state")); err != nil {
		t.Fatal(err)
	}
	if fx.r, err = Open(filepath.Join(dir, "inbox"), fx.ledger, func() { fx.stored++ }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fx.close)
	return fx
}

func (fx *fixture) close() {
	_ = fx.r.Close()
	_ = fx.ledger.Close()
}

func TestStoresAFileAndRecognisesItAgain(t *testing.T) {
	dir := t.TempDir()
	fx := open(t, dir)
	data := bytes.Repeat([]byte("parquet"), 1000)
	if ack := send(fx.r, ref("a.parquet", data), data, 1, 0, 1000); !ack.Stored {
		t.Fatalf("ack %+v", ack)
	}
	got, err := os.ReadFile(filepath.Join(dir, "inbox/node=n1/logs/dt=2026-10-29/hour=08/a.parquet"))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("stored file: %v", err)
	}
	if fx.stored != 1 {
		t.Fatalf("archiver woken %d times", fx.stored)
	}
	if ack := fx.r.Chunk(&api.Chunk{File: ref("a.parquet", data), Probe: true}); !ack.Duplicate {
		t.Fatalf("probe of a stored file %+v, want a duplicate", ack)
	}
	if pending := fx.ledger.PendingArchive(); len(pending) != 1 {
		t.Fatalf("pending archive %v", pending)
	}

	// Archived and gone from the inbox, the file is still known - also after
	// a restart.
	if err := fx.ledger.Archived("node=n1/logs/dt=2026-10-29/hour=08/a.parquet", []string{"x"}); err != nil {
		t.Fatal(err)
	}
	fx.close()
	fx2 := open(t, dir)
	if _, err := os.Stat(filepath.Join(dir, "inbox/node=n1/logs/dt=2026-10-29/hour=08/a.parquet")); err == nil {
		t.Fatal("an archived file left in the inbox after a restart")
	}
	if ack := send(fx2.r, ref("a.parquet", data), data, 2, 0, 1000); !ack.Duplicate {
		t.Fatalf("after archiving and a restart %+v, want a duplicate", ack)
	}
}

// Files renamed into the inbox but never recorded - a crash between the two -
// were never acknowledged: they go at the start, and their senders send them
// again. Files waiting for the archive stay.
func TestStartKeepsOnlyWhatTheLedgerWaitsFor(t *testing.T) {
	dir := t.TempDir()
	fx := open(t, dir)
	data := []byte("waiting")
	if ack := send(fx.r, ref("w.parquet", data), data, 1, 0, 100); !ack.Stored {
		t.Fatalf("ack %+v", ack)
	}
	fx.close()
	inbox := filepath.Join(dir, "inbox/node=n1/logs/dt=2026-10-29/hour=08")
	for _, name := range []string{"orphan.parquet", "x.parquet.part"} {
		if err := os.WriteFile(filepath.Join(inbox, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	open(t, dir)
	entries, _ := os.ReadDir(inbox)
	if len(entries) != 1 || entries[0].Name() != "w.parquet" {
		t.Fatalf("inbox after the start: %v", entries)
	}
}

// An upload that broke off is continued: the probe says how much arrived.
func TestProbeTellsWhereToContinue(t *testing.T) {
	fx := open(t, t.TempDir())
	data := bytes.Repeat([]byte("resume"), 1000)
	fr := ref("r.parquet", data)
	if ack := fx.r.Chunk(&api.Chunk{File: fr, Probe: true}); !ack.Ok || ack.Have != 0 || ack.Duplicate {
		t.Fatalf("probe of a new file %+v", ack)
	}
	fx.r.Chunk(&api.Chunk{File: fr, Offset: 0, Data: data[:2000]}) // then the link broke
	ack := fx.r.Chunk(&api.Chunk{File: fr, Probe: true})
	if !ack.Ok || ack.Have != 2000 {
		t.Fatalf("probe after 2000 bytes %+v", ack)
	}
	if ack := send(fx.r, fr, data, 0, int(ack.Have), 1000); !ack.Stored {
		t.Fatalf("continued upload %+v", ack)
	}
	if fx.r.Stats.Resumed.Load() != 1 {
		t.Errorf("resumed %d", fx.r.Stats.Resumed.Load())
	}
	// Another content under the same name does not continue the part.
	other := bytes.Repeat([]byte("other!"), 1000)
	fx.r.Chunk(&api.Chunk{File: ref("s.parquet", data), Offset: 0, Data: data[:1000]})
	if ack := fx.r.Chunk(&api.Chunk{File: ref("s.parquet", other), Probe: true}); ack.Have != 0 {
		t.Fatalf("probe for other content %+v, want to start over", ack)
	}
}

// A probe hands the file to its upload attempt: chunks of the attempt that
// broke off, arriving late, cannot move the copy from under it.
func TestProbeHandsTheFileToItsAttempt(t *testing.T) {
	fx := open(t, t.TempDir())
	data := bytes.Repeat([]byte("owned"), 1000)
	fr := ref("o.parquet", data)
	fx.r.Chunk(&api.Chunk{File: fr, Upload: 1, Probe: true})
	fx.r.Chunk(&api.Chunk{File: fr, Upload: 1, Offset: 0, Data: data[:1000]})
	if ack := fx.r.Chunk(&api.Chunk{File: fr, Upload: 2, Probe: true}); ack.Have != 1000 {
		t.Fatalf("probe of attempt 2 %+v", ack)
	}
	if ack := fx.r.Chunk(&api.Chunk{File: fr, Upload: 1, Offset: 1000, Data: data[1000:2000]}); ack.Ok {
		t.Fatal("a late chunk of attempt 1 moved the copy")
	}
	if ack := send(fx.r, fr, data, 2, 1000, 1000); !ack.Stored {
		t.Fatalf("attempt 2 %+v", ack)
	}
	if fx.r.Stats.Superseded.Load() != 1 {
		t.Errorf("superseded %d", fx.r.Stats.Superseded.Load())
	}
}

func TestRefusesAChunkOutOfOrderAndABadChecksum(t *testing.T) {
	fx := open(t, t.TempDir())
	data := bytes.Repeat([]byte("x"), 3000)
	fr := ref("b.parquet", data)
	if ack := fx.r.Chunk(&api.Chunk{File: fr, Offset: 1000, Data: data[1000:2000]}); ack.Ok {
		t.Fatal("chunk at 1000 accepted before the one at 0")
	}
	fx.r.Chunk(&api.Chunk{File: fr, Offset: 0, Data: data[:1000]})
	if ack := fx.r.Chunk(&api.Chunk{File: fr, Offset: 2000, Data: data[2000:]}); ack.Ok {
		t.Fatal("chunk skipping 1000 bytes accepted")
	}
	bad := ref("c.parquet", data)
	bad.Crc32C++
	if ack := send(fx.r, bad, data, 0, 0, 1000); ack.Ok || ack.Stored {
		t.Fatalf("file with a wrong checksum accepted: %+v", ack)
	}
	if fx.r.Stats.CRCFailures.Load() != 1 {
		t.Errorf("crc failures %d", fx.r.Stats.CRCFailures.Load())
	}
	if _, ok := fx.ledger.Known("node=n1/logs/dt=2026-10-29/hour=08/c.parquet"); ok {
		t.Error("a file that failed its checksum is in the ledger")
	}
}

func TestIdleUploadsExpire(t *testing.T) {
	fx := open(t, t.TempDir())
	data := []byte("zzzz")
	fx.r.Chunk(&api.Chunk{File: ref("e.parquet", data), Offset: 0, Data: data[:2]})
	now := time.Now()
	fx.r.now = func() time.Time { return now.Add(time.Hour) }
	fx.r.Expire(time.Minute)
	if fx.r.Uploads() != 0 {
		t.Fatal("idle upload not expired")
	}
}

func TestRefusesPathsOutsideTheInbox(t *testing.T) {
	fx := open(t, t.TempDir())
	for _, fr := range []*api.FileRef{
		{Node: "../x", Table: "logs", Partition: "dt=2026-10-29/hour=08", Name: "a"},
		{Node: "..", Table: "logs", Partition: "dt=2026-10-29/hour=08", Name: "a"},
		{Node: "n1", Table: "logs", Partition: "dt=../../etc/hour=08", Name: "a"},
		{Node: "n1", Table: "logs", Partition: "dt=2026-10-29/hour=08", Name: "../../a"},
		{Node: "n1", Table: "logs", Partition: "dt=2026-10-29/hour=08", Name: ".."},
		{Node: "n1", Table: "metrics", Partition: "dt=2026-10-29/hour=08", Name: "a"},
	} {
		if ack := fx.r.Chunk(&api.Chunk{File: fr, Last: true}); ack.Ok {
			t.Errorf("accepted %+v", fr)
		}
	}
}

func TestPaths(t *testing.T) {
	rel, err := Path(&api.FileRef{Node: "n-1", Table: "events", Partition: "dt=2026-10-29/hour=23", Name: "p.parquet"})
	if err != nil || rel != "node=n-1/events/dt=2026-10-29/hour=23/p.parquet" {
		t.Fatalf("%q %v", rel, err)
	}
	node, table, part, name, err := ParsePath(rel)
	if err != nil || node != "n-1" || table != "events" || part != "dt=2026-10-29/hour=23" || name != "p.parquet" {
		t.Fatalf("%q %q %q %q %v", node, table, part, name, err)
	}
}
