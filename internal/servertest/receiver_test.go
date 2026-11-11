package servertest

import (
	"bytes"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c12s/cosmicdust/pkg/api"
)

func ref(name string, data []byte) *api.FileRef {
	return &api.FileRef{Node: "n1", Table: "logs", Partition: "dt=2026-10-22/hour=08", Name: name,
		Crc32C: crc32.Checksum(data, castagnoli), Size: int64(len(data))}
}

// send delivers data from offset in chunks of size and returns the last ack.
func send(r *Receiver, fr *api.FileRef, data []byte, from, size int) *api.Ack {
	for off := from; ; off += size {
		end := min(off+size, len(data))
		ack := r.Chunk(&api.Chunk{File: fr, Offset: int64(off), Data: data[off:end], Last: end == len(data)})
		if !ack.Ok || ack.Duplicate || ack.Stored || end == len(data) {
			return ack
		}
	}
}

func open(t *testing.T, dir string) *Receiver {
	t.Helper()
	r, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestStoresAFileAndRecognisesItAgain(t *testing.T) {
	dir := t.TempDir()
	r := open(t, dir)
	data := bytes.Repeat([]byte("parquet"), 1000)
	if ack := send(r, ref("a.parquet", data), data, 0, 1000); !ack.Stored {
		t.Fatalf("ack %+v", ack)
	}
	got, err := os.ReadFile(filepath.Join(dir, "node=n1/logs/dt=2026-10-22/hour=08/a.parquet"))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("stored file: %v", err)
	}
	if ack := r.Chunk(&api.Chunk{File: ref("a.parquet", data), Probe: true}); !ack.Duplicate {
		t.Fatalf("probe of a stored file %+v, want a duplicate", ack)
	}
	if ack := send(r, ref("a.parquet", data), data, 0, 1000); !ack.Duplicate {
		t.Fatalf("second upload %+v, want a duplicate", ack)
	}
	_ = r.Close()
	r2 := open(t, dir) // after a restart the file is still known
	defer func() { _ = r2.Close() }()
	if ack := send(r2, ref("a.parquet", data), data, 0, 1000); !ack.Duplicate {
		t.Fatalf("after a restart %+v, want a duplicate", ack)
	}
	if !r2.Has("n1", "logs/dt=2026-10-22/hour=08/a.parquet") {
		t.Fatal("Has does not see the file")
	}
}

// An upload that broke off is continued: the probe says how much arrived.
func TestProbeTellsWhereToContinue(t *testing.T) {
	dir := t.TempDir()
	r := open(t, dir)
	defer func() { _ = r.Close() }()
	data := bytes.Repeat([]byte("resume"), 1000)
	fr := ref("r.parquet", data)
	if ack := r.Chunk(&api.Chunk{File: fr, Probe: true}); !ack.Ok || ack.Have != 0 || ack.Duplicate {
		t.Fatalf("probe of a new file %+v", ack)
	}
	r.Chunk(&api.Chunk{File: fr, Offset: 0, Data: data[:2000]}) // then the link broke
	ack := r.Chunk(&api.Chunk{File: fr, Probe: true})
	if !ack.Ok || ack.Have != 2000 {
		t.Fatalf("probe after 2000 bytes %+v", ack)
	}
	if ack := send(r, fr, data, int(ack.Have), 1000); !ack.Stored {
		t.Fatalf("continued upload %+v", ack)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "node=n1/logs/dt=2026-10-22/hour=08/r.parquet"))
	if !bytes.Equal(got, data) {
		t.Fatal("the continued file differs")
	}
	if r.Stats.Resumed.Load() != 1 {
		t.Errorf("resumed %d", r.Stats.Resumed.Load())
	}
	// Another content under the same name does not continue the part.
	other := bytes.Repeat([]byte("other!"), 1000)
	r.Chunk(&api.Chunk{File: ref("s.parquet", data), Offset: 0, Data: data[:1000]})
	if ack := r.Chunk(&api.Chunk{File: ref("s.parquet", other), Probe: true}); ack.Have != 0 {
		t.Fatalf("probe for other content %+v, want to start over", ack)
	}
}

// A probe hands the file to its upload attempt: chunks of the attempt that
// broke off, arriving late, cannot move the copy from under it - with or
// without a part already there.
func TestProbeHandsTheFileToItsAttempt(t *testing.T) {
	r := open(t, t.TempDir())
	defer func() { _ = r.Close() }()
	data := bytes.Repeat([]byte("owned"), 1000)
	fr := ref("o.parquet", data)
	r.Chunk(&api.Chunk{File: fr, Upload: 1, Probe: true})
	r.Chunk(&api.Chunk{File: fr, Upload: 1, Offset: 0, Data: data[:1000]})
	if ack := r.Chunk(&api.Chunk{File: fr, Upload: 2, Probe: true}); ack.Have != 1000 {
		t.Fatalf("probe of attempt 2 %+v", ack)
	}
	if ack := r.Chunk(&api.Chunk{File: fr, Upload: 1, Offset: 1000, Data: data[1000:2000]}); ack.Ok {
		t.Fatal("a late chunk of attempt 1 moved the copy")
	}
	if ack := send2(r, fr, data, 2, 1000, 1000); !ack.Stored {
		t.Fatalf("attempt 2 %+v", ack)
	}

	// Attempt 3 probes before attempt 2's first chunk arrives; that chunk,
	// late, does not take the file back.
	fr2 := ref("p.parquet", data)
	r.Chunk(&api.Chunk{File: fr2, Upload: 3, Probe: true})
	if ack := r.Chunk(&api.Chunk{File: fr2, Upload: 2, Offset: 0, Data: data[:1000]}); ack.Ok {
		t.Fatal("a late first chunk of an older attempt took the file")
	}
	if ack := send2(r, fr2, data, 3, 0, 1000); !ack.Stored {
		t.Fatalf("attempt 3 %+v", ack)
	}
	if r.Stats.Superseded.Load() != 2 {
		t.Errorf("superseded %d", r.Stats.Superseded.Load())
	}
}

// send2 is send for one upload attempt.
func send2(r *Receiver, fr *api.FileRef, data []byte, upload uint64, from, size int) *api.Ack {
	for off := from; ; off += size {
		end := min(off+size, len(data))
		ack := r.Chunk(&api.Chunk{File: fr, Upload: upload, Offset: int64(off), Data: data[off:end], Last: end == len(data)})
		if !ack.Ok || ack.Duplicate || ack.Stored || end == len(data) {
			return ack
		}
	}
}

// The data has all arrived but the last chunk did not: an empty last chunk at
// the end completes it.
func TestEmptyLastChunkCompletes(t *testing.T) {
	r := open(t, t.TempDir())
	defer func() { _ = r.Close() }()
	data := []byte("complete")
	fr := ref("c.parquet", data)
	r.Chunk(&api.Chunk{File: fr, Offset: 0, Data: data})
	if ack := r.Chunk(&api.Chunk{File: fr, Probe: true}); ack.Have != int64(len(data)) {
		t.Fatalf("probe %+v", ack)
	}
	if ack := r.Chunk(&api.Chunk{File: fr, Offset: int64(len(data)), Last: true}); !ack.Stored {
		t.Fatalf("empty last chunk %+v", ack)
	}
}

func TestRefusesAChunkOutOfOrderAndABadChecksum(t *testing.T) {
	r := open(t, t.TempDir())
	defer func() { _ = r.Close() }()
	data := bytes.Repeat([]byte("x"), 3000)
	fr := ref("b.parquet", data)
	if ack := r.Chunk(&api.Chunk{File: fr, Offset: 1000, Data: data[1000:2000]}); ack.Ok {
		t.Fatal("chunk at 1000 accepted before the one at 0")
	}
	r.Chunk(&api.Chunk{File: fr, Offset: 0, Data: data[:1000]})
	if ack := r.Chunk(&api.Chunk{File: fr, Offset: 2000, Data: data[2000:]}); ack.Ok {
		t.Fatal("chunk skipping 1000 bytes accepted")
	}
	bad := ref("c.parquet", data)
	bad.Crc32C++
	if ack := send(r, bad, data, 0, 1000); ack.Ok || ack.Stored {
		t.Fatalf("file with a wrong checksum accepted: %+v", ack)
	}
	if r.Stats.CRCFailures.Load() != 1 {
		t.Errorf("crc failures %d", r.Stats.CRCFailures.Load())
	}
}

func TestAnUploadStartedOverReplacesThePartialOne(t *testing.T) {
	dir := t.TempDir()
	r := open(t, dir)
	defer func() { _ = r.Close() }()
	data := bytes.Repeat([]byte("y"), 3000)
	fr := ref("d.parquet", data)
	r.Chunk(&api.Chunk{File: fr, Offset: 0, Data: data[:1000]}) // then the sender started over
	if ack := send(r, fr, data, 0, 1000); !ack.Stored {
		t.Fatalf("restarted upload %+v", ack)
	}
	if r.Stats.Discarded.Load() != 1 {
		t.Errorf("partials discarded %d, want 1", r.Stats.Discarded.Load())
	}
	if parts, _ := filepath.Glob(filepath.Join(dir, "node=n1/logs/*/*/*.part")); len(parts) != 0 {
		t.Errorf("parts left: %v", parts)
	}
}

func TestLeftoverPartsAreRemovedAtStartAndIdleOnesExpire(t *testing.T) {
	dir := t.TempDir()
	r := open(t, dir)
	data := []byte("zzzz")
	r.Chunk(&api.Chunk{File: ref("e.parquet", data), Offset: 0, Data: data[:2]})
	now := time.Now()
	r.now = func() time.Time { return now.Add(time.Hour) }
	r.Expire(time.Minute)
	if len(r.partials) != 0 {
		t.Fatal("idle partial not expired")
	}
	r.Chunk(&api.Chunk{File: ref("f.parquet", data), Offset: 0, Data: data[:2]})
	for _, p := range r.partials { // a crash: the .part stays on disk
		_ = p.f.Close()
	}
	r2 := open(t, dir)
	defer func() { _ = r2.Close() }()
	if parts, _ := filepath.Glob(filepath.Join(dir, "node=n1/logs/*/*/*.part")); len(parts) != 0 {
		t.Fatalf("parts survived a restart: %v", parts)
	}
}

func TestRefusesPathsOutsideItsDirectory(t *testing.T) {
	r := open(t, t.TempDir())
	defer func() { _ = r.Close() }()
	for _, fr := range []*api.FileRef{
		{Node: "../x", Table: "logs", Partition: "dt=2026-10-22/hour=08", Name: "a"},
		{Node: "n1", Table: "logs", Partition: "dt=../../etc/hour=08", Name: "a"},
		{Node: "n1", Table: "logs", Partition: "dt=2026-10-22/hour=08", Name: "../../a"},
		{Node: "n1", Table: "logs/..", Partition: "dt=2026-10-22/hour=08", Name: "a"},
	} {
		if ack := r.Chunk(&api.Chunk{File: fr, Last: true}); ack.Ok {
			t.Errorf("accepted %+v", fr)
		}
	}
}

func TestUrgentEventsAreStoredOnce(t *testing.T) {
	dir := t.TempDir()
	r := open(t, dir)
	e := &api.Event{Node: "n1", Ts: time.Now().UnixNano(), ContainerId: "c1", Action: "oom"}
	if ack := r.Event(e); !ack.Stored {
		t.Fatalf("first %+v", ack)
	}
	if ack := r.Event(e); !ack.Duplicate {
		t.Fatalf("second %+v", ack)
	}
	_ = r.Close()
	r2 := open(t, dir)
	defer func() { _ = r2.Close() }()
	if ack := r2.Event(e); !ack.Duplicate {
		t.Fatalf("after a restart %+v", ack)
	}
}
