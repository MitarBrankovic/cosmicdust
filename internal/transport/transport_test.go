package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/c12s/cosmicdust/internal/servertest"
	"github.com/c12s/cosmicdust/pkg/api"
	"github.com/c12s/cosmicdust/pkg/cosmicdust"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func quiet() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestChunks(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 25) // 250 bytes
	ref := &api.FileRef{Size: 250}
	for _, tc := range []struct {
		from   int64
		size   int
		chunks int
	}{{0, 100, 3}, {0, 250, 1}, {0, 50, 5}, {100, 100, 2}, {250, 100, 1}} {
		var got []byte
		var n int
		var lastAt int64 = -1
		err := chunks(bytes.NewReader(data[tc.from:]), ref, 7, tc.from, tc.size, nil, func(c *api.Chunk) error {
			if c.Offset != tc.from+int64(len(got)) {
				return fmt.Errorf("chunk at %d after %d bytes", c.Offset, len(got))
			}
			got = append(got, c.Data...)
			n++
			if c.Last {
				lastAt = c.Offset
			}
			return nil
		})
		if err != nil || n != tc.chunks || !bytes.Equal(got, data[tc.from:]) || lastAt < 0 {
			t.Errorf("from %d by %d: %d chunks, %d bytes, last at %d, err %v", tc.from, tc.size, n, len(got), lastAt, err)
		}
	}
	// A read error ends the upload; it is not the end of the file.
	boom := errors.New("disk")
	r := io.MultiReader(bytes.NewReader(data[:150]), &failing{err: boom})
	sent := 0
	err := chunks(r, ref, 7, 0, 100, nil, func(c *api.Chunk) error {
		sent++
		if c.Last {
			return errors.New("a read error made a last chunk")
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("read error: %v after %d chunks", err, sent)
	}
}

type failing struct{ err error }

func (f *failing) Read([]byte) (int, error) { return 0, f.err }

func TestLimiterPaces(t *testing.T) {
	now := time.Unix(0, 0)
	var slept time.Duration
	l := newLimiter(1000)
	l.now = func() time.Time { return now }
	l.sleep = func(d time.Duration) { slept += d; now = now.Add(d) }
	for range 5 {
		l.wait(1000) // the first second of tokens is a burst, the rest waits
	}
	if slept < 3900*time.Millisecond || slept > 4100*time.Millisecond {
		t.Fatalf("5000 bytes at 1000/s slept %v, want ~4 s", slept)
	}
	var nilLimiter *limiter
	nilLimiter.wait(1 << 30) // no limit
}

func TestFileRef(t *testing.T) {
	ts := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	ref, err := fileRef("n1", cosmicdust.FileRef{Table: "logs", Path: "logs/dt=2026-10-22/hour=09/part-1.parquet", Size: 10,
		CRC: 7, Rows: 3, MinTime: ts, MaxTime: ts.Add(time.Second)})
	if err != nil || ref.Partition != "dt=2026-10-22/hour=09" || ref.Name != "part-1.parquet" || ref.Node != "n1" ||
		ref.MaxTs-ref.MinTs != int64(time.Second) {
		t.Fatalf("%+v %v", ref, err)
	}
	if _, err := fileRef("n1", cosmicdust.FileRef{Table: "logs", Path: "events/dt=2026-10-22/hour=09/x"}); err == nil {
		t.Fatal("a path of another table accepted")
	}
	if err := (&Options{Node: "a.b"}).defaults(); err == nil {
		t.Fatal("a node that is not one subject token accepted")
	}
}

// store writes a file of a node's store and returns its reference.
func storeFile(t *testing.T, root, name string, data []byte) cosmicdust.FileRef {
	t.Helper()
	rel := "logs/dt=2026-10-22/hour=09/" + name
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	return cosmicdust.FileRef{Table: "logs", Path: rel, Size: int64(len(data)), CRC: crc32.Checksum(data, castagnoli),
		Rows: 1, MinTime: now, MaxTime: now}
}

func upload(t *testing.T, tr cosmicdust.Transport, root string, ref cosmicdust.FileRef) cosmicdust.Ack {
	t.Helper()
	f, err := os.Open(filepath.Join(root, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ack, err := tr.Upload(ctx, ref, f)
	if err != nil {
		t.Fatalf("upload %s: %v", ref.Path, err)
	}
	return ack
}

// exercise runs the same checks over either transport.
func exercise(t *testing.T, tr cosmicdust.Transport, r *servertest.Receiver, resumable bool) {
	root := t.TempDir()
	data := bytes.Repeat([]byte("a parquet file "), 5000) // 75 KB, 19 chunks of 4 KiB
	ref := storeFile(t, root, "part-1.parquet", data)
	if ack := upload(t, tr, root, ref); ack.Status != cosmicdust.AckStored {
		t.Fatalf("first upload %+v", ack)
	}
	if !r.Has("n1", ref.Path) {
		t.Fatal("the receiver does not have the file")
	}
	if ack := upload(t, tr, root, ref); ack.Status != cosmicdust.AckDuplicate {
		t.Fatalf("second upload %+v, want a duplicate", ack)
	}

	// A file whose declared checksum is wrong is refused, not a failure.
	bad := storeFile(t, root, "part-2.parquet", data[:1000])
	bad.CRC++
	if ack := upload(t, tr, root, bad); ack.Status != cosmicdust.AckRejected || ack.Reason == "" {
		t.Fatalf("bad checksum %+v", ack)
	}

	// An upload that breaks off is continued from where the receiver's copy
	// ends, and the file arrives whole.
	ref3 := storeFile(t, root, "part-3.parquet", data)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	broken := io.MultiReader(bytes.NewReader(data[:30000]), &failing{err: errors.New("cut")})
	if _, err := tr.Upload(ctx, ref3, broken); err == nil {
		t.Fatal("an upload from a failing reader succeeded")
	}
	resumedBefore := r.Stats.Resumed.Load()
	if ack := upload(t, tr, root, ref3); ack.Status != cosmicdust.AckStored {
		t.Fatalf("continued upload %+v", ack)
	}
	if resumable && r.Stats.Resumed.Load() != resumedBefore+1 {
		t.Errorf("the second upload did not continue the first (resumed %d)", r.Stats.Resumed.Load())
	}

	// Urgent events.
	code := 137
	e := cosmicdust.Event{Time: time.Now(), Runtime: "docker", Action: "die", Kind: cosmicdust.KindExit, ContainerID: "c1",
		Name: "web", ExitCode: &code, OOMKilled: true, Epoch: 2}
	if ack, err := tr.Urgent(ctx, e); err != nil || ack.Status != cosmicdust.AckStored {
		t.Fatalf("urgent %+v %v", ack, err)
	}
	if ack, err := tr.Urgent(ctx, e); err != nil || ack.Status != cosmicdust.AckDuplicate {
		t.Fatalf("urgent again %+v %v", ack, err)
	}
	if r.Stats.Events.Load() != 1 {
		t.Errorf("events stored %d", r.Stats.Events.Load())
	}
}

func TestGRPC(t *testing.T) {
	r, err := servertest.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := servertest.NewGRPCServer(r)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	tr, err := New("grpc://"+lis.Addr().String(), Options{Node: "n1", ChunkBytes: 4 << 10, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	// A gRPC stream cut by the sender may or may not have delivered its
	// last chunks, so whether the second upload continued is not checked.
	exercise(t, tr, r, false)
}

// startNATS runs nats-server on a free port, or skips the test.
func startNATS(t *testing.T) string {
	t.Helper()
	port := freePort(t)
	runNATS(t, port)
	return fmt.Sprintf("nats://127.0.0.1:%d", port)
}

func freePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().(*net.TCPAddr).Port
}

// runNATS runs nats-server on port until the test ends or stop is called.
func runNATS(t *testing.T, port int) (stop func()) {
	t.Helper()
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server is not installed")
	}
	srv := exec.Command(bin, "-a", "127.0.0.1", "-p", fmt.Sprint(port))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop = func() { once.Do(func() { _ = srv.Process.Kill(); _ = srv.Wait() }) }
	t.Cleanup(stop)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for deadline := time.Now().Add(10 * time.Second); ; {
		if c, err := net.Dial("tcp", addr); err == nil {
			_ = c.Close()
			return stop
		}
		if time.Now().After(deadline) {
			t.Fatal("nats-server did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitUp(t *testing.T, up <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-up:
	case <-time.After(within):
		t.Fatalf("no signal of the connection within %v", within)
	}
}

// Both transports say when the connection for files is up: the first time,
// and when the control plane is back after an outage.
func TestReconnected(t *testing.T) {
	t.Run("nats", func(t *testing.T) {
		port := freePort(t)
		stop := runNATS(t, port)
		tr, err := NewNATS(fmt.Sprintf("nats://127.0.0.1:%d", port), Options{Node: "n1", Logger: quiet()})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tr.Close() }()
		waitUp(t, tr.Reconnected(), 5*time.Second)

		stop()
		runNATS(t, port)
		waitUp(t, tr.Reconnected(), 10*time.Second)
	})
	t.Run("grpc", func(t *testing.T) {
		r, err := servertest.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := lis.Addr().String()
		srv := servertest.NewGRPCServer(r)
		go func() { _ = srv.Serve(lis) }()
		tr, err := NewGRPC(addr, Options{Node: "n1", Logger: quiet()})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tr.Close() }()
		waitUp(t, tr.Reconnected(), 5*time.Second)

		srv.Stop()
		lis, err = net.Listen("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		srv = servertest.NewGRPCServer(r)
		go func() { _ = srv.Serve(lis) }()
		defer srv.Stop()
		waitUp(t, tr.Reconnected(), 15*time.Second)
	})
}

func TestNATS(t *testing.T) {
	url := startNATS(t)
	r, err := servertest.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	stop, err := servertest.ServeNATS(nc, "cosmicdust", r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	tr, err := New(url, Options{Node: "n1", ChunkBytes: 4 << 10, Window: 4, AckTimeout: 5 * time.Second, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	// NATS keeps the order of one connection's messages: the probe of the
	// second upload comes after the chunks of the first.
	exercise(t, tr, r, true)
}

// With the broker up and no receiver, an upload fails at the probe at once,
// instead of sending the file into the void or waiting for an ack.
func TestNATSWithoutReceiver(t *testing.T) {
	url := startNATS(t)
	tr, err := New(url, Options{Node: "n1", AckTimeout: 10 * time.Second, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	root := t.TempDir()
	ref := storeFile(t, root, "part-1.parquet", []byte("nobody listens"))
	f, _ := os.Open(filepath.Join(root, ref.Path))
	defer func() { _ = f.Close() }()
	start := time.Now()
	_, err = tr.Upload(context.Background(), ref, f)
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("upload without a receiver: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("took %v", took)
	}
}
