package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, string(body)
}

func TestMetricsEndpoint(t *testing.T) {
	h := Handler(Options{Gatherer: NewRegistry("v-test")})
	code, body := get(t, h, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics: %d", code)
	}
	for _, want := range []string{`cosmicdust_agent_build_info{goversion="`, `version="v-test"`, "go_goroutines", "process_resident_memory_bytes"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics has no %s", want)
		}
	}
}

func TestHealthz(t *testing.T) {
	var health error
	h := Handler(Options{Gatherer: NewRegistry("v"), Health: func() error { return health }})
	if code, body := get(t, h, "/healthz"); code != http.StatusOK || strings.TrimSpace(body) != "ok" {
		t.Fatalf("healthy: %d %q", code, body)
	}
	health = errors.New("not running: tailer")
	if code, body := get(t, h, "/healthz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "tailer") {
		t.Fatalf("unhealthy: %d %q", code, body)
	}
}

func TestPprofOnlyWhenAsked(t *testing.T) {
	off := Handler(Options{Gatherer: NewRegistry("v")})
	if code, _ := get(t, off, "/debug/pprof/"); code != http.StatusNotFound {
		t.Fatalf("pprof off: %d", code)
	}
	on := Handler(Options{Gatherer: NewRegistry("v"), Pprof: true})
	if code, _ := get(t, on, "/debug/pprof/"); code != http.StatusOK {
		t.Fatalf("pprof on: %d", code)
	}
}

func TestServerFailsOnTakenPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	s := NewServer(Options{Addr: ln.Addr().String(), Gatherer: NewRegistry("v"), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("Run on a taken port returned nil")
	}
}

func TestServerStopsCleanly(t *testing.T) {
	s := NewServer(Options{Addr: "127.0.0.1:0", Gatherer: NewRegistry("v"), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
}
