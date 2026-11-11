package configs

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRetentionFileFallsBackToTheDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention.yaml")
	if err := os.WriteFile(path, []byte(`
default:
  logs: {search: 5d}
apps:
  shop.api:
    logs: {search: 3d, archive: 14d}
  billing:
    events: {archive: 1y}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRetention(path); err == nil {
		t.Fatal(`"1y" accepted`)
	}
	if err := os.WriteFile(path, []byte(`
default:
  logs: {search: 5d}
apps:
  shop.api:
    logs: {search: 3d, archive: 14d}
  billing:
    events: {archive: 365d}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRetention(path)
	if err != nil {
		t.Fatal(err)
	}
	day := 24 * time.Hour
	for _, tc := range []struct {
		app, table      string
		search, archive time.Duration
	}{
		{"shop.api", "logs", 3 * day, 14 * day},
		{"shop.api", "events", 30 * day, 90 * day}, // not set: the default
		{"billing", "logs", 5 * day, 30 * day},     // the file's default search, the built-in archive
		{"billing", "events", 30 * day, 365 * day},
		{"unknown", "logs", 5 * day, 30 * day},
	} {
		w := r.For(tc.app, tc.table)
		if w.Search.D() != tc.search || w.Archive.D() != tc.archive {
			t.Errorf("%s %s: search %v archive %v, want %v %v", tc.app, tc.table, w.Search, w.Archive, tc.search, tc.archive)
		}
	}
	if r.Urgent.D() != 7*day {
		t.Errorf("urgent %v", r.Urgent)
	}
	if r.Longest() != 365*day {
		t.Errorf("longest %v", r.Longest())
	}
}

func TestDurations(t *testing.T) {
	for in, want := range map[string]time.Duration{"7d": 7 * 24 * time.Hour, "36h": 36 * time.Hour, "90m": 90 * time.Minute} {
		if d, err := ParseDuration(in); err != nil || d != want {
			t.Errorf("%s: %v %v", in, d, err)
		}
	}
	for _, bad := range []string{"-1d", "d", "x", "-5m"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if s := FormatDuration(48 * time.Hour); s != "2d" {
		t.Errorf("2 days as %q", s)
	}
}

func TestEnvNeedsSomethingToReceiveOn(t *testing.T) {
	t.Setenv("NATS_ADDRESS", "")
	t.Setenv("GRPC_ADDRESS", "")
	if _, err := NewFromEnv(); err == nil {
		t.Fatal("no NATS and no gRPC accepted")
	}
	t.Setenv("GRPC_ADDRESS", ":0")
	t.Setenv("PARTIAL_IDLE", "1d")
	c, err := NewFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.PartialIdle != 24*time.Hour || c.SubjectPrefix != "cosmicdust" || c.EsReplicas != -1 {
		t.Fatalf("%+v", c)
	}
}
