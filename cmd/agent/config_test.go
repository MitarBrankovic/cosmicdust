package main

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestDefaults(t *testing.T) {
	cfg, err := parseConfig(nil, env(nil), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dataDir != "/var/lib/cosmicdust" || cfg.listen != ":9101" || cfg.memoryLimit != 64<<20 ||
		cfg.shutdownTimeout != 30*time.Second || cfg.logLevel != slog.LevelInfo || cfg.logFormat != "text" {
		t.Fatalf("defaults: %+v", cfg)
	}
}

func TestEnvFillsWhatFlagsLeave(t *testing.T) {
	cfg, err := parseConfig([]string{"-listen", ":9200"}, env(map[string]string{
		"COSMICDUST_LISTEN":           ":9300", // the flag wins
		"COSMICDUST_DATA_DIR":         "/data/cosmicdust",
		"COSMICDUST_MEMORY_LIMIT":     "48MiB",
		"COSMICDUST_SHUTDOWN_TIMEOUT": "5s",
		"COSMICDUST_LOG_LEVEL":        "debug",
		"COSMICDUST_PPROF":            "true",
	}), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != ":9200" {
		t.Errorf("listen %q: the environment overrode the flag", cfg.listen)
	}
	if cfg.dataDir != "/data/cosmicdust" || cfg.memoryLimit != 48<<20 || cfg.shutdownTimeout != 5*time.Second ||
		cfg.logLevel != slog.LevelDebug || !cfg.pprof {
		t.Fatalf("from env: %+v", cfg)
	}
}

func TestBadValues(t *testing.T) {
	cases := map[string]struct {
		args []string
		env  map[string]string
	}{
		"bad env size":   {env: map[string]string{"COSMICDUST_MEMORY_LIMIT": "lots"}},
		"bad env bool":   {env: map[string]string{"COSMICDUST_PPROF": "maybe"}},
		"bad log format": {args: []string{"-log-format", "xml"}},
		"stray argument": {args: []string{"run"}},
		"empty data dir": {args: []string{"-data-dir", ""}},
	}
	for name, c := range cases {
		if _, err := parseConfig(c.args, env(c.env), io.Discard); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestEnvName(t *testing.T) {
	if got := envName("shutdown-timeout"); got != "COSMICDUST_SHUTDOWN_TIMEOUT" {
		t.Fatal(got)
	}
}

func TestSize(t *testing.T) {
	for in, want := range map[string]size{
		"0": 0, "1024": 1024, "64MiB": 64 << 20, "1 GiB": 1 << 30, "512KiB": 512 << 10, "7B": 7,
	} {
		var s size
		if err := s.Set(in); err != nil || s != want {
			t.Errorf("Set(%q) = %d, %v; want %d", in, s, err, want)
		}
	}
	for _, in := range []string{"", "-1", "64MB", "1.5GiB", "9999999999999GiB"} {
		var s size
		if err := s.Set(in); err == nil {
			t.Errorf("Set(%q) accepted", in)
		}
	}
	for s, want := range map[size]string{0: "0", 64 << 20: "64MiB", 1 << 30: "1GiB", 1000: "1000B"} {
		if got := s.String(); got != want {
			t.Errorf("String(%d) = %q, want %q", int64(s), got, want)
		}
	}
}
