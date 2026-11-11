package nodeid

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCreatesOnceAndKeeps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-id")
	first, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 {
		t.Fatalf("id %q is not 32 hex digits", first)
	}
	again, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("second Load returned %q, first %q", again, first)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("%d files left in the directory, want only node-id", len(entries))
	}
}

func TestLoadKeepsAGivenID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-id")
	if err := os.WriteFile(path, []byte("edge-07\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	id, err := Load(path)
	if err != nil || id != "edge-07" {
		t.Fatalf("Load = %q, %v", id, err)
	}
}

func TestLoadRejectsAnEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-id")
	if err := os.WriteFile(path, []byte("\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load accepted an empty file")
	}
}

func TestTwoNodesDiffer(t *testing.T) {
	a, _ := Load(filepath.Join(t.TempDir(), "node-id"))
	b, _ := Load(filepath.Join(t.TempDir(), "node-id"))
	if a == b {
		t.Fatalf("two fresh nodes got the same id %q", a)
	}
}
