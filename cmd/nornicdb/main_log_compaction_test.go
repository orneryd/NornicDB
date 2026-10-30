package main

import (
	"os"
	"testing"
)

func TestCompactStreamIfOversized_TruncatesFile(t *testing.T) {
	tmp, err := os.CreateTemp("", "nornicdb-stdio-log-*.log")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	payload := make([]byte, 2048)
	for i := range payload {
		payload[i] = 'x'
	}
	if _, err := tmp.Write(payload); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	compactStreamIfOversized("test", tmp, 1024)

	info, err := tmp.Stat()
	if err != nil {
		t.Fatalf("stat temp file: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected truncated file size 0, got %d", info.Size())
	}
}

func TestCompactStreamIfOversized_NoOpWhenBelowLimit(t *testing.T) {
	tmp, err := os.CreateTemp("", "nornicdb-stdio-log-*.log")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	if _, err := tmp.Write([]byte("small-log")); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	compactStreamIfOversized("test", tmp, 1024)

	info, err := tmp.Stat()
	if err != nil {
		t.Fatalf("stat temp file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected file to remain non-empty")
	}
}
