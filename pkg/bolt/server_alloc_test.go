package bolt

import (
	"bufio"
	"io"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var boltAllocSink error

func TestWriteRecordNoFlush_Allocs_SmallRowIsZero(t *testing.T) {
	session := &Session{
		writer: bufio.NewWriterSize(io.Discard, 64*1024),
	}
	row := []any{int64(1), "Alice", int64(30)}

	// Prime any one-time allocations (buffer growth) outside measurement.
	if err := session.writeRecordNoFlush(row); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		boltAllocSink = session.writeRecordNoFlush(row)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocations, got %f", allocs)
	}
}

func TestWriteRecordNoFlush_Allocs_LargeRowIsZero(t *testing.T) {
	session := &Session{
		writer: bufio.NewWriterSize(io.Discard, 64*1024),
	}

	node := &storage.Node{
		ID:     "n1",
		Labels: []string{"Person", "Employee"},
		Properties: map[string]any{
			"name":  "Alice",
			"age":   int64(30),
			"title": "Staff Engineer",
		},
	}
	row := []any{node, "ok", float64(3.14159)}

	if err := session.writeRecordNoFlush(row); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(500, func() {
		boltAllocSink = session.writeRecordNoFlush(row)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocations, got %f", allocs)
	}
}

func TestWriteMessageNoFlush_Allocs_IsZero(t *testing.T) {
	session := &Session{
		writer: bufio.NewWriterSize(io.Discard, 64*1024),
	}

	msg := make([]byte, 1024)
	msg[0] = 0xB1
	msg[1] = MsgRecord

	allocs := testing.AllocsPerRun(1000, func() {
		boltAllocSink = session.writeMessageNoFlush(msg)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocations, got %f", allocs)
	}
}
