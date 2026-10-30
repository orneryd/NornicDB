package bolt

import (
	"bufio"
	"io"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkEncodePackStreamValue_RowSmall_Legacy(b *testing.B) {
	row := []any{int64(1), "Alice", int64(30)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encodePackStreamValue(row)
	}
}

func BenchmarkEncodePackStreamValueInto_RowSmall(b *testing.B) {
	row := []any{int64(1), "Alice", int64(30)}
	buf := make([]byte, 0, 1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf = encodePackStreamListInto(buf, row)
	}
}

func BenchmarkEncodePackStreamValue_RowLarge_Legacy(b *testing.B) {
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

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encodePackStreamValue(row)
	}
}

func BenchmarkEncodePackStreamValueInto_RowLarge(b *testing.B) {
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
	buf := make([]byte, 0, 16*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf = encodePackStreamListInto(buf, row)
	}
}

func BenchmarkBolt_WriteRecordNoFlush_SmallRow(b *testing.B) {
	row := []any{int64(1), "Alice", int64(30)}
	session := &Session{
		writer: bufio.NewWriterSize(io.Discard, 256*1024),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := session.writeRecordNoFlush(row, "nornic"); err != nil {
			b.Fatal(err)
		}
		// mimic end-of-pull flush
		if err := session.sendSuccess(map[string]any{"has_more": true}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBolt_WriteRecordNoFlush_LargeRow(b *testing.B) {
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
	session := &Session{
		writer: bufio.NewWriterSize(io.Discard, 256*1024),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := session.writeRecordNoFlush(row, "nornic"); err != nil {
			b.Fatal(err)
		}
		if err := session.sendSuccess(map[string]any{"has_more": true}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBolt_RecordBatch_FlushPerRecord_SmallRow(b *testing.B) {
	const batchSize = 64

	row := []any{int64(1), "Alice", int64(30)}
	session := &Session{
		writer: bufio.NewWriterSize(io.Discard, 256*1024),
	}

	buf := make([]byte, 0, 1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < batchSize; j++ {
			buf = buf[:0]
			buf = append(buf, recordHeader...)
			buf = encodePackStreamListInto(buf, row)
			if err := session.sendChunk(buf); err != nil {
				b.Fatal(err)
			}
		}
		if err := session.sendSuccess(map[string]any{"has_more": false}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBolt_SendRecordsBatched_SmallRow(b *testing.B) {
	const batchSize = 64

	row := []any{int64(1), "Alice", int64(30)}
	rows := make([][]any, batchSize)
	for i := range rows {
		rows[i] = row
	}

	session := &Session{
		writer: bufio.NewWriterSize(io.Discard, 256*1024),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := session.sendRecordsBatched(rows, "nornic"); err != nil {
			b.Fatal(err)
		}
		if err := session.sendSuccess(map[string]any{"has_more": false}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBolt_RecordBatch_FlushPerRecord_LargeRow(b *testing.B) {
	const batchSize = 64

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
	session := &Session{
		writer: bufio.NewWriterSize(io.Discard, 256*1024),
	}

	buf := make([]byte, 0, 16*1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < batchSize; j++ {
			buf = buf[:0]
			buf = append(buf, recordHeader...)
			buf = encodePackStreamListInto(buf, row)
			if err := session.sendChunk(buf); err != nil {
				b.Fatal(err)
			}
		}
		if err := session.sendSuccess(map[string]any{"has_more": false}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBolt_SendRecordsBatched_LargeRow(b *testing.B) {
	const batchSize = 64

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
	rows := make([][]any, batchSize)
	for i := range rows {
		rows[i] = row
	}

	session := &Session{
		writer: bufio.NewWriterSize(io.Discard, 256*1024),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := session.sendRecordsBatched(rows, "nornic"); err != nil {
			b.Fatal(err)
		}
		if err := session.sendSuccess(map[string]any{"has_more": false}); err != nil {
			b.Fatal(err)
		}
	}
}
