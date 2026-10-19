package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The interval and line statements at a long source's size: 2,000 sections
// and a page of 200 spans per statement; a 200 KB text with 200 offsets.
func newTextIntervalBenchExecutor(b *testing.B) *StorageExecutor {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "textintervalbench"))
	starts, ends, labels := make([]interface{}, 2000), make([]interface{}, 2000), make([]interface{}, 2000)
	for i := range starts {
		starts[i], ends[i], labels[i] = int64(i*100), int64(i*100+90), "section"
	}
	line := strings.Repeat("a line of ordinary text é ", 3) + "\n"
	text := strings.Repeat(line, 200_000/len(line))
	_, err := exec.Execute(context.Background(),
		"CREATE (:Source {id: 'source-1', text: $text, starts: $starts, ends: $ends, labels: $labels})",
		map[string]interface{}{"text": text, "starts": starts, "ends": ends, "labels": labels})
	if err != nil {
		b.Fatal(err)
	}
	return exec
}

func benchmarkTextIntervalQuery(b *testing.B, query string, params map[string]interface{}) {
	exec := newTextIntervalBenchExecutor(b)
	ctx := context.Background()
	if _, err := exec.Execute(ctx, query, params); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, query, params); err != nil {
			b.Fatal(err)
		}
	}
}

func textIntervalBenchSpans() []interface{} {
	spans := make([]interface{}, 200)
	for i := range spans {
		start := int64(i * 997 % 199_000)
		spans[i] = map[string]interface{}{"start": start, "end": start + 250}
	}
	return spans
}

func BenchmarkTextIntervalOverlaps(b *testing.B) {
	benchmarkTextIntervalQuery(b, `
MATCH (s:Source {id: 'source-1'})
UNWIND $spans AS q
WITH s, q.start AS start, CASE WHEN q.end > q.start THEN q.end ELSE q.start + 1 END AS stop
RETURN [i IN range(0, size(coalesce(s.starts, [])) - 1)
        WHERE s.starts[i] < stop AND start < s.ends[i]
        | {index: i,
           start: CASE WHEN start > s.starts[i] THEN start ELSE s.starts[i] END,
           end: CASE WHEN stop < s.ends[i] THEN stop ELSE s.ends[i] END,
           label: s.labels[i]}] AS overlaps`, map[string]interface{}{"spans": textIntervalBenchSpans()})
}

func BenchmarkTextIntervalOverlapsBoundLists(b *testing.B) {
	benchmarkTextIntervalQuery(b, `
MATCH (s:Source {id: 'source-1'})
WITH s.starts AS starts, s.ends AS ends, s.labels AS labels
UNWIND $spans AS q
WITH starts, ends, labels, q.start AS start, CASE WHEN q.end > q.start THEN q.end ELSE q.start + 1 END AS stop
RETURN [i IN range(0, size(starts) - 1)
        WHERE starts[i] < stop AND start < ends[i]
        | {index: i,
           start: CASE WHEN start > starts[i] THEN start ELSE starts[i] END,
           end: CASE WHEN stop < ends[i] THEN stop ELSE ends[i] END,
           label: labels[i]}] AS overlaps`, map[string]interface{}{"spans": textIntervalBenchSpans()})
}

func BenchmarkTextLinePositions(b *testing.B) {
	offsets := make([]interface{}, 200)
	for i := range offsets {
		offsets[i] = int64(i * 997 % 199_000)
	}
	benchmarkTextIntervalQuery(b, `
MATCH (s:Source {id: 'source-1'})
WITH s.text AS text
UNWIND $offsets AS offset
WITH text, offset, substring(text, 0, offset) AS before
WITH text, offset, before, size(before) - size(replace(before, '\n', '')) AS breaks
RETURN breaks + 1 AS line, size(last(split(before, '\n'))) AS column`, map[string]interface{}{"offsets": offsets})
}
