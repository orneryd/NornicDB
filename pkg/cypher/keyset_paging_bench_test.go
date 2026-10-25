package cypher

import (
	"context"
	"testing"
)

// BenchmarkKeysetPage pages 20,000 indexed nodes, four per sort value, with
// the keyset predicate, near the start and near the end of the list (#939).
// The result cache is off, so every page runs.
func BenchmarkKeysetPage(b *testing.B) {
	exec := newKeysetPagingExecutor(b, "UNWIND range(0, 19999) AS i CREATE (:Item {t: i / 4, id: 'id' + toString(i)})")
	ctx := context.Background()
	const page = "MATCH (n:Item) WHERE n.t > $t OR (n.t = $t AND n.id > $id) RETURN n.t AS t, n.id AS id ORDER BY n.t, n.id LIMIT 20"
	for _, position := range []struct {
		name string
		t    int64
	}{{"Start", 10}, {"End", 4980}} {
		b.Run(position.name, func(b *testing.B) {
			params := map[string]interface{}{"t": position.t, "id": ""}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, page, params)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 20 {
					b.Fatalf("got %d rows", len(result.Rows))
				}
			}
		})
	}
}
