package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkMergeActions measures MERGE with ON CREATE / ON MATCH SET, SET and
// REMOVE, alone and after UNWIND, including a FOREACH body with MERGE actions
// (the shape of graph ingestion statements).
func BenchmarkMergeActions(b *testing.B) {
	ctx := context.Background()
	rows := make([]interface{}, 10)
	for i := range rows {
		rows[i] = map[string]interface{}{"id": int64(i), "v": int64(i), "src": int64(i % 3)}
	}
	for _, bc := range []struct {
		name  string
		query string
	}{
		{name: "merge_on_create_on_match", query: "MERGE (n:BM {id: 1}) ON CREATE SET n.x = 1 ON MATCH SET n.y = $i"},
		{name: "merge_set", query: "MERGE (n:BM {id: 1}) SET n.y = $i"},
		{name: "merge_set_remove", query: "MERGE (n:BM {id: 1}) SET n.y = $i REMOVE n.z"},
		{name: "unwind_merge_on_create", query: "UNWIND $rows AS row MERGE (n:BM {id: row.id}) ON CREATE SET n.x = row.v SET n.y = row.v"},
		{name: "unwind_merge_foreach", query: "UNWIND $rows AS row MERGE (n:BM {id: row.id}) ON CREATE SET n.x = row.v " +
			"FOREACH (_ IN CASE WHEN row.src IS NULL THEN [] ELSE [1] END | MERGE (s:BS {id: row.src}) ON CREATE SET s.c = 1 SET s.t = row.v MERGE (n)-[:SRC]->(s))"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			exec, _ := newTestExecutor(b)
			_, err := exec.Execute(ctx, "CREATE (:BM {id: 1, z: 1})", nil)
			require.NoError(b, err)
			params := map[string]interface{}{"i": int64(0), "rows": rows}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				params["i"] = int64(i)
				if _, err := exec.Execute(ctx, bc.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
