package cypher

import "testing"

// BenchmarkMergeRetryShape times the per-statement MERGE retry-shape check
// the Bolt session runs for every statement of an explicit transaction
// (#961): a plain MERGE, a MERGE with ON CREATE SET (which sets the
// analyzer's CREATE flag), and a multi-MERGE upsert.
func BenchmarkMergeRetryShape(b *testing.B) {
	for _, query := range []struct{ name, cypher string }{
		{"merge-set", "MERGE (u:User {id: $id}) SET u.name = $name"},
		{"merge-on-create", "MERGE (u:User {id: $id}) ON CREATE SET u.created = timestamp() SET u.name = $name"},
		{"multi-merge", "MERGE (o:O {hash: $h}) ON CREATE SET o.id = 'sha256:' + $h MERGE (v:V {id: $v}) ON CREATE SET v.original_id = o.id MERGE (v)-[:HAS]->(o) MERGE (u:U {id: $u}) MERGE (u)-[:OF]->(v) RETURN v.id"},
	} {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = IsRetrySafeMergeCommitQuery(analyzeQuery(query.cypher))
			}
		})
	}
}
