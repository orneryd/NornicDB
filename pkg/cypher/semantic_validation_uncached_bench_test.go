package cypher

import "testing"

// BenchmarkSemanticValidationUncached measures the compile-time semantic
// validation of statements seen for the first time (no validation cache).
func BenchmarkSemanticValidationUncached(b *testing.B) {
	statements := map[string]string{
		"match_where_return": "MATCH (n:Person {name: $name})-[r:KNOWS]->(m:Person) WHERE m.age > 30 AND labels(n) = ['Person'] RETURN n.name AS name, type(r) AS t, keys(m) AS k ORDER BY name LIMIT 10",
		"unwind_merge_set":   "UNWIND $rows AS row MERGE (n:Item {id: row.id}) ON CREATE SET n.created = timestamp() SET n += row.props, n.label = 'x' RETURN count(n) AS c",
		"long_with_chain":    "MATCH (a:A)-[:R]->(b:B) WITH a, collect(b) AS bs WHERE size(bs) > 1 WITH a, [x IN bs WHERE x.v > 0 | x.v] AS vs RETURN a.id AS id, vs, CASE WHEN size(vs) > 2 THEN 'many' ELSE 'few' END AS kind",
	}
	for name, statement := range statements {
		b.Run(name, func(b *testing.B) {
			exec := &StorageExecutor{}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				exec.semanticValidationCache = newSemanticValidationCache(1)
				exec.matchSemanticValidationCache = newSemanticValidationCache(1)
				exec.mergeSemanticValidationCache = newSemanticValidationCache(1)
				if err := exec.validateSemanticScopes(statement); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
