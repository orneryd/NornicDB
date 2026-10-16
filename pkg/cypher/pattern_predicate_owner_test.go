package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A relationship pattern used as a predicate (in WHERE, under NOT/AND/OR, or
// as exists(pattern)) is decided by the EXISTS owner with the whole row: a
// bound relationship variable, a row value in a property map and a row with
// no node binding are all seen (#1317 S04-S07). Expected values are Neo4j
// 5.26.30's.
func TestPatternPredicateUsesExistsOwner(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	if _, err := exec.Execute(ctx, "CREATE (:A {id:1})-[:R]->(b:B {name:'x'}), (:C)-[:R]->(b), (:A {id:2})-[:R]->(:X), (:A {id:3})-[:R]->(:X)", nil); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"S04 bound relationship in WHERE",
			"MATCH (a:A)-[r:R]->(b:B), (c:C) WHERE (c)-[r]->(b) RETURN count(*) AS c", []string{"0"}},
		{"bound relationship under NOT",
			"MATCH (a:A)-[r:R]->(b:B), (c:C) WHERE NOT (c)-[r]->(b) RETURN count(*) AS c", []string{"1"}},
		{"S05 row value in a property map",
			"WITH 'x' AS nm MATCH (a:A) WHERE (a)-[:R]->(:B {name: nm}) RETURN count(a) AS c", []string{"1"}},
		{"row value in a property map, no match",
			"WITH 'y' AS nm MATCH (a:A) WHERE (a)-[:R]->(:B {name: nm}) RETURN count(a) AS c", []string{"0"}},
		{"S06 exists() with a bound relationship",
			"MATCH (a:A)-[r:R]->(b:B), (c:C) RETURN exists((c)-[r]->(b)) AS e", []string{"false"}},
		{"exists() with a bound relationship that matches",
			"MATCH (a:A)-[r:R]->(b:B), (c:C) RETURN exists((a)-[r]->(b)) AS e", []string{"true"}},
		{"S07 row with no node binding",
			"MATCH (a)-[r:R]->(b:X) WITH r WHERE ()-[r]->(:X) RETURN count(*) AS c", []string{"2"}},
		{"row with no node binding, no match",
			"MATCH (a)-[r:R]->(b:X) WITH r WHERE ()-[r]->(:B) RETURN count(*) AS c", []string{"0"}},
		{"pattern under OR",
			"MATCH (a:A) WHERE (a)-[:R]->(:X) OR a.id = 1 RETURN a.id AS id ORDER BY id", []string{"1", "2", "3"}},
		{"pattern under AND",
			"MATCH (a:A) WHERE (a)-[:R]->(:X) AND a.id > 2 RETURN a.id AS id ORDER BY id", []string{"3"}},
		{"exists() over a null node",
			"OPTIONAL MATCH (z:Nope) RETURN exists((z)-[:R]->()) AS e", []string{"false"}},
		{"WHERE pattern over a null node",
			"OPTIONAL MATCH (z:Nope) WITH z MATCH (a:A {id:1}) WHERE (z)-[:R]->() RETURN count(*) AS c", []string{"0"}},
		{"WHERE NOT pattern over a null node",
			"OPTIONAL MATCH (z:Nope) WITH z MATCH (a:A {id:1}) WHERE NOT (z)-[:R]->() RETURN count(*) AS c", []string{"1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tc.query, nil)
			if err != nil {
				t.Fatalf("%s: %v", tc.query, err)
			}
			got := make([]string, 0, len(result.Rows))
			for _, row := range result.Rows {
				got = append(got, fmt.Sprint(row[0]))
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("%s:\n got %v\nwant %v", tc.query, got, tc.want)
			}
		})
	}
}

// BenchmarkWherePatternPredicate is a WHERE relationship-pattern predicate
// over bound nodes, the shape the removed bound-pattern copy served.
func BenchmarkWherePatternPredicate(b *testing.B) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "benchmark"))
	if _, err := exec.Execute(ctx, "UNWIND range(1, 50) AS i CREATE (:S {i: i})-[:T]->(:U {i: i})", nil); err != nil {
		b.Fatal(err)
	}
	const query = "MATCH (s:S), (u:U) WHERE s.i = u.i AND (s)-[:T]->(u) RETURN count(*) AS c"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(ctx, query, nil)
		if err != nil || len(result.Rows) != 1 {
			b.Fatalf("rows=%v err=%v", result, err)
		}
	}
}
