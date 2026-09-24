package cypher

// Row-equality coverage for NornicDB #562: seeding a later MATCH from a node
// bound by an earlier clause must return exactly the rows the unseeded
// traversal returns. The seed node still has to satisfy the pattern's own
// labels and inline properties on the endpoint it seeds.
//
// Every expected row set below was produced by running the same graph and
// queries against Neo4j 2026 community (cypher-shell, --format plain), and
// matches NornicDB main at ecc83220.
//
// OPTIONAL MATCH with inline constraints is not in the table: it runs through the pipeline's
// OPTIONAL MATCH operator, which this change does not touch. On main that
// operator already ignores inline properties on a reused variable
// (`MATCH (a:Foo {id:'a'}) OPTIONAL MATCH (a {x:2})-[r]->(b)` returns two
// rows where Neo4j returns one row with b = null), and it ignores inline
// relationship properties the same way. That is a separate defect; the
// OPTIONAL MATCH rows below are the ones where main already agrees with Neo4j.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// issue562SemanticsGraph is small on purpose: every endpoint constraint in
// the table has one node that satisfies it and one that does not.
const issue562SemanticsGraph = `CREATE (a:Foo {id:'a', x:1}), (c:Foo:Bar {id:'c', x:2}), (b:Tgt {id:'b'}), (d:Tgt:Extra {id:'d'}),
 (a)-[:R {w:1}]->(b), (a)-[:R {w:3}]->(c), (c)-[:R {w:2}]->(d), (c)-[:S]->(c)`

func issue562RowStrings(rows [][]interface{}) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		cells := make([]string, 0, len(row))
		for _, cell := range row {
			cells = append(cells, fmt.Sprint(cell))
		}
		out = append(out, strings.Join(cells, ", "))
	}
	sort.Strings(out)
	return out
}

// TestIssue562BoundSeedRowEquality checks that seeding from an earlier-bound
// node never changes the result: endpoint inline properties and extra labels
// are still enforced, and OPTIONAL MATCH, null bindings, zero-hop varlen,
// direction, self-loops, both-endpoints-bound and chained patterns return the
// same rows as an unseeded traversal (and Neo4j).
func TestIssue562BoundSeedRowEquality(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := exec.Execute(ctx, issue562SemanticsGraph, nil)
	require.NoError(t, err)

	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"start_prop_mismatch", `MATCH (a:Foo {id:'a'}) MATCH (a {x:2})-[r]->(b) RETURN a.id, b.id`, []string{}},
		{"start_prop_match", `MATCH (a:Foo {id:'a'}) MATCH (a {x:1})-[r]->(b) RETURN a.id, b.id`, []string{"a, b", "a, c"}},
		{"start_extra_label_mismatch", `MATCH (a:Foo {id:'a'}) MATCH (a:Bar)-[r]->(b) RETURN a.id, b.id`, []string{}},
		{"start_extra_label_match", `MATCH (a:Foo {id:'c'}) MATCH (a:Bar)-[r]->(b) RETURN a.id, b.id`, []string{"c, c", "c, d"}},
		{"start_multi_label_and", `MATCH (a {id:'a'}) MATCH (a:Foo:Bar)-[r]->(b) RETURN a.id, b.id`, []string{}},
		{"start_param_prop_mismatch", `MATCH (a:Foo {id:'a'}) MATCH (a {x:$x})-[r]->(b) RETURN a.id, b.id`, []string{}},
		{"start_prop_mismatch_where", `MATCH (a:Foo {id:'a'}) MATCH (a {x:2})-[r]->(b) WHERE r.w > 0 RETURN a.id, b.id`, []string{}},
		{"start_prop_match_where", `MATCH (a:Foo {id:'a'}) MATCH (a {x:1})-[r]->(b) WHERE r.w = 3 RETURN a.id, b.id`, []string{"a, c"}},
		{"start_prop_mismatch_with", `MATCH (a:Foo {id:'a'}) WITH a MATCH (a {x:2})-[r]->(b) RETURN a.id, b.id`, []string{}},
		{"end_prop_mismatch", `MATCH (b:Tgt {id:'b'}) MATCH (a)-[r]->(b {id:'zzz'}) RETURN a.id, b.id`, []string{}},
		{"end_multi_label_mismatch", `MATCH (b:Tgt {id:'b'}) MATCH (a)-[r]->(b:Tgt:Extra) RETURN a.id, b.id`, []string{}},
		{"end_multi_label_match", `MATCH (d:Tgt {id:'d'}) MATCH (a)-[r]->(d:Tgt:Extra) RETURN a.id, d.id`, []string{"c, d"}},
		{"end_seed_start_prop_mismatch", `MATCH (b:Tgt {id:'b'}) MATCH (a {x:2})-[r]->(b) RETURN a.id, b.id`, []string{}},
		{"end_seed_start_label_mismatch", `MATCH (b:Tgt {id:'b'}) MATCH (a:Bar)-[r]->(b) RETURN a.id, b.id`, []string{}},
		{"end_where_zero", `MATCH (b:Tgt {id:'b'}) MATCH (a)-[r]->(b) WHERE r.w = 99 RETURN a.id, b.id`, []string{}},
		{"end_where_match", `MATCH (b:Tgt {id:'b'}) MATCH (a {x:1})-[r]->(b) WHERE r.w = 1 RETURN a.id, b.id`, []string{"a, b"}},
		{"optional_start_bound", `MATCH (a:Foo {id:'a'}) OPTIONAL MATCH (a)-[r:R]->(b) RETURN a.id, b.id`, []string{"a, b", "a, c"}},
		{"optional_end_bound", `MATCH (d:Tgt {id:'d'}) OPTIONAL MATCH (x)-[r]->(d) RETURN x.id, d.id`, []string{"c, d"}},
		{"optional_null_then_optional", `OPTIONAL MATCH (n:Missing) OPTIONAL MATCH (n)-[r]->(b) RETURN n.id, b.id`, []string{"<nil>, <nil>"}},
		{"optional_then_match_seeded", `MATCH (a:Foo {id:'a'}) OPTIONAL MATCH (a)-[:R]->(m:Bar) MATCH (m)-[r]->(z) RETURN a.id, m.id, z.id`, []string{"a, c, c", "a, c, d"}},
		{"null_bound_start", `OPTIONAL MATCH (n:Missing) MATCH (n)-[r]->(b) RETURN n.id, b.id`, []string{}},
		{"null_bound_end", `OPTIONAL MATCH (n:Missing) MATCH (a)-[r]->(n) RETURN a.id, n.id`, []string{}},
		{"varlen_zero_hop_start_mismatch", `MATCH (a:Foo {id:'a'}) MATCH (a {x:2})-[:R*0..2]->(b) RETURN a.id, b.id`, []string{}},
		{"varlen_zero_hop_start_match", `MATCH (a:Foo {id:'a'}) MATCH (a {x:1})-[:R*0..2]->(b) RETURN a.id, b.id`, []string{"a, a", "a, b", "a, c", "a, d"}},
		{"varlen_zero_hop_end", `MATCH (d:Tgt {id:'d'}) MATCH (a)-[:R*0..2]->(d) RETURN a.id, d.id`, []string{"a, d", "c, d", "d, d"}},
		{"varlen_zero_hop_end_start_prop", `MATCH (d:Tgt {id:'d'}) MATCH (a {x:1})-[:R*0..2]->(d) RETURN a.id, d.id`, []string{"a, d"}},
		{"varlen_zero_hop_end_prop_mismatch", `MATCH (d:Tgt {id:'d'}) MATCH (a)-[:R*0..2]->(d {id:'zzz'}) RETURN a.id, d.id`, []string{}},
		{"direction_incoming", `MATCH (c:Foo {id:'c'}) MATCH (c)<-[r]-(x) RETURN c.id, x.id`, []string{"c, a", "c, c"}},
		{"direction_incoming_prop_mismatch", `MATCH (c:Foo {id:'c'}) MATCH (c {x:1})<-[r]-(x) RETURN c.id, x.id`, []string{}},
		{"direction_undirected", `MATCH (c:Foo {id:'c'}) MATCH (c)-[r]-(x) RETURN c.id, x.id, type(r)`, []string{"c, a, R", "c, c, S", "c, d, R"}},
		{"direction_undirected_end_bound", `MATCH (d:Tgt {id:'d'}) MATCH (x)-[r]-(d) RETURN x.id, d.id`, []string{"c, d"}},
		{"self_loop", `MATCH (c:Foo {id:'c'}) MATCH (c)-[r]->(c) RETURN c.id, type(r)`, []string{"c, S"}},
		{"self_loop_prop_mismatch", `MATCH (c:Foo {id:'c'}) MATCH (c {x:1})-[r]->(c) RETURN c.id, type(r)`, []string{}},
		{"self_loop_end_label_mismatch", `MATCH (c:Foo {id:'c'}) MATCH (c)-[r]->(c:Extra) RETURN c.id, type(r)`, []string{}},
		{"both_bound_end_prop_mismatch", `MATCH (a:Foo {id:'a'}), (c:Foo {id:'c'}) MATCH (a)-[r]->(c {x:1}) RETURN a.id, c.id`, []string{}},
		{"both_bound_match", `MATCH (a:Foo {id:'a'}), (c:Foo {id:'c'}) MATCH (a {x:1})-[r]->(c:Bar) RETURN a.id, c.id`, []string{"a, c"}},
		{"chained_start_prop_mismatch", `MATCH (a:Foo {id:'a'}) MATCH (a {x:2})-[:R]->(m)-[:R]->(z) RETURN a.id, m.id, z.id`, []string{}},
		{"chained_start_prop_match", `MATCH (a:Foo {id:'a'}) MATCH (a {x:1})-[:R]->(m)-[:R]->(z) RETURN a.id, m.id, z.id`, []string{"a, c, d"}},
		{"chained_start_label_mismatch", `MATCH (a:Foo {id:'a'}) MATCH (a:Bar)-[:R]->(m)-[:R]->(z) RETURN a.id, m.id, z.id`, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := exec.Execute(ctx, tc.query, map[string]interface{}{"x": int64(2)})
			require.NoError(t, err, tc.query)
			require.Equal(t, tc.want, issue562RowStrings(res.Rows), tc.query)
		})
	}
}
