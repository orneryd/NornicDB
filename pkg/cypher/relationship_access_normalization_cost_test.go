package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Native relationship reads revisit their pattern for every incoming row.
// Already canonical text must not allocate another identical string per row.
func TestRelationshipCanonicalPatternNoRewriteAllocations(t *testing.T) {
	for _, pattern := range []string{
		"(a)-[r:T {key:$key}]->(b)",
		"(a)<-[r:T]-(b)",
		"(a)-[r:T]-(b)",
		"(a)-[r:T {text:'dash -[]-> inside'}]->(b)",
	} {
		t.Run(pattern, func(t *testing.T) {
			var got string
			allocations := testing.AllocsPerRun(50, func() {
				got = normalizeAnonymousTraversalRelationships(pattern)
			})
			require.Equal(t, pattern, got)
			require.Zero(t, allocations, "canonical source must be reused")
		})
	}
}

// Exercise both the no-rewrite route and the quoted-token/bare-edge route
// through Execute. Neither normalization choice may change rows or planning.
func TestRelationshipCanonicalAndBarePatternsPublic(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical_relationships"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:Canonical {name:'a'}), (b:Canonical {name:'b'}), (c:Canonical {name:'c'}), (a)-[:CANONICAL {text:'arrow --> inside'}]->(b), (b)-[:CANONICAL {text:'dash -[]-> inside'}]->(c)", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, query, want string
	}{
		{"bracketed", "MATCH (a:Canonical {name:'a'})-[r:CANONICAL]->(b) RETURN b.name", "b"},
		{"bare_outgoing", "MATCH (a:Canonical {name:'a'})-->(b) RETURN b.name", "b"},
		{"bare_incoming", "MATCH (b)<--(a:Canonical {name:'a'}) RETURN b.name", "b"},
		{"bare_undirected", "MATCH (a:Canonical {name:'a'})--(b) RETURN b.name", "b"},
		{"quoted_arrow", "MATCH (a)-[r:CANONICAL {text:'arrow --> inside'}]->(b) RETURN b.name", "b"},
		{"quoted_brackets", "MATCH (a)-[r:CANONICAL {text:'dash -[]-> inside'}]->(b) RETURN b.name", "c"},
		{"mixed_chain", "MATCH (a:Canonical {name:'a'})-->(b)-[r:CANONICAL]->(c) RETURN c.name", "c"},
	} {
		for _, mode := range []string{"", "PROFILE ", "EXPLAIN "} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				result, err := exec.Execute(ctx, mode+tc.query, nil)
				require.NoError(t, err)
				if mode == "EXPLAIN " {
					require.Empty(t, result.Rows)
					require.NotEmpty(t, planStringFromResult(t, result))
					return
				}
				require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
			})
		}
	}
}
