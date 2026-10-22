package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Each statement gets its own transaction so an expected baseline expression
// error cannot poison the independent NULL-predicate and OPTIONAL controls.
func TestPipelineTraversalDeferredPredicateBindings(t *testing.T) {
	queries := []struct {
		name, query string
		want        [][]interface{}
	}{
		{"row NULL", "UNWIND [null] AS x MATCH (p:P)-[:R {value:x}]->(q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"literal NULL", "MATCH (p:P)-[:R {value:null}]->(q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"mixed rows", "UNWIND [1,null,3] AS x MATCH (p:P)-[:R {value:x}]->(q) RETURN count(q)", [][]interface{}{{int64(2)}}},
		{"mixed map", "MATCH (p:P)-[:R {value:1, absent:null}]->(q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"optional NULL", "UNWIND [null] AS x OPTIONAL MATCH (p:P)-[:R {value:x}]->(q) RETURN count(*), count(q)", [][]interface{}{{int64(1), int64(0)}}},
		{"optional mixed", "UNWIND [1,null,3] AS x OPTIONAL MATCH (p:P)-[:R {value:x}]->(q) RETURN count(*), count(q)", [][]interface{}{{int64(3), int64(2)}}},
		{"non NULL control", "UNWIND [1,2,3] AS x MATCH (p:P)-[:R {value:x}]->(q) RETURN count(q)", [][]interface{}{{int64(3)}}},
		{"node NULL control", "UNWIND [null] AS x MATCH (p:P {id:x})-[:R]->(q) RETURN count(q)", [][]interface{}{{int64(0)}}},
		{"index seed WHERE", "MATCH (p:P)-[:R]->(q:Q) WHERE q.id > 0 RETURN p.id ORDER BY p.id LIMIT 2", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"index seed late filter", "MATCH (p:P)-[:R]->(q:Q) WHERE q.id > 1 RETURN p.id ORDER BY p.id LIMIT 2", [][]interface{}{{int64(2)}, {int64(3)}}},
		{"index seed mixed filter", "MATCH (p:P)-[:R]->(q:Q) WHERE p.id > 0 AND q.id > 1 RETURN p.id ORDER BY p.id LIMIT 2", [][]interface{}{{int64(2)}, {int64(3)}}},
		{"index seed relationship filter", "MATCH (p:P)-[r:R]->(q:Q) WHERE r.value > 1 RETURN p.id ORDER BY p.id LIMIT 2", [][]interface{}{{int64(2)}, {int64(3)}}},
		{"index seed own predicate control", "MATCH (p:P)-[:R]->(q:Q) WHERE p.id > 1 RETURN p.id ORDER BY p.id LIMIT 2", [][]interface{}{{int64(2)}, {int64(3)}}},
		{"index seed row WHERE", "UNWIND [0] AS x MATCH (p:P)-[:R]->(q:Q) WHERE q.id > x RETURN p.id ORDER BY p.id LIMIT 2", [][]interface{}{{int64(1)}, {int64(2)}}},
	}
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("index=%t", indexed), func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "deferred_predicate")
			for value := int64(1); value <= 3; value++ {
				p, q := storage.NodeID(fmt.Sprintf("p%d", value)), storage.NodeID(fmt.Sprintf("q%d", value))
				_, err := store.CreateNode(&storage.Node{ID: p, Labels: []string{"P"}, Properties: map[string]interface{}{"id": value}})
				require.NoError(t, err)
				_, err = store.CreateNode(&storage.Node{ID: q, Labels: []string{"Q"}, Properties: map[string]interface{}{"id": value}})
				require.NoError(t, err)
				require.NoError(t, store.CreateEdge(&storage.Edge{
					ID: storage.EdgeID(fmt.Sprintf("r%d", value)), StartNode: p, EndNode: q,
					Type: "R", Properties: map[string]interface{}{"value": value},
				}))
			}
			if indexed {
				_, err := NewStorageExecutor(store).Execute(context.Background(), "CREATE INDEX deferred_predicate_id FOR (p:P) ON (p.id)", nil)
				require.NoError(t, err)
			}
			for _, explicit := range []bool{false, true} {
				for _, query := range queries {
					for _, profile := range []bool{false, true} {
						t.Run(fmt.Sprintf("transaction=%t/%s/profile=%t", explicit, query.name, profile), func(t *testing.T) {
							executor := NewStorageExecutor(store)
							ctx := context.Background()
							if explicit {
								_, err := executor.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) }()
							}
							text := query.query
							if profile {
								text = "PROFILE " + text
							}
							got, err := executor.Execute(ctx, text, nil)
							require.NoError(t, err, text)
							require.Equal(t, query.want, got.Rows, text)
						})
					}
				}
			}
		})
	}
}

func TestTraversalMutationPropertiesStillRemoveNull(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "mutation_null")
	executor := NewStorageExecutor(store)
	ctx := context.Background()
	for _, query := range []string{
		"CREATE (p:P)-[r:R {value:null}]->(q:Q) RETURN properties(r)",
		"MATCH (p:P)-[r:R]->(q:Q) SET r.value = 1 SET r.value = null RETURN properties(r)",
	} {
		got, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, got.Rows)
	}
}

func TestTraversalSeedPredicateDependencies(t *testing.T) {
	for _, tc := range []struct {
		predicate string
		onlySeed  bool
	}{
		{"p.id > 0", true},
		{"id(p) > 0", true},
		{"p['id'] > 0", true},
		{"p.id > $threshold", true},
		{"'q.id' IS NOT NULL AND p.id > 0", true},
		{"math.round(p.id) > 0", true},
		{"`p`.`id` > 0", true},
		{"`p` . `id` > 0", true},
		{"p.id > q.id", false},
		{"r.value > 0", false},
		{"x > 0", false},
	} {
		t.Run(tc.predicate, func(t *testing.T) {
			require.Equal(t, tc.onlySeed, traversalSeedWhereOnlyReferences(tc.predicate, "p"))
		})
	}
}
