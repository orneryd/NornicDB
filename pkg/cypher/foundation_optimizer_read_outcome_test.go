// Author: Lane #1644 (Codex). An optimization attempt must decline on an
// unrecovered read, preserving its public bool protocol and issued data view.
package cypher_test

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type foundationOptimizerReadFault struct {
	storage.Engine
	failure error
}

func (v *foundationOptimizerReadFault) GetNode(storage.NodeID) (*storage.Node, error) {
	return nil, v.failure
}

func TestFoundationPublicOptimizerReadOutcome(t *testing.T) {
	patterns := []struct {
		name, query string
		info        cypher.PatternInfo
	}{
		{"mutual", "MATCH (a:Doc)-[:R]->(b:Doc)-[:R]->(a) RETURN a.name,b.name", cypher.PatternInfo{Pattern: cypher.PatternMutualRelationship, RelType: "R"}},
		{"incoming", "MATCH (a:Doc)<-[r:R]-(b) RETURN a.name,count(r)", cypher.PatternInfo{Pattern: cypher.PatternIncomingCountAgg, RelType: "R"}},
		{"outgoing", "MATCH (a:Doc)-[r:R]->(b) RETURN a.name,count(r)", cypher.PatternInfo{Pattern: cypher.PatternOutgoingCountAgg, RelType: "R"}},
		{"edge-property", "MATCH (a)-[r:R]->(b:Doc) RETURN b.name,avg(r.weight),count(r)", cypher.PatternInfo{Pattern: cypher.PatternEdgePropertyAgg, RelType: "R", AggProperty: "weight", AggFunctions: []string{"avg", "count"}}},
	}
	for _, pattern := range patterns {
		for _, policy := range []string{"failure", "missing", "healthy", "issued-pending"} {
			t.Run(pattern.name+"/"+policy, func(t *testing.T) {
				base := storage.NewMemoryEngine()
				defer base.Close()
				tenant := storage.NewNamespacedEngine(base, "tenant")
				for _, id := range []storage.NodeID{"a", "b"} {
					_, err := tenant.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"name": string(id)}})
					require.NoError(t, err)
				}
				var issued storage.Engine = tenant
				if policy == "failure" {
					issued = &foundationOptimizerReadFault{Engine: tenant, failure: errors.New("required optimizer point read failed")}
				}
				if policy == "missing" {
					issued = &foundationOptimizerReadFault{Engine: tenant, failure: storage.ErrNotFound}
				}
				executor := cypher.NewStorageExecutor(issued)
				ctx := context.Background()
				if policy == "issued-pending" {
					_, err := executor.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer executor.Execute(ctx, "ROLLBACK", nil)
					_, err = executor.Execute(ctx, "MATCH (a:Doc {name:'a'}),(b:Doc {name:'b'}) CREATE (a)-[:R {weight:1}]->(b),(b)-[:R {weight:1}]->(a)", nil)
					require.NoError(t, err)
				} else {
					for _, edge := range []*storage.Edge{{ID: "ab", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]any{"weight": int64(1)}}, {ID: "ba", StartNode: "b", EndNode: "a", Type: "R", Properties: map[string]any{"weight": int64(1)}}} {
						require.NoError(t, tenant.CreateEdge(edge))
					}
				}
				result, handled := executor.ExecuteOptimized(ctx, pattern.query, pattern.info)
				if policy == "failure" {
					require.False(t, handled)
					require.Nil(t, result)
				} else {
					require.True(t, handled)
					require.NotNil(t, result)
					if policy == "missing" {
						require.Empty(t, result.Rows)
					} else {
						require.NotEmpty(t, result.Rows)
					}
				}
			})
		}
	}
}
