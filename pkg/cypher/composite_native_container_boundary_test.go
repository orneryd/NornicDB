package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNativeParameterCacheSnapshotBoundary(t *testing.T) {
	for _, relationship := range []bool{false, true} {
		var first, different, same, foreign interface{}
		if relationship {
			left := &storage.Edge{ID: "same", Database: "one", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]interface{}{"a": "b c:d"}}
			right := storage.CopyEdge(left)
			right.Properties = map[string]interface{}{"a": "b", "c": "d"}
			first, different, same = left, right, storage.CopyEdge(left)
			foreign = left.WithIdentity(storage.EntityIdentity{Database: "two", LocalID: "same"})
		} else {
			left := &storage.Node{ID: "same", Database: "one", Labels: []string{"P"}, Properties: map[string]interface{}{"a": "b c:d"}}
			right := storage.CopyNode(left)
			right.Properties = map[string]interface{}{"a": "b", "c": "d"}
			first, different, same = left, right, storage.CopyNode(left)
			foreign = left.WithIdentity(storage.EntityIdentity{Database: "two", LocalID: "same"})
		}
		for _, wrap := range []struct {
			name  string
			value func(interface{}) interface{}
		}{
			{"direct", func(v interface{}) interface{} { return v }},
			{"list", func(v interface{}) interface{} { return []interface{}{v} }},
			{"array", func(v interface{}) interface{} { return [1]interface{}{v} }},
			{"map", func(v interface{}) interface{} { return map[string]interface{}{"entity": v} }},
		} {
			t.Run(map[bool]string{false: "node", true: "relationship"}[relationship]+"/"+wrap.name, func(t *testing.T) {
				cache := NewQueryCache(10)
				query := "WITH $value AS n RETURN n"
				params := func(value interface{}) map[string]interface{} {
					return map[string]interface{}{"value": wrap.value(value)}
				}
				cache.Put(query, params(first), &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{"first snapshot"}}}, time.Minute)
				_, hit := cache.Get(query, params(same))
				require.True(t, hit, "equivalent detached snapshot should hit")
				_, hit = cache.Get(query, params(foreign))
				require.False(t, hit, "foreign source must not reuse a same-local-ID entry")
				_, hit = cache.Get(query, params(different))
				require.False(t, hit, "native snapshot properties cannot be keyed by their ambiguous formatted text")
			})
		}
	}
}

// Exercise the path aggregation owners with detached views of the same native
// entity. The field guard cannot see identity consumed through formatted values.
func TestNativeTraversalSnapshotAggregationBoundary(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "one"))
	node := &storage.Node{ID: "same", Database: "one", Properties: map[string]interface{}{"v": int64(1)}}
	nodeView := storage.CopyNode(node)
	nodeView.Properties["v"] = int64(2)
	foreignNode := node.WithIdentity(storage.EntityIdentity{Database: "two", LocalID: "same"})
	edge := &storage.Edge{ID: "same-rel", Database: "one", StartNode: "same", EndNode: "target", Type: "R", Properties: map[string]interface{}{"v": int64(1)}}
	edgeView := storage.CopyEdge(edge)
	edgeView.Properties["v"] = int64(2)
	foreignEdge := edge.WithIdentity(storage.EntityIdentity{Database: "two", LocalID: "same-rel"})
	paths := []PathResult{{Nodes: []*storage.Node{node}, Relationships: []*storage.Edge{edge}}, {Nodes: []*storage.Node{nodeView}, Relationships: []*storage.Edge{edgeView}}, {Nodes: []*storage.Node{foreignNode}, Relationships: []*storage.Edge{foreignEdge}}}
	match := &TraversalMatch{StartNode: nodePatternInfo{variable: "n"}, Relationship: RelationshipPattern{Variable: "r"}}
	for _, variable := range []string{"n", "r"} {
		t.Run(variable, func(t *testing.T) {
			count := executor.aggregatePathCount(context.Background(), paths, match, "count(DISTINCT "+variable+")")
			require.Equal(t, int64(2), count, "same identity is one count regardless of detached properties")
			collected := executor.aggregatePathCollect(context.Background(), paths, match, "collect(DISTINCT "+variable+")", true)
			require.Len(t, collected, 2, "collect and count must share native entity equivalence")
		})
	}
}
