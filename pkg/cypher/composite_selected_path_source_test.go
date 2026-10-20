package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A bound foreign endpoint cannot become the selected graph's same-ID node,
// including a zero-hop match. Both uniform and quantified searches obey this.
func TestCompositeSelectedPathSourceBoundary(t *testing.T) {
	_, manager := newCompositeIdentityExecutor(t, false)
	first, err := manager.GetStorage("one")
	require.NoError(t, err)
	second, err := manager.GetStorage("two")
	require.NoError(t, err)
	source, err := first.GetNode("same")
	require.NoError(t, err)
	executor := NewStorageExecutor(first)
	for _, endpoint := range []struct {
		name  string
		store storage.Engine
		id    storage.NodeID
		want  int64
	}{{"same", first, "same", 1}, {"reachable", first, "target", 1}, {"foreign same local ID", second, "same", 0}, {"foreign reachable local ID", second, "target", 0}} {
		t.Run(endpoint.name, func(t *testing.T) {
			target, err := endpoint.store.GetNode(endpoint.id)
			require.NoError(t, err)
			ordinary, err := executor.Execute(context.Background(), "WITH $source AS a,$target AS b MATCH p=(a)-[:R*0..2]->(b) RETURN count(*)", map[string]interface{}{"source": source, "target": target})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{endpoint.want}}, ordinary.Rows)
			for _, pattern := range []string{"(a)-[:R*0..2]->(b)", "(a)(()-[:R]->()){0,2}(b)"} {
				result, err := executor.Execute(context.Background(), "WITH $source AS a,$target AS b MATCH p = ANY SHORTEST "+pattern+" RETURN count(*)", map[string]interface{}{"source": source, "target": target})
				require.NoError(t, err, pattern)
				require.Equal(t, [][]interface{}{{endpoint.want}}, result.Rows, pattern)
			}
		})
	}
}
