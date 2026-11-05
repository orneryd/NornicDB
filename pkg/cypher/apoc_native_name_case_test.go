package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNativeAPOCCanonicalCamelCaseWithoutPlugin(t *testing.T) {
	// Native fallback must work without a registered plugin masking name checks.
	withPluginFunctions(t, nil)
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical_apoc"))
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (:CanonicalAPOC {key:'a'})-[:CanonicalAPOC]->(:CanonicalAPOC {key:'b'})", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH ()-[r:CanonicalAPOC]->() RETURN apoc.rel.startNode(r).key, apoc.rel.endNode(r).key", [][]interface{}{{"a", "b"}}},
		{"RETURN apoc.rel.startNode(null), apoc.rel.endNode(null)", [][]interface{}{{nil, nil}}},
		{"MATCH ()-[r:CanonicalAPOC]->() WITH apoc.rel.startNode(r) AS n RETURN n.key", [][]interface{}{{"a"}}},
		{"RETURN apoc.coll.containsAll([1,2],[2]), apoc.coll.indexOf([1,2],2)", [][]interface{}{{true, int64(1)}}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, err := e.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Rows)
		})
	}
	for _, name := range []string{"apoc.rel.startnode", "apoc.rel.endnode", "apoc.rel.StartNode", "apoc.rel.endNODE", "APOC.REL.STARTNODE", "apoc.Node.id"} {
		t.Run(name, func(t *testing.T) {
			_, err := e.Execute(ctx, "RETURN "+name+"(null)", nil)
			requireSemanticDetail(t, err, "UnknownFunction")
		})
	}
}
