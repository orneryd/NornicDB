package cypher

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Startup maintenance rebuilds MVCC heads before traffic, and pruning runs
// afterwards. A node deleted and recreated before that rebuild must stay
// readable through public Cypher: its live body replaces the archived
// tombstone as the current head, so pruning keeps the numeric identity that
// the relationship indexes address.
func TestRebuildThenPruneKeepsRecreatedNodeRelationshipsReadable(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			ctx := t.Context()

			_, err = base.CreateNode(ctx, &storage.Node{ID: "test:a", Labels: []string{"Endpoint"}, Properties: map[string]any{"name": "a"}})
			require.NoError(t, err)
			_, err = base.CreateNode(ctx, &storage.Node{ID: "test:b", Labels: []string{"Endpoint"}, Properties: map[string]any{"name": "b"}})
			require.NoError(t, err)
			require.NoError(t, base.DeleteNode(ctx, "test:a"))
			_, err = base.CreateNode(ctx, &storage.Node{ID: "test:a", Labels: []string{"Endpoint"}, Properties: map[string]any{"name": "a"}})
			require.NoError(t, err)
			require.NoError(t, base.CreateEdge(ctx, &storage.Edge{
				ID:        "test:e",
				StartNode: "test:a",
				EndNode:   "test:b",
				Type:      "LINK",
			}))

			require.NoError(t, base.RebuildMVCCHeads(ctx))
			_, err = base.PruneMVCCVersions(ctx, storage.MVCCPruneOptions{})
			require.NoError(t, err)

			executor := NewStorageExecutor(base)
			traversal, err := executor.Execute(ctx, "MATCH (a:Endpoint {name: 'a'})-[:LINK]->(b:Endpoint {name: 'b'}) RETURN a.name AS start, b.name AS finish", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"a", "b"}}, traversal.Rows)

			labels, err := executor.Execute(ctx, "MATCH (n:Endpoint) RETURN n.name AS name ORDER BY name", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"a"}, {"b"}}, labels.Rows)
		})
	}
}
