package bolt

import (
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestV152PortComputedMapScopedCall uses the recovered CONTEXTS statement from
// the V2 NOR-400 Bolt probe and its one synthetic node.
func TestV152PortComputedMapScopedCall(t *testing.T) {
	ctx := t.Context()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	_, err := store.CreateNode(ctx, &storage.Node{
		ID: "context", Labels: []string{"PDGraphNode", "PDContext"},
		Properties: map[string]any{"id": "context", "kind": "case", "search_text": "synthetic"},
	})
	require.NoError(t, err)
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(ctx)
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	const scoped = `
UNWIND $ids AS context_id
MATCH (n:PDGraphNode:PDContext {id: coalesce(context_id, 'fallback')})
CALL (n) {
  OPTIONAL MATCH (n)<-[link:PD_RELATION {kind: 'point_of'}]-(point:PDPoint:PDGraphNode)
  WHERE coalesce(link.eff_usable, false)
  WITH DISTINCT point ORDER BY point.id
  RETURN collect(CASE WHEN point IS NOT NULL THEN {id: point.id,
           statement: point.statement, revision: coalesce(point.revision, 0)} END) AS points
}
RETURN n.id AS id, n.kind AS kind, coalesce(n.label, n.name) AS label,
       n.search_text AS text, points, CASE WHEN n IS NULL THEN null ELSE toString(uuid.v5('d0522127-356e-598a-b9ee-637f373807a7', apoc.convert.toJson({kind: n.kind, label: coalesce(n.label, n.name), text: n.search_text, points: points}))) END AS version, coalesce(n.collection_ids, []) AS scope,
       n.updated_at AS updated_at
ORDER BY n.id`

	for _, test := range []struct {
		name, statement string
	}{
		{"scoped", scoped},
		{"importing-with", strings.Replace(scoped, "CALL (n) {", "CALL { WITH n", 1)},
		{"plain-property-map", strings.Replace(scoped, "coalesce(context_id, 'fallback')", "context_id", 1)},
	} {
		for _, id := range []string{"absent", "context"} {
			t.Run(test.name+"/"+id, func(t *testing.T) {
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				defer tx.Close(ctx)
				result, err := tx.Run(ctx, test.statement, map[string]any{"ids": []any{id}})
				require.NoError(t, err)
				records, err := result.Collect(ctx)
				require.NoError(t, err)
				require.NoError(t, tx.Commit(ctx))
				if id == "absent" {
					require.Empty(t, records)
					return
				}
				require.Len(t, records, 1)
				require.Equal(t, id, records[0].AsMap()["id"])
				require.Empty(t, records[0].AsMap()["points"])
				require.NotEmpty(t, records[0].AsMap()["version"])
			})
		}
	}
}
