// Author: Codex implementation lane, 2026-10-27 (NOR-148 representation retention).
package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Retiring a large vocabulary and repeatedly replacing it must preserve the
// release score inputs without another storage population. Rehashing is an
// internal representation operation, not another corpus admission.
func TestFulltextMaintainedPublicVocabularyRetirement(t *testing.T) {
	for _, relationships := range []bool{false, true} {
		t.Run(fmt.Sprint(relationships), func(t *testing.T) {
			exec, store := fulltextMaintainedFixture(t, 80)
			ctx := context.Background()
			procedure, index, column := "queryNodes", "source_text", "node"
			if relationships {
				procedure, index, column = "queryRelationships", "link_text", "relationship"
				_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX link_text FOR ()-[r:LINK]-() ON EACH [r.text,r.title]", nil)
				require.NoError(t, err)
				for i := 0; i < 20; i++ {
					id := storage.EdgeID(fmt.Sprintf("edge%02d", i))
					require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: id, StartNode: "n000000", EndNode: "n000040", Type: "LINK", Properties: map[string]any{"id": string(id), "text": strings.Repeat("hotel vocabulary retired ", 200), "title": "Correspondence"}}))
				}
			}
			query := fmt.Sprintf("CALL db.index.fulltext.%s('%s','hotel') YIELD %s,score RETURN %s.id,score", procedure, index, column, column)
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			scans := store.scans.Load()
			for round := 0; round < 3; round++ {
				for i := 0; i < 20; i++ {
					text := []string{"hotel", "routine", "hotel"}[round]
					if relationships {
						edge, err := store.GetEdge(ctx, storage.EdgeID(fmt.Sprintf("edge%02d", i)))
						require.NoError(t, err)
						edge.Properties["text"] = text
						require.NoError(t, store.UpdateEdge(ctx, storage.EdgeUpdateRequest{After: edge}))
					} else {
						node, err := store.GetNode(ctx, storage.NodeID(fmt.Sprintf("n%06d", i*4)))
						require.NoError(t, err)
						node.Properties["text"] = text
						require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: node}))
					}
					var expectedEngine storage.Engine = store.NamespacedEngine
					if relationships {
						edges, err := store.NamespacedEngine.AllEdges(ctx)
						require.NoError(t, err)
						var rows []*storage.Node
						for _, edge := range edges {
							rows = append(rows, &storage.Node{ID: storage.NodeID(edge.ID), Labels: []string{"Source"}, Properties: edge.Properties})
						}
						expectedEngine = &fulltextCapturedEngine{Engine: store.NamespacedEngine, rows: rows}
					}
					expected := fulltextReleaseOracle(t, expectedEngine, "hotel")
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Equal(t, expected, result.Rows)
					require.Equal(t, scans, store.scans.Load(), "vocabulary retirement must rehash the admitted representation, not rescan storage")
				}
			}
		})
	}
}
