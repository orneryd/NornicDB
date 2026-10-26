// Author: Codex implementation lane, 2026-10-27. Pending-marker public result contract.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFulltextMaintainedPublicPendingMarkerNeutrality(t *testing.T) {
	exec, store := fulltextMaintainedFixture(t, 80)
	base := store.GetInnerEngine().(*storage.BadgerEngine)
	node, err := store.GetNode("n000040")
	require.NoError(t, err)
	node.NamedEmbeddings = map[string][]float32{"title": {1, 2}}
	node.EmbedMeta = map[string]any{"model": "fixture"}
	require.NoError(t, store.UpdateNode(node))
	for _, text := range []string{"hotel", "*", "text:*"} {
		query := "CALL db.index.fulltext.queryNodes('source_text',$q) YIELD node,score RETURN node,score"
		before, err := exec.Execute(context.Background(), query, map[string]any{"q": text})
		require.NoError(t, err)
		base.MarkNodeEmbedded(store.PrefixNodeID(node.ID))
		scans := store.scans.Load()
		after, err := exec.Execute(context.Background(), query, map[string]any{"q": text})
		require.NoError(t, err)
		require.Equal(t, before.Rows, after.Rows, "every returned entity field and score must survive pending-marker retirement")
		// Ranked demand reuses its complete admitted corpus. Ordered demand
		// performs the original materialized acquisition on each invocation;
		// marker neutrality must not be mistaken for a prefix-cache obligation.
		expectedScans := scans
		if text != "hotel" {
			expectedScans++
		}
		require.Equal(t, expectedScans, store.scans.Load())
	}
}
