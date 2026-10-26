// Author: Codex implementation lane, 2026-10-27. Pending-marker public result contract.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFulltextMaintainedPublicPendingMarkerRetirement(t *testing.T) {
	exec, store := fulltextMaintainedFixture(t, 80)
	base := store.GetInnerEngine().(*storage.BadgerEngine)
	node, err := store.GetNode("n000040")
	require.NoError(t, err)
	node.NamedEmbeddings = map[string][]float32{"title": {1, 2}}
	node.EmbedMeta = map[string]any{"model": "fixture"}
	require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: node}))
	for _, text := range []string{"hotel", "*", "text:*"} {
		query := "CALL db.index.fulltext.queryNodes('source_text',$q) YIELD node,score RETURN node,score"
		before, err := exec.Execute(context.Background(), query, map[string]any{"q": text})
		require.NoError(t, err)
		base.MarkNodeEmbedded(store.PrefixNodeID(node.ID))
		scans := store.scans.Load()
		after, err := exec.Execute(context.Background(), query, map[string]any{"q": text})
		require.NoError(t, err)
		require.Equal(t, before.Rows, after.Rows, "every returned entity field and score must survive pending-marker retirement")
		// A retirement is a reader-relevant commit, so ranked demand re-reads
		// its corpus once; ordered demand performs the original materialized
		// acquisition on each invocation anyway.
		require.Equal(t, scans+1, store.scans.Load())
	}
}
