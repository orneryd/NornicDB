package search

import (
	"slices"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestL9MaterializationOwner(t *testing.T) {
	hit := SearchResult{
		ID: "n",
		Labels: []string{"Original"},
		Properties: map[string]any{"nested": []any{"original"}},
	}
	original := hit.MaterializedNode()
	detached := *original
	detached.Labels = slices.Clone(original.Labels)
	detached.Properties = map[string]any{"nested": slices.Clone(original.Properties["nested"].([]any))}
	detached.Labels[0] = "Control"
	detached.Properties["nested"].([]any)[0] = "control"
	require.Equal(t, "Original", hit.Labels[0])
	require.Equal(t, "original", hit.Properties["nested"].([]any)[0])

	t.Logf("labels: retained=%p returned=%p", &hit.Labels[0], &original.Labels[0])
	require.Same(t, &hit.Labels[0], &original.Labels[0])
	original.Labels[0] = "Exposed"
	original.Properties["nested"].([]any)[0] = "exposed"
	second := hit.MaterializedNode()
	require.Equal(t, []string{"Exposed"}, slices.Clone(second.Labels))
	require.Equal(t, []any{"exposed"}, second.Properties["nested"])
	require.Equal(t, storage.NodeID("n"), second.ID)
}
