package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NORNICDB_SEARCH_BM25_LABELS: only nodes carrying one of the listed labels
// are keyword hits; when a node loses its listed label, it stops being one;
// the label list joins the BM25 build fingerprint only when set (#1067).
func TestServiceFulltextLabelAllowlist(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	service := NewService(base)
	service.SetFulltextLabels([]string{"Searchable", " ", "Source", "Searchable"})
	require.Equal(t, []string{"Searchable", "Source"}, service.FulltextLabels())

	listed := &storage.Node{ID: "listed", Labels: []string{"Doc", "Searchable"}, Properties: map[string]any{"text": "zephyr report"}}
	other := &storage.Node{ID: "other", Labels: []string{"Doc"}, Properties: map[string]any{"text": "zephyr memo"}}
	require.NotEmpty(t, service.extractSearchableText(listed))
	require.Empty(t, service.extractSearchableText(other))
	require.NoError(t, service.IndexNode(listed))
	require.NoError(t, service.IndexNode(other))

	ids := func() []string {
		var out []string
		for _, hit := range service.fulltext().Search("zephyr", 10) {
			out = append(out, hit.ID)
		}
		return out
	}
	require.Equal(t, []string{"listed"}, ids())

	listed.Labels = []string{"Doc"}
	require.NoError(t, service.IndexNode(listed))
	require.Empty(t, ids(), "a node that lost its listed label is no longer a keyword hit")

	require.Contains(t, service.composeBM25BuildSettings(), ";labels=Searchable,Source")
	unset := NewService(storage.NewMemoryEngine())
	require.NotContains(t, unset.composeBM25BuildSettings(), "labels=")
	require.NotEqual(t, unset.composeBM25BuildSettings(), service.composeBM25BuildSettings())
}
