package search

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFulltextDocumentLabelScopeReplacesRetainedView(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	service := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, label := range []string{"First", "Second"} {
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID("scope:" + label), Labels: []string{label}, Properties: map[string]any{"text": "hello"}})
		require.NoError(t, err)
	}
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		return &FulltextDocument{Text: node.Properties["text"].(string), Tokens: 1}
	}
	for _, label := range []string{"First", "Second", "First"} {
		scope := FulltextDocumentScope{Name: "same", Definition: "same", QueryNodes: true, NodeLabels: []string{label}}
		var ids []string
		err := service.ReadFulltextDocuments(t.Context(), store, scope, build, func(ctx context.Context, docs FulltextDocuments) error {
			return docs.Each(ctx, func(id string, _ *FulltextDocument) error { ids = append(ids, id); return nil })
		})
		require.NoError(t, err)
		require.Equal(t, []string{"scope:" + label}, ids, "label selection belongs to retained-view identity")
	}
}
