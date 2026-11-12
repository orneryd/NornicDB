package search

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFulltextLiteralRetentionAccountsAndDetachesActiveLease(t *testing.T) {
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	nodes := make([]*storage.Node, 256)
	for i := range nodes {
		text := "ordinary text"
		if i == 19 {
			text = "selected unique"
		}
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("literal-retention:%d", i)), Properties: map[string]interface{}{"text": text}}
	}
	require.NoError(t, store.BulkCreateNodes(t.Context(), nodes))
	const limit = int64(8 << 20)
	const headroom = int64(1 << 20)
	var used atomic.Int64
	used.Store(4 << 20)
	policy := memory.NewPolicy(limit, headroom, time.Hour, func() (memory.Sample, error) { return memory.Sample{Used: used.Load()}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	options := DefaultServiceOptions("")
	options.MemoryPolicy = policy
	service := NewService(store, DefaultVectorDimensions, &options)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	scope := FulltextDocumentScope{Name: "literal-retention", Definition: "text"}
	build := func(node *storage.Node, _ *storage.Edge) *FulltextDocument {
		return &FulltextDocument{Text: node.Properties["text"].(string), Tokens: 2}
	}
	selected := func(ctx context.Context, documents FulltextDocuments) {
		t.Helper()
		var ids []string
		require.NoError(t, ReadFulltextCandidates(ctx, documents, FulltextLiteralSelector{Op: FulltextAllOf, Children: []FulltextLiteralSelector{{Op: FulltextLiteral, Text: "selected"}, {Op: FulltextLiteral, Text: "unique"}}}, FulltextLiteralTree{}, func(id string, doc *FulltextDocument) error {
			if doc.Text == "selected unique" {
				ids = append(ids, id)
			}
			return nil
		}))
		require.Equal(t, []string{"literal-retention:19"}, ids)
		require.NotNil(t, documents.view.literals, "the control must exceed the signature threshold")
		require.Positive(t, cap(documents.view.literals.plans), "retained accounting must include compound-plan scratch")
	}
	var first *fulltextDocumentView
	require.NoError(t, service.ReadFulltextDocuments(t.Context(), store, scope, build, func(ctx context.Context, documents FulltextDocuments) error {
		selected(ctx, documents)
		first = documents.view
		return nil
	}))
	status := service.FulltextRetention()
	require.Len(t, status, 1)
	require.True(t, status[0].Retained)
	require.GreaterOrEqual(t, status[0].Bytes, first.retainedBytes+int64(len(first.documents))*192+first.literals.retainedBytes(), "report includes posting and slot capacity")
	require.NoError(t, service.ReadFulltextDocuments(t.Context(), store, scope, build, func(ctx context.Context, documents FulltextDocuments) error {
		require.Same(t, first, documents.view)
		selected(ctx, documents)
		used.Store(limit - headroom + 1)
		require.NoError(t, policy.Refresh())
		released := service.FulltextRetention()
		require.Len(t, released, 1)
		require.True(t, released[0].StorageReads)
		require.False(t, released[0].Retained)
		selected(ctx, documents)
		used.Store(4 << 20)
		require.NoError(t, policy.Refresh())
		return nil
	}))
	status = service.FulltextRetention()
	require.Len(t, status, 1)
	require.True(t, status[0].StorageReads, "an active detached generation cannot republish after pressure recovers")
	require.False(t, status[0].Retained)
	require.NoError(t, service.ReadFulltextDocuments(t.Context(), store, scope, build, func(ctx context.Context, documents FulltextDocuments) error {
		require.NotSame(t, first, documents.view)
		selected(ctx, documents)
		return nil
	}))
	status = service.FulltextRetention()
	require.Len(t, status, 1)
	require.True(t, status[0].Retained)
}
