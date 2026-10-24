package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type candidateFilterReadSpy struct {
	storage.Engine

	lightNodes          map[storage.NodeID]*storage.Node
	fullBatchReads      int
	fullIndividualReads int
	lightBatchReads     int
	lightReads          int
	batchErr            error
}

func BenchmarkTypeOnlyCandidateFilterFromMetadata(b *testing.B) {
	service := NewService(fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)))
	b.Cleanup(func() { require.NoError(b, service.engine.Close()) })
	results := make([]indexResult, 1000)
	for i := range results {
		id := fmt.Sprintf("doc-%d", i)
		results[i] = indexResult{ID: id}
		if i&1 == 0 {
			service.nodeLabels[id] = []string{"Document"}
		} else {
			service.nodeLabels[id] = []string{"Other"}
		}
	}
	b.ReportAllocs()
	scratch := make([]indexResult, len(results))
	b.ResetTimer()
	for range b.N {
		copy(scratch, results)
		got := service.filterByTypeAndProperties(context.Background(), scratch, &SearchOptions{Types: []string{"Document"}}, nil)
		if len(got) != 500 {
			b.Fatalf("got %d matches", len(got))
		}
	}
}

func (s *candidateFilterReadSpy) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	s.fullBatchReads++
	return s.Engine.BatchGetNodes(ids)
}

func (s *candidateFilterReadSpy) GetNode(id storage.NodeID) (*storage.Node, error) {
	s.fullIndividualReads++
	return s.Engine.GetNode(id)
}

func (s *candidateFilterReadSpy) BatchGetNodesWithoutEmbeddings(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	s.lightBatchReads++
	if s.batchErr != nil {
		return nil, s.batchErr
	}
	result := make(map[storage.NodeID]*storage.Node, len(ids))
	for _, id := range ids {
		if node, ok := s.lightNodes[id]; ok {
			result[id] = node
		}
	}
	return result, nil
}

func (s *candidateFilterReadSpy) GetNodeWithoutEmbeddings(id storage.NodeID) (*storage.Node, error) {
	s.lightReads++
	if node, ok := s.lightNodes[id]; ok {
		return node, nil
	}
	return nil, storage.ErrNotFound
}

func TestCandidateFiltersUseEmbeddingFreeBatchReads(t *testing.T) {
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	spy := &candidateFilterReadSpy{
		Engine: base,
		lightNodes: map[storage.NodeID]*storage.Node{
			"doc-a": {ID: "doc-a", Labels: []string{"Document"}, Properties: map[string]any{"state": "ready"}},
			"doc-b": {ID: "doc-b", Labels: []string{"Document"}, Properties: map[string]any{"state": "draft"}},
		},
	}
	service := NewService(spy)
	for _, node := range spy.lightNodes {
		require.NoError(t, service.IndexNode(node))
	}

	got := service.filterByTypeAndProperties(
		context.Background(),
		[]indexResult{{ID: "doc-a"}, {ID: "doc-b"}},
		&SearchOptions{Types: []string{"Document"}, Filters: map[string][]string{"state": {"ready"}}},
		map[string]bool{})

	require.Equal(t, []indexResult{{ID: "doc-a"}}, got)
	require.Equal(t, 1, spy.lightBatchReads)
	require.Zero(t, spy.fullBatchReads, "candidate filtering must not decode stored embeddings")
	require.Zero(t, spy.fullIndividualReads, "candidate filtering must not decode stored embeddings")
	require.Zero(t, spy.lightReads, "supported batch readers must not degrade to per-node reads")
}

func TestTypeOnlyCandidateFilterUsesInMemoryMetadata(t *testing.T) {
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	spy := &candidateFilterReadSpy{
		Engine: base,
		lightNodes: map[storage.NodeID]*storage.Node{
			"label-match": {
				ID:         "label-match",
				Labels:     []string{"Document"},
				Properties: map[string]any{"body": "large document text"},
			},
			"property-match": {
				ID:         "property-match",
				Labels:     []string{"Legacy"},
				Properties: map[string]any{"type": "Document", "body": "large document text"},
			},
			"miss": {
				ID:         "miss",
				Labels:     []string{"Other"},
				Properties: map[string]any{"body": "large document text"},
			},
		},
	}
	service := NewService(spy)
	for _, node := range spy.lightNodes {
		require.NoError(t, service.IndexNode(node))
	}

	got := service.filterByTypeAndProperties(
		context.Background(),
		[]indexResult{{ID: "label-match"}, {ID: "property-match"}, {ID: "miss"}},
		&SearchOptions{Types: []string{"document"}},
		map[string]bool{})

	require.Equal(t, []indexResult{{ID: "label-match"}, {ID: "property-match"}}, got)
	require.Zero(t, spy.lightBatchReads, "type-only filtering must not read document properties")
	require.Zero(t, spy.lightReads, "type-only filtering must not read document properties")
	require.Zero(t, spy.fullBatchReads, "type-only filtering must not decode stored nodes")
	require.Zero(t, spy.fullIndividualReads, "type-only filtering must not decode stored nodes")
}
