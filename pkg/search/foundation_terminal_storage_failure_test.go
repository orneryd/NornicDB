// Author: Lane #1644 (Codex). Public search storage-failure and recovery policy.
package search

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
)

type foundationSearchPointView struct {
	storage.Engine
	pointFailure error
	points       atomic.Int64
	topology     atomic.Int64
}

func (v *foundationSearchPointView) ReadCapabilitySource() (storage.Engine, bool) {
	v.topology.Add(1)
	return nil, false
}
func (v *foundationSearchPointView) GetNode(id storage.NodeID) (*storage.Node, error) {
	v.points.Add(1)
	if v.pointFailure != nil {
		return nil, v.pointFailure
	}
	return v.Engine.GetNode(id)
}
func (v *foundationSearchPointView) GetNodeWithoutEmbeddings(id storage.NodeID) (*storage.Node, error) {
	n, e := v.GetNode(id)
	if n != nil {
		n.ChunkEmbeddings = nil
		n.NamedEmbeddings = nil
	}
	return n, e
}
func (v *foundationSearchPointView) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	out := map[storage.NodeID]*storage.Node{}
	for _, id := range ids {
		n, e := v.GetNode(id)
		if errors.Is(e, storage.ErrNotFound) {
			continue
		}
		if e != nil {
			return nil, e
		}
		out[id] = n
	}
	return out, nil
}

type foundationSearchBatchView struct {
	*foundationSearchPointView
	batchFailure error
	failOnce     bool
	batches      atomic.Int64
}

func (v *foundationSearchBatchView) BatchGetNodesWithoutEmbeddings(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	calls := v.batches.Add(1)
	if v.batchFailure != nil && (!v.failOnce || calls == 1) {
		return nil, v.batchFailure
	}
	return v.BatchGetNodes(ids)
}
func TestFoundationPublicSearchStorageFailure(t *testing.T) {
	for _, mode := range []string{"bm25", "vector", "hybrid"} {
		for _, policy := range []string{"point-only-failure", "batch-and-point-failure", "batch-refusal-point-failure", "point-recovery", "missing-point"} {
			t.Run(mode+"/"+policy, func(t *testing.T) {
				b := storage.NewMemoryEngine()
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				base := storage.NewNamespacedEngine(b, "tenant")
				node := &storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"content": "alpha searchable document", "eligible": "yes"}, ChunkEmbeddings: [][]float32{{1, 0}}}
				_, err := base.CreateNode(node)
				require.NoError(t, err)
				cause := errors.New("injected unrecovered storage read failure")
				point := &foundationSearchPointView{Engine: base, pointFailure: cause}
				var engine storage.Engine = point
				if policy != "point-only-failure" {
					batch := &foundationSearchBatchView{foundationSearchPointView: point, batchFailure: errors.New("recoverable batch transport failure")}
					if policy == "batch-refusal-point-failure" {
						batch.batchFailure = storage.ErrNotImplemented
					}
					engine = batch
					if policy == "point-recovery" {
						point.pointFailure = nil
						batch.failOnce = true
					}
					if policy == "missing-point" {
						point.pointFailure = storage.ErrNotFound
					}
				}
				service := NewServiceWithDimensions(engine, 2)
				t.Cleanup(func() { require.NoError(t, service.Close()) })
				require.NoError(t, service.IndexNode(node))
				opts := DefaultSearchOptions()
				opts.Limit = 3
				opts.Types = []string{"Doc"}
				opts.Filters = map[string][]string{"eligible": {"yes"}}
				query := "alpha"
				var vector []float32
				if mode != "bm25" {
					vector = []float32{1, 0}
				}
				if mode == "vector" {
					query = ""
				}
				response, err := service.Search(context.Background(), query, vector, opts)
				if policy == "point-recovery" {
					require.NoError(t, err)
					require.NotNil(t, response)
					require.Len(t, response.Results, 1)
				} else if policy == "missing-point" {
					require.NoError(t, err)
					require.NotNil(t, response)
					require.Empty(t, response.Results)
				} else {
					require.ErrorIs(t, err, cause, "unrecovered point failure must reach public Search")
					require.Nil(t, response)
				}
				require.Positive(t, point.points.Load(), "public search must actually reach the issued point reader")
			})
		}
	}
}

// Public row schedules count actual provider visits. Search's scalar point
// adapter must be bound once; increasing matches cannot rediscover each row.
func TestFoundationPublicSearchReadBindingWork(t *testing.T) {
	visits := func(count int) int64 {
		b := storage.NewMemoryEngine()
		defer b.Close()
		point := &foundationSearchPointView{Engine: storage.NewNamespacedEngine(b, "tenant")}
		service := NewServiceWithDimensions(point, 2)
		defer service.Close()
		for i := 0; i < count; i++ {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprint("doc-", i)), Labels: []string{"Doc"}, Properties: map[string]any{"content": "alpha document", "eligible": "yes"}}
			_, err := point.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, service.IndexNode(node))
		}
		point.topology.Store(0)
		opts := DefaultSearchOptions()
		opts.Limit = count
		opts.MaxCandidateLimit = count
		opts.AdaptiveOverfetch = false
		opts.Types = []string{"Doc"}
		opts.Filters = map[string][]string{"eligible": {"yes"}}
		result, err := service.Search(context.Background(), "alpha", nil, opts)
		require.NoError(t, err)
		require.Len(t, result.Results, count)
		require.GreaterOrEqual(t, point.points.Load(), int64(count))
		return point.topology.Load()
	}
	small, large := visits(4), visits(40)
	require.LessOrEqual(t, large, small+4, "provider traversal must be independent of matching row count")
}
