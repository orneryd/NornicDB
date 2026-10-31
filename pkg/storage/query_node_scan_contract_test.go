// Caller-ownership correction author: Codex, NOR-160, 29 October 2026.
package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueryNodeScanDecayOptionAcrossReadViews(t *testing.T) {
	for _, view := range []string{"storage", "physical", "logical", "current", "buffered-storage", "buffered-tx"} {
		t.Run(view, func(t *testing.T) {
			var b *BadgerEngine
			buffered := view == "buffered-storage" || view == "buffered-tx"
			if buffered {
				b = newWriteBehindTestEngine(t, time.Hour)
			} else {
				var err error
				b, err = runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
				require.NoError(t, err)
				t.Cleanup(func() { _ = b.Close() })
			}
			b.SetDecayEnabled(true)
			for _, node := range []*Node{
				{ID: "other:visible", Labels: []string{"Doc"}},
				{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{VisibilitySuppressed: true}}, ID: "test:hidden", Labels: []string{"Doc"}},
				{ID: "test:visible", Labels: []string{"Doc"}},
			} {
				if buffered && node.ID != "other:visible" {
					bufferedCreate(t, b, node)
				} else {
					_, err := b.CreateNode(node)
					require.NoError(t, err)
				}
			}
			if buffered {
				_, ok := b.writeBehind.LookupNode("test:hidden")
				require.True(t, ok, "decay options must reach the unflushed overlay")
			}
			scan := b.StreamNodesWithOptions
			if view != "storage" && view != "buffered-storage" {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				if view == "logical" || view == "current" {
					tx.snapshotTx.Discard()
					tx.snapshotTx = nil
					tx.readTS = b.currentMVCCReadVersion("test")
					if view == "current" {
						tx.readTS = MVCCVersion{}
					}
				}
				scan = tx.StreamNodesWithOptions
			}
			// Flip the option on the same transaction to exercise its cached
			// label populations, including replay after an early stop.
			for _, apply := range []bool{true, false, true, false} {
				for _, label := range []string{"", "Doc"} {
					for _, stop := range []bool{true, false} {
						t.Run(fmt.Sprintf("decay=%v/label=%s/stop=%v", apply, label, stop), func(t *testing.T) {
							var ids []NodeID
							err := scan(context.Background(), StreamNodesOptions{
								Prefix: "test:", Label: label, QueryNodes: true, ApplyDecayFilter: apply,
							}, func(node *Node) error {
								ids = append(ids, node.ID)
								if stop {
									return ErrIterationStopped
								}
								return nil
							})
							require.NoError(t, err)
							if stop || apply {
								require.Len(t, ids, 1)
							} else {
								require.ElementsMatch(t, []NodeID{"test:hidden", "test:visible"}, ids)
							}
							if apply {
								require.Equal(t, []NodeID{"test:visible"}, ids)
							}
						})
					}
				}
			}
		})
	}
}

func TestQueryNodeScanBufferedCallerCopy(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, b, &Node{
		ID: "test:buffered", Labels: []string{"Doc"}, Properties: map[string]any{"title": "original"},
		ChunkEmbeddings: [][]float32{{1, 2, 3}}, NamedEmbeddings: map[string][]float32{"named": {4, 5, 6}},
		EmbedMeta: map[string]any{"has_embedding": true, "chunk_count": 1},
	})
	resident, ok := b.writeBehind.LookupNode("test:buffered")
	require.True(t, ok, "the regression must exercise an unflushed overlay")
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for name, scan := range map[string]func(context.Context, StreamNodesOptions, func(*Node) error) error{
		"storage": b.StreamNodesWithOptions, "transaction": tx.StreamNodesWithOptions,
	} {
		for _, label := range []string{"", "Doc"} {
			for _, strip := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/label=%s/strip=%v", name, label, strip), func(t *testing.T) {
					rows := 0
					var got *Node
					err := scan(context.Background(), StreamNodesOptions{
						Prefix: "test:", Label: label, QueryNodes: true, StripEmbeddings: strip,
					}, func(node *Node) error {
						rows++
						got = node
						return nil
					})
					require.NoError(t, err)
					require.Equal(t, 1, rows)
					require.Equal(t, resident.ID, got.ID)
					require.Equal(t, "original", got.Properties["title"])
					if strip {
						require.Empty(t, got.ChunkEmbeddings)
						require.Empty(t, got.NamedEmbeddings)
					} else {
						require.NotSame(t, &resident.ChunkEmbeddings[0][0], &got.ChunkEmbeddings[0][0], "returned resident vectors are caller-owned")
						require.NotSame(t, &resident.NamedEmbeddings["named"][0], &got.NamedEmbeddings["named"][0])
					}
					got.Properties["title"] = "caller"
					require.Equal(t, "original", resident.Properties["title"], "caller properties must be isolated")
				})
			}
		}
	}
	t.Run("stop-before-pending-rows", func(t *testing.T) {
		_, err := tx.CreateNode(&Node{ID: "test:pending", Labels: []string{"Doc"}})
		require.NoError(t, err)
		rows := 0
		err = tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{Label: "Doc", QueryNodes: true}, func(*Node) error {
			rows++
			return ErrIterationStopped
		})
		require.NoError(t, err)
		require.Equal(t, 1, rows, "an overlay stop must not continue into pending rows")
	})
}

func TestQueryNodeScanNestedVisitorsKeepTheirOptions(t *testing.T) {
	b, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 2}}})
		require.NoError(t, err)
	}
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	var outer, inner []NodeID
	var innerVectors [][]float32
	err = tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
		Label: "Doc", Prefix: "test:a", QueryNodes: true,
	}, func(node *Node) error {
		outer = append(outer, node.ID)
		return tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
			Label: "Doc", Prefix: "test:b", QueryNodes: true, StripEmbeddings: true,
		}, func(nested *Node) error {
			inner = append(inner, nested.ID)
			innerVectors = nested.ChunkEmbeddings
			return nil
		})
	})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:a"}, outer)
	require.Equal(t, []NodeID{"test:b"}, inner)
	require.Empty(t, innerVectors)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = tx.StreamNodesWithOptions(ctx, StreamNodesOptions{Label: "Doc", QueryNodes: true}, func(*Node) error {
		cancel()
		return ErrIterationStopped
	})
	require.NoError(t, err)
	var reused []NodeID
	err = tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
		Label: "Doc", Prefix: "test:b", QueryNodes: true,
	}, func(node *Node) error { reused = append(reused, node.ID); return nil })
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:b"}, reused, "a later scan must not retain another scan's context or prefix")
}

func TestQueryNodeScanLogicalSnapshotsSkipStoredVectors(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		for _, current := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/current=%v", tc.name, current), func(t *testing.T) {
				b := newSidecarTestBadger(t)
				tc.create(t, b, "test:lazy")
				corruptFirstEmbeddingChunk(t, b, "test:lazy")
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				tx.snapshotTx.Discard()
				tx.snapshotTx = nil
				tx.readTS = b.currentMVCCReadVersion("test")
				if current {
					tx.readTS = MVCCVersion{}
				}
				for _, label := range []string{"", "Doc"} {
					b.nodeCacheMu.RLock()
					resident := b.nodeCache["test:lazy"]
					b.nodeCacheMu.RUnlock()
					var got *Node
					err := tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
						Prefix: "test:", Label: label, QueryNodes: true,
					}, func(node *Node) error { got = node; return nil })
					require.NoError(t, err)
					require.NotNil(t, got)
					require.True(t, got.HasChunkEmbeddings())
					if len(got.ChunkEmbeddings) == 0 {
						require.Equal(t, tc.chunks, chunkCountFromMeta(got.EmbedMeta))
					} else {
						require.NotNil(t, resident)
						require.NotSame(t, &resident.ChunkEmbeddings[0][0], &got.ChunkEmbeddings[0][0], "resident vectors remain caller-owned")
					}
				}
			})
		}
	}
}
