package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestResidualFinalCacheImages(t *testing.T) {
	const largePopulation = 6000
	for _, route := range []string{"ordinary", "buffered", "large", "prepared", "wal"} {
		for _, ending := range []string{"update", "delete-edge", "cascade", "move-type"} {
			t.Run(route+"/"+ending, func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: route == "buffered", LowMemory: route == "large", WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				t.Cleanup(func() { _ = b.Close() })
				for _, id := range []NodeID{"test:a", "test:b", "test:middle"} {
					_, err := b.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
				edge := &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]any{"p": int64(0)}}
				require.NoError(t, b.CreateEdge(edge))
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(route == "buffered"))
				if route == "large" {
					stageLargeCreate(t, tx, largePopulation)
				}
				edge.Properties = map[string]any{"p": int64(1)}
				require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: edge}))
				require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:middle", Properties: map[string]any{"v": int64(1)}}, Policy: ClientNodeEdits}))
				edge.Properties = map[string]any{"p": int64(2)}
				switch ending {
				case "delete-edge":
					require.NoError(t, tx.DeleteEdge(edge.ID))
				case "cascade":
					require.NoError(t, tx.DeleteGraphNode("test:a", CascadeEdges))
				case "move-type":
					edge.StartNode, edge.EndNode, edge.Type = "test:b", "test:middle", "FINAL"
					require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: edge}))
				default:
					require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: edge}))
				}
				deleted := ending == "delete-edge" || ending == "cascade"
				observed := false
				var observationErr error
				t.Cleanup(setCommitCacheObserver(func(op Operation) {
					if op.Type != OpUpdateNode || op.Node == nil || op.Node.ID != "test:middle" {
						return
					}
					observed = true
					point, err := b.GetStoredEdge("test:e")
					if deleted && errors.Is(err, ErrNotFound) {
						point, err = nil, nil
					}
					if err != nil {
						observationErr = err
						return
					}
					if deleted && point != nil {
						observationErr = fmt.Errorf("deleted point was resurrected: %v", point)
						return
					}
					if !deleted && (point == nil || !reflect.DeepEqual(point.Properties, edge.Properties) || point.Type != edge.Type || point.StartNode != edge.StartNode || point.EndNode != edge.EndNode) {
						observationErr = fmt.Errorf("point differs from the final image: %v", point)
						return
					}
					for name, read := range map[string]func() ([]*Edge, error){
						"out-old":     func() ([]*Edge, error) { return b.GetOutgoingEdges("test:a") },
						"out-new":     func() ([]*Edge, error) { return b.GetOutgoingEdges("test:b") },
						"in-old":      func() ([]*Edge, error) { return b.GetIncomingEdges("test:b") },
						"in-new":      func() ([]*Edge, error) { return b.GetIncomingEdges("test:middle") },
						"between-old": func() ([]*Edge, error) { return b.GetEdgesBetween("test:a", "test:b") },
						"between-new": func() ([]*Edge, error) { return b.GetEdgesBetween("test:b", "test:middle") },
						"type-old":    func() ([]*Edge, error) { return b.GetEdgesByType("T") },
						"type-new":    func() ([]*Edge, error) { return b.GetEdgesByType("FINAL") },
						"all":         b.AllEdges,
					} {
						found, readErr := read()
						if readErr != nil {
							observationErr = fmt.Errorf("%s: %w", name, readErr)
							return
						}
						want := 0
						newSide := name == "out-new" || name == "in-new" || name == "between-new" || name == "type-new"
						if !deleted && (name == "all" || newSide == (ending == "move-type")) {
							want = 1
						}
						if name == "all" && route == "large" {
							// The retained large-load helper creates a chain as well
							// as nodes. Those unrelated relationships remain visible.
							want += largePopulation - 1
						}
						if len(found) != want {
							observationErr = fmt.Errorf("%s returned %d rows, want %d", name, len(found), want)
							return
						}
						for _, image := range found {
							if image.ID != edge.ID {
								continue
							}
							if deleted || !reflect.DeepEqual(image.Properties, point.Properties) || image.Type != point.Type || image.StartNode != point.StartNode || image.EndNode != point.EndNode {
								observationErr = fmt.Errorf("%s returned an intermediate image: %v", name, image)
								return
							}
						}
					}
				}))
				if route == "prepared" || route == "wal" {
					prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "final-" + route + "-" + ending})
					require.NoError(t, err)
					t.Cleanup(prepared.Abort)
					var owner EntityCommitSubmitter = b
					if route == "wal" {
						t.Cleanup(config.WithWALEnabled())
						dir := t.TempDir()
						wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
						require.NoError(t, err)
						t.Cleanup(func() { _ = wal.Close() })
						owner = NewWALEngine(b, wal)
					}
					receipt, err := owner.SubmitEntityCommit(context.Background(), prepared, nil)
					require.NoError(t, err)
					require.NoError(t, receipt.Wait(context.Background()))
				} else {
					require.NoError(t, tx.Commit())
				}
				require.NoError(t, b.FlushWriteBehind())
				require.True(t, observed)
				require.NoError(t, observationErr)
				point, err := b.GetStoredEdge("test:e")
				if deleted {
					require.ErrorIs(t, err, ErrNotFound)
					return
				}
				require.NoError(t, err)
				require.Equal(t, int64(2), point.Properties["p"])
			})
		}
	}
}

func TestResidualUnrelatedCachesRemainWarm(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			for _, id := range []NodeID{"test:keep-a", "test:keep-b"} {
				_, err := b.CreateNode(&Node{ID: id, Properties: map[string]any{"v": int64(1)}})
				require.NoError(t, err)
			}
			require.NoError(t, b.CreateEdge(&Edge{ID: "test:keep-e", StartNode: "test:keep-a", EndNode: "test:keep-b", Type: "KEEP"}))
			_, err = b.GetStoredNode("test:keep-a")
			require.NoError(t, err)
			_, err = b.GetOutgoingEdges("test:keep-a")
			require.NoError(t, err)
			_, err = b.GetEdgesByType("KEEP")
			require.NoError(t, err)
			node, edge := b.nodeCache["test:keep-a"], b.edgeCache["test:keep-e"]
			var globalIssuer atomic.Pointer[string]
			t.Cleanup(setDurableCacheHook(func(ts uint64) {
				b.durableCaches.mu.Lock()
				global := b.durableCaches.all == ts
				b.durableCaches.mu.Unlock()
				if global {
					stack := string(debug.Stack())
					globalIssuer.Store(&stack)
				}
			}))
			types := reflect.ValueOf(b.edgeTypeCache["KEEP"]).Pointer()
			adjacency := reflect.ValueOf(b.outgoingAdjCache["test:keep-a"]).Pointer()
			if buffered {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				_, err = tx.CreateNode(&Node{ID: "other:new", Labels: []string{"OTHER"}, Properties: map[string]any{"fresh-property": int64(1)}})
				require.NoError(t, err)
				for key, value := range tx.pendingWrites {
					var projection physicalCacheProjection
					projection.add(b, []byte(key), value)
					if projection.unknown {
						t.Logf("raw projection could not classify staged key %x", []byte(key))
					}
				}
				require.NoError(t, tx.Commit())
				require.NoError(t, b.FlushWriteBehind())
			} else {
				_, err := b.CreateNode(&Node{ID: "other:new", Labels: []string{"OTHER"}, Properties: map[string]any{"fresh-property": int64(1)}})
				require.NoError(t, err)
			}
			if stack := globalIssuer.Load(); stack != nil {
				t.Logf("global cache issuer: %s", *stack)
			}
			require.Same(t, node, b.nodeCache["test:keep-a"])
			require.Same(t, edge, b.edgeCache["test:keep-e"])
			require.Equal(t, types, reflect.ValueOf(b.edgeTypeCache["KEEP"]).Pointer())
			require.Equal(t, adjacency, reflect.ValueOf(b.outgoingAdjCache["test:keep-a"]).Pointer())
		})
	}
}
