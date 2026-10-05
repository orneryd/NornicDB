package storage

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStreamNodesOptionsParityAcrossStacks runs the same stream battery
// against every production stack: Memory (embedded Badger), Badger, WAL,
// Async, Namespaced, and Composite. Every stack must return identical IDs for
// the legacy full stream and the unified options kernel (all, prefix-scoped,
// projected).
func TestStreamNodesOptionsParityAcrossStacks(t *testing.T) {
	type stackCase struct {
		name   string
		build  func(t *testing.T) Engine
		prefix string // user-level prefix that still matches every seeded node
		count  int    // seeded node count
	}
	stacks := []stackCase{
		{name: "memory", prefix: "db:", count: 3, build: func(t *testing.T) Engine {
			engine := NewMemoryEngine()
			t.Cleanup(func() { _ = engine.Close() })
			return engine
		}},
		{name: "badger", prefix: "db:", count: 3, build: func(t *testing.T) Engine {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			return engine
		}},
		{name: "wal", prefix: "db:", count: 3, build: func(t *testing.T) Engine {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			wal, err := NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			return NewWALEngine(engine, wal)
		}},
		{name: "async", prefix: "db:", count: 3, build: func(t *testing.T) Engine {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			return NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		}},
		{name: "namespaced", prefix: "n-", count: 3, build: func(t *testing.T) Engine {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			return NewNamespacedEngine(engine, "db")
		}},
		{name: "composite", prefix: "", count: 2, build: func(t *testing.T) Engine {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			nsA := NewNamespacedEngine(engine, "a")
			nsB := NewNamespacedEngine(engine, "b")
			return NewCompositeEngine(
				map[string]Engine{"a": nsA, "b": nsB},
				map[string]string{"a": "a", "b": "b"},
				map[string]string{"a": "read_write", "b": "read_write"},
			)
		}},
	}

	for _, stack := range stacks {
		t.Run(stack.name, func(t *testing.T) {
			engine := stack.build(t)
			ctx := context.Background()

			switch e := engine.(type) {
			case *NamespacedEngine:
				_, err := e.CreateNode(&Node{ID: "n-1", Labels: []string{"Doc"}, Properties: map[string]any{"a": int64(1), "b": "two"}})
				require.NoError(t, err)
				_, err = e.CreateNode(&Node{ID: "n-2", Labels: []string{"Doc"}, Properties: map[string]any{"a": int64(2)}})
				require.NoError(t, err)
				_, err = e.CreateNode(&Node{ID: "n-3", Labels: []string{"Other"}, Properties: map[string]any{"a": int64(3)}})
				require.NoError(t, err)
			case *CompositeEngine:
				nsA, err := e.GetConstituentByAlias("a")
				require.NoError(t, err)
				nsB, err := e.GetConstituentByAlias("b")
				require.NoError(t, err)
				_, err = nsA.CreateNode(&Node{ID: "n-1", Labels: []string{"Doc"}, Properties: map[string]any{"a": int64(1)}})
				require.NoError(t, err)
				_, err = nsB.CreateNode(&Node{ID: "n-2", Labels: []string{"Doc"}, Properties: map[string]any{"a": int64(2)}})
				require.NoError(t, err)
			default:
				for index := int64(1); index <= 3; index++ {
					_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("db:n-%d", index)), Labels: []string{"Doc"}, Properties: map[string]any{"a": index}})
					require.NoError(t, err)
				}
			}
			if async, ok := engine.(*AsyncEngine); ok {
				require.NoError(t, async.Flush())
			}

			collect := func(fn func(context.Context, func(*Node) error) error) []NodeID {
				var ids []NodeID
				require.NoError(t, fn(ctx, func(node *Node) error {
					if node != nil {
						ids = append(ids, node.ID)
					}
					return nil
				}))
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
				return ids
			}

			all := collect(func(ctx context.Context, fn func(*Node) error) error {
				return engine.StreamNodesWithOptions(ctx, StreamNodesOptions{}, fn)
			})
			require.Len(t, all, stack.count)

			prefixIDs := collect(func(ctx context.Context, fn func(*Node) error) error {
				return engine.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: stack.prefix}, fn)
			})
			require.Equal(t, all, prefixIDs)

			projected := collect(func(ctx context.Context, fn func(*Node) error) error {
				return engine.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: stack.prefix, Projection: []string{"a"}}, fn)
			})
			require.Equal(t, all, projected)
			if reader, ok := engine.(ProjectedPrefixNodeReader); ok {
				legacyProjected := collect(func(ctx context.Context, fn func(*Node) error) error {
					return reader.StreamNodesByPrefixProjected(ctx, stack.prefix, []string{"a"}, func(node *Node) error {
						require.Contains(t, node.Properties, "a")
						require.Equal(t, map[string]any{"a": node.Properties["a"]}, node.Properties)
						return fn(node)
					})
				})
				require.Equal(t, projected, legacyProjected)
				fullProjected := collect(func(ctx context.Context, fn func(*Node) error) error {
					return reader.StreamNodesByPrefixProjected(ctx, stack.prefix, nil, func(node *Node) error {
						if stack.name == "namespaced" && node.ID == "n-1" {
							require.Equal(t, "two", node.Properties["b"])
						}
						return fn(node)
					})
				})
				require.Equal(t, all, fullProjected)
			}

			// Legacy streaming entry point stays consistent with the kernel.
			legacy := collect(func(ctx context.Context, fn func(*Node) error) error {
				return engine.(StreamingEngine).StreamNodes(ctx, fn)
			})
			require.Equal(t, all, legacy)
		})
	}
}
