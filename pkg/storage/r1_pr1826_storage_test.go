package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826. Test-only file; compiles on the PR head and on
// its base.

// The embedding flag a projection reads must not depend on which image of the
// node the caches hold.
func TestR1PR1826HasEmbeddingDoesNotDependOnCacheState(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := t.Context()
	id := NodeID("test:inline-vectors")
	_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"key": "one"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
	require.NoError(t, err)
	projected := func() bool {
		node, err := b.GetNodeProjected(ctx, id, []string{"key", hasEmbeddingProperty})
		require.NoError(t, err)
		return node.HasChunkEmbeddings()
	}
	query, err := b.GetQueryNode(ctx, id)
	require.NoError(t, err)
	want := query.HasChunkEmbeddings()
	t.Logf("query read: HasChunkEmbeddings=%t meta=%v", want, query.EmbedMeta)
	require.Equal(t, want, projected(), "right after the write")
	b.cacheDeleteNode(id, nodeCacheAllForms)
	require.Equal(t, want, projected(), "with an empty cache")
	_, err = b.GetNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, want, projected(), "after a whole-node read")
}

// A dropped and recreated database must not serve the dropped image.
func TestR1PR1826DroppedNamespaceServesNoCachedImage(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := t.Context()
	id := NodeID("tenant:x")
	create := func(value string) {
		_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"key": value, "body": value + " body"}})
		require.NoError(t, err)
	}
	read := func() string {
		node, err := b.GetNodeProjected(ctx, id, []string{"key"})
		require.NoError(t, err)
		return node.Properties["key"].(string)
	}
	create("old")
	b.cacheDeleteNode(id, nodeCacheAllForms)
	require.Equal(t, "old", read())
	require.Equal(t, "old", read())
	_, _, err := b.DeleteByPrefix("tenant:")
	require.NoError(t, err)
	_, err = b.GetNodeProjected(ctx, id, []string{"key"})
	require.ErrorIs(t, err, ErrNotFound)
	create("new")
	require.Equal(t, "new", read())
	b.cacheDeleteNode(id, nodeCacheAllForms)
	require.Equal(t, "new", read())
	require.Equal(t, "new", read())
}

// A rejected direct write must leave nothing of itself in the read cache.
func TestR1PR1826RejectedWriteLeavesNoImage(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := t.Context()
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "r1_mail", Type: ConstraintUnique, EntityType: ConstraintEntityNode, Label: "Person", Properties: []string{"mail"}}))
	for _, name := range []string{"y", "x"} {
		_, err := b.CreateNode(ctx, &Node{ID: NodeID("test:" + name), Labels: []string{"Person"}, Properties: map[string]any{"mail": name, "note": "committed"}})
		require.NoError(t, err)
	}
	read := func(id NodeID) map[string]any {
		node, err := b.GetNodeProjected(ctx, id, []string{"mail", "note"})
		require.NoError(t, err)
		return node.Properties
	}
	b.cacheDeleteNode("test:x", nodeCacheAllForms)
	require.Equal(t, map[string]any{"mail": "x", "note": "committed"}, read("test:x"))
	err := b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:x", Labels: []string{"Person"}, Properties: map[string]any{"mail": "y", "note": "rejected"}}})
	require.Error(t, err)
	require.Equal(t, map[string]any{"mail": "x", "note": "committed"}, read("test:x"))
	err = b.BulkCreateNodes(ctx, []*Node{
		{ID: "test:b1", Labels: []string{"Person"}, Properties: map[string]any{"mail": "dup", "note": "rejected"}},
		{ID: "test:b2", Labels: []string{"Person"}, Properties: map[string]any{"mail": "dup", "note": "rejected"}},
	})
	require.Error(t, err)
	_, err = b.GetNodeProjected(ctx, "test:b1", []string{"mail", "note"})
	require.ErrorIs(t, err, ErrNotFound)
	_, err = b.CreateNode(ctx, &Node{ID: "test:b1", Labels: []string{"Person"}, Properties: map[string]any{"mail": "b1", "note": "committed"}})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"mail": "b1", "note": "committed"}, read("test:b1"))
}

// Readers of every projected route against one writer: a read that starts after
// a write completed must see that write or a later one, a pinned transaction
// must keep reading the version it started on, and every image must belong to
// one version.
func TestR1PR1826ProjectedReadsAgainstWriter(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeBehind=%t", writeBehind), func(t *testing.T) {
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: writeBehind}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			ctx := t.Context()
			const nodes = 4
			id := func(i int) NodeID { return NodeID(fmt.Sprintf("test:stress-%d", i)) }
			properties := func(n int64) map[string]any {
				return map[string]any{"seq": n, "a": fmt.Sprint("a", n), "b": fmt.Sprint("b", n), "list": []int64{n, n}, "big": strings.Repeat(fmt.Sprint(n%10), 4096)}
			}
			for i := 0; i < nodes; i++ {
				_, err := b.CreateNode(ctx, &Node{ID: id(i), Labels: []string{"Doc"}, Properties: properties(0)})
				require.NoError(t, err)
			}
			var completed [nodes]atomic.Int64
			stop := make(chan struct{})
			failures := make(chan error, 64)
			fail := func(format string, args ...any) {
				select {
				case failures <- fmt.Errorf(format, args...):
				default:
				}
			}
			verify := func(node *Node, floor int64, what string) (int64, bool) {
				seq, ok := node.Properties["seq"].(int64)
				if !ok {
					fail("%s: no seq in %v", what, node.Properties)
					return 0, false
				}
				if seq < floor {
					fail("%s: read seq %d after write %d completed", what, seq, floor)
					return seq, false
				}
				for name, value := range node.Properties {
					want := properties(seq)[name]
					if fmt.Sprint(value) != fmt.Sprint(want) {
						fail("%s: seq %d carries %s of another version", what, seq, name)
						return seq, false
					}
					switch typed := value.(type) {
					case []int64:
						for i := range typed {
							typed[i] = -1 // the result belongs to the caller
						}
					}
				}
				node.Properties["a"] = "caller changed it"
				return seq, true
			}
			fieldSets := [][]string{{"seq"}, {"seq", "a"}, {"seq", "b", "list"}, {"seq", "big", "absent"}, {"seq", "a", "b", "list", "big", "missing"}}
			var workers sync.WaitGroup
			for worker := 0; worker < 6; worker++ {
				workers.Go(func() {
					for n := 0; ; n++ {
						select {
						case <-stop:
							return
						default:
						}
						i := (n + worker) % nodes
						floor := completed[i].Load()
						node, err := b.GetNodeProjected(ctx, id(i), fieldSets[(n*7+worker)%len(fieldSets)])
						if err != nil {
							fail("point: %w", err)
							return
						}
						if _, ok := verify(node, floor, "point"); !ok {
							return
						}
					}
				})
			}
			workers.Go(func() {
				for n := 0; ; n++ {
					select {
					case <-stop:
						return
					default:
					}
					var floors [nodes]int64
					for i := range floors {
						floors[i] = completed[i].Load()
					}
					err := b.StreamNodesByLabelProjected(ctx, "Doc", fieldSets[n%len(fieldSets)], func(node *Node) error {
						var i int
						if _, err := fmt.Sscanf(string(node.ID), "test:stress-%d", &i); err != nil {
							return err
						}
						verify(node, floors[i], "label scan")
						return nil
					})
					if err != nil {
						fail("label scan: %w", err)
						return
					}
					if n%5 == 0 {
						if _, err := b.GetNode(ctx, id(n%nodes)); err != nil {
							fail("whole read: %w", err)
							return
						}
					}
				}
			})
			if !writeBehind {
				workers.Go(func() {
					for n := 0; ; n++ {
						select {
						case <-stop:
							return
						default:
						}
						tx, err := b.BeginTransaction(ctx, OrdinaryBegin)
						if err != nil {
							fail("begin: %w", err)
							return
						}
						pinned := int64(-1)
						for k := 0; k < 12; k++ {
							node, err := tx.GetNodeProjected(ctx, id(n%nodes), fieldSets[(n+k)%len(fieldSets)])
							if err != nil {
								fail("pinned read: %w", err)
								break
							}
							seq, ok := verify(node, 0, "pinned")
							if !ok {
								break
							}
							if pinned >= 0 && seq != pinned {
								fail("pinned transaction moved from seq %d to %d", pinned, seq)
								break
							}
							pinned = seq
						}
						_ = tx.Rollback()
					}
				})
			}
			deadline := time.Now().Add(4 * time.Second)
			for n := int64(1); time.Now().Before(deadline); n++ {
				i := int(n) % nodes
				require.NoError(t, b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: id(i), Labels: []string{"Doc"}, Properties: properties(n)}}))
				completed[i].Store(n)
				node, err := b.GetNodeProjected(ctx, id(i), []string{"seq", "a"})
				require.NoError(t, err)
				require.Equal(t, n, node.Properties["seq"], "the writer reads its own completed write")
			}
			close(stop)
			workers.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
		})
	}
}

// What a reader that keeps naming properties a node does not have costs: the
// size of the node's cache entry, the time of one more read, and the time of
// an unrelated write meanwhile.
func TestR1PR1826AbsentNamesCost(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := context.Background()
	id := NodeID("test:absent-names")
	other := NodeID("test:other")
	for _, node := range []NodeID{id, other} {
		_, err := b.CreateNode(ctx, &Node{ID: node, Labels: []string{"Doc"}, Properties: map[string]any{"key": "one", "seq": int64(0)}})
		require.NoError(t, err)
	}
	b.cacheDeleteNode(id, nodeCacheAllForms)
	const perRead = 2000
	next := 0
	names := func() []string {
		out := make([]string, perRead)
		for i := range out {
			out[i] = fmt.Sprintf("absent_property_name_%09d", next)
			next++
		}
		return out
	}
	read := func(fields []string) time.Duration {
		start := time.Now()
		_, err := b.GetNodeProjected(ctx, id, fields)
		require.NoError(t, err)
		return time.Since(start)
	}
	writes := func(count int) (p50, worst time.Duration) {
		var took []time.Duration
		for i := 0; i < count; i++ {
			start := time.Now()
			require.NoError(t, b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: other, Labels: []string{"Doc"}, Properties: map[string]any{"key": "two", "seq": int64(i)}}}))
			took = append(took, time.Since(start))
		}
		sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
		return took[len(took)/2], took[len(took)-1]
	}
	for _, reads := range []int{1, 100, 200, 300} {
		for next < reads*perRead {
			read(names())
		}
		var last time.Duration
		for i := 0; i < 5; i++ {
			if took := read(names()); took > last {
				last = took
			}
		}
		b.nodeBodyCacheMu.RLock()
		bytes := b.nodeBodyCacheBytes
		b.nodeBodyCacheMu.RUnlock()
		stop := make(chan struct{})
		var reader sync.WaitGroup
		reader.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					read(names())
				}
			}
		})
		p50, worst := writes(100)
		close(stop)
		reader.Wait()
		idle50, idleWorst := writes(100)
		t.Logf("R1COST names=%d cacheBytes=%d slowestRead=%s unrelated write while reading p50=%s max=%s, idle p50=%s max=%s",
			next, bytes, last, p50, worst, idle50, idleWorst)
	}
}
