package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompositeIndexReplaceNodeSingleProperty(t *testing.T) {
	idx := &CompositeIndex{Label: "Doc", Properties: []string{"id"}}
	require.NoError(t, idx.ReplaceNode(t.Context(), "a", nil, map[string]interface{}{"id": "k"}))
	require.NoError(t, idx.ReplaceNode(t.Context(), "b", nil, map[string]interface{}{"id": "k"}))
	require.Equal(t, []NodeID{"a", "b"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "k")))

	// An unchanged key keeps its entry in place: no removal, no re-sort.
	idx.keysDirty = false
	require.NoError(t, idx.ReplaceNode(t.Context(), "a", map[string]interface{}{"id": "k", "c": 1}, map[string]interface{}{"id": "k", "c": 2}))
	require.Equal(t, []NodeID{"a", "b"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "k")))
	require.False(t, idx.keysDirty)

	// A changed key moves the node.
	require.NoError(t, idx.ReplaceNode(t.Context(), "a", map[string]interface{}{"id": "k"}, map[string]interface{}{"id": "m"}))
	require.Equal(t, []NodeID{"b"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "k")))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "m")))

	// Losing the key, or the index's label (nil new properties), removes it;
	// a key-less value files nothing.
	require.NoError(t, idx.ReplaceNode(t.Context(), "a", map[string]interface{}{"id": "m"}, map[string]interface{}{"other": 1}))
	require.NoError(t, idx.ReplaceNode(t.Context(), "b", map[string]interface{}{"id": "k"}, nil))
	require.NoError(t, idx.ReplaceNode(t.Context(), "c", nil, map[string]interface{}{"id": nil}))
	require.Empty(t, idx.values)
	require.True(t, idx.keysDirty)

	// A node missing from the index is filed by an update that keeps its key.
	require.NoError(t, idx.ReplaceNode(t.Context(), "d", map[string]interface{}{"id": "k"}, map[string]interface{}{"id": "k"}))
	require.Equal(t, []NodeID{"d"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "k")))
	idx.RemoveNode("absent", map[string]interface{}{"id": "zzz"})
	require.Equal(t, []NodeID{"d"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "k")))
}

func TestCompositeIndexReplaceNodeComposite(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddCompositeIndex("loc", "Place", []string{"country", "city"}))
	idx, ok := sm.GetCompositeIndex("loc")
	require.True(t, ok)
	full := map[string]interface{}{"country": "US", "city": "NYC"}
	require.NoError(t, idx.IndexNode(t.Context(), "a", full))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "US", "NYC")))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupPrefix(t.Context(), "US")))

	// Unchanged values keep both entries.
	require.NoError(t, idx.ReplaceNode(t.Context(), "a", full, map[string]interface{}{"country": "US", "city": "NYC", "pop": 8}))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "US", "NYC")))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupPrefix(t.Context(), "US")))

	// Changing the trailing value keeps the shared prefix and moves the full key.
	require.NoError(t, idx.ReplaceNode(t.Context(), "a", full, map[string]interface{}{"country": "US", "city": "LA"}))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "US", "NYC")))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "US", "LA")))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupPrefix(t.Context(), "US")))

	// Dropping the trailing property leaves only the prefix entry; restoring
	// it files the full key again.
	require.NoError(t, idx.ReplaceNode(t.Context(), "a", map[string]interface{}{"country": "US", "city": "LA"}, map[string]interface{}{"country": "US"}))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "US", "LA")))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupPrefix(t.Context(), "US")))
	require.NoError(t, idx.ReplaceNode(t.Context(), "a", map[string]interface{}{"country": "US"}, full))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "US", "NYC")))

	// Changing the leading value moves every entry; removal empties the index.
	require.NoError(t, idx.ReplaceNode(t.Context(), "a", full, map[string]interface{}{"country": "CA", "city": "NYC"}))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(idx.LookupPrefix(t.Context(), "US")))
	require.Equal(t, []NodeID{"a"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), "CA", "NYC")))
	idx.RemoveNode("a", map[string]interface{}{"country": "CA", "city": "NYC"})
	require.Empty(t, idx.fullIndex)
	require.Empty(t, idx.prefixIndex)
}

// TestCompositeIndexReplaceNodeNeverHidesUnchangedKey races seeks against
// updates that keep the key: every seek must see the node.
func TestCompositeIndexReplaceNodeNeverHidesUnchangedKey(t *testing.T) {
	single := &CompositeIndex{Label: "Doc", Properties: []string{"id"}}
	pair := &CompositeIndex{Label: "Doc", Properties: []string{"tenant", "id"}, fullIndex: map[string][]NodeID{}, prefixIndex: map[string][]NodeID{}}
	props := map[string]interface{}{"tenant": "t", "id": "k"}
	require.NoError(t, single.ReplaceNode(t.Context(), "n", nil, props))
	require.NoError(t, pair.ReplaceNode(t.Context(), "n", nil, props))
	// A seek reports its read error on the test and yields no node, which is
	// counted as a miss like an empty result.
	seekFull := func(idx *CompositeIndex, values ...interface{}) []NodeID {
		return checkedIndexRead[[]NodeID](t)(idx.LookupFull(context.Background(), values...))
	}
	seekPrefix := func(idx *CompositeIndex, values ...interface{}) []NodeID {
		return checkedIndexRead[[]NodeID](t)(idx.LookupPrefix(context.Background(), values...))
	}
	var stop atomic.Bool
	var misses atomic.Int64
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if len(seekFull(single, "k")) != 1 || len(seekFull(pair, "t", "k")) != 1 || len(seekPrefix(pair, "t")) != 1 {
					misses.Add(1)
				}
			}
		}()
	}
	for i := 0; i < 20000; i++ {
		next := map[string]interface{}{"tenant": "t", "id": "k", "c": i}
		require.NoError(t, single.ReplaceNode(t.Context(), "n", props, next))
		require.NoError(t, pair.ReplaceNode(t.Context(), "n", props, next))
		props = next
	}
	stop.Store(true)
	wg.Wait()
	require.Zero(t, misses.Load())
}

// cancelOnKeyPreparation is a value whose canonical key preparation cancels the
// write's lifetime. indexValueKey reads a value's temporal key (TemporalTime)
// while preparing that value's key, so reaching it proves the cancellation
// landed inside ReplaceNode's preparation: after admission succeeded and before
// the first posting changed.
type cancelOnKeyPreparation struct {
	cancel  context.CancelFunc
	reached *atomic.Bool
}

func (v cancelOnKeyPreparation) TemporalTime() time.Time {
	v.reached.Store(true)
	v.cancel()
	return time.Unix(0, 0).UTC()
}

// compositeIndexSnapshot copies the populations ReplaceNode promises to leave
// alone when its preparation fails: the values, full and prefix populations, the
// dirty flag and the fill state.
type compositeIndexSnapshot struct {
	values      map[interface{}][]NodeID
	fullIndex   map[string][]NodeID
	prefixIndex map[string][]NodeID
	keysDirty   bool
	unfilled    bool
}

func snapshotCompositeIndex(idx *CompositeIndex) compositeIndexSnapshot {
	snapshot := compositeIndexSnapshot{
		values:      make(map[interface{}][]NodeID, len(idx.values)),
		fullIndex:   make(map[string][]NodeID, len(idx.fullIndex)),
		prefixIndex: make(map[string][]NodeID, len(idx.prefixIndex)),
		keysDirty:   idx.keysDirty,
		unfilled:    idx.unfilled.Load(),
	}
	for value, ids := range idx.values {
		snapshot.values[value] = append([]NodeID(nil), ids...)
	}
	for hash, ids := range idx.fullIndex {
		snapshot.fullIndex[hash] = append([]NodeID(nil), ids...)
	}
	for hash, ids := range idx.prefixIndex {
		snapshot.prefixIndex[hash] = append([]NodeID(nil), ids...)
	}
	return snapshot
}

// withIndexedValue copies a property image and files value under key, so a case
// can cancel the write while preparing exactly one side's keys.
func withIndexedValue(source map[string]interface{}, key string, value interface{}) map[string]interface{} {
	image := make(map[string]interface{}, len(source)+1)
	for name, stored := range source {
		image[name] = stored
	}
	image[key] = value
	return image
}

// TestReplaceNodeCancelledPreparationMovesNothing pins ReplaceNode's ordering:
// both images' keys are prepared before the first posting changes, so a lifetime
// that cancels during preparation moves nothing — not even the entries of the
// side that prepared successfully. A canceled lifetime is likewise refused, and a
// retry on a live lifetime performs the move.
func TestReplaceNodeCancelledPreparationMovesNothing(t *testing.T) {
	type indexedCase struct {
		name       string
		add        func(*SchemaManager) error
		index      string
		key        string
		seed       map[string]interface{}
		live       map[string]interface{}
		seedLookup []interface{}
		liveLookup []interface{}
	}
	cases := []indexedCase{
		{
			name:       "single property",
			add:        func(sm *SchemaManager) error { return sm.AddPropertyIndex("doc_id", "Doc", []string{"id"}) },
			index:      "doc_id",
			key:        "id",
			seed:       map[string]interface{}{"id": "k"},
			live:       map[string]interface{}{"id": "m"},
			seedLookup: []interface{}{"k"},
			liveLookup: []interface{}{"m"},
		},
		{
			name: "composite",
			add: func(sm *SchemaManager) error {
				return sm.AddCompositeIndex("doc_key", "Doc", []string{"tenant", "id"})
			},
			index:      "doc_key",
			key:        "id",
			seed:       map[string]interface{}{"tenant": "t", "id": "k"},
			live:       map[string]interface{}{"tenant": "t", "id": "m"},
			seedLookup: []interface{}{"t", "k"},
			liveLookup: []interface{}{"t", "m"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewSchemaManager()
			require.NoError(t, tc.add(sm))
			idx, ok := sm.GetCompositeIndex(tc.index)
			require.True(t, ok)
			const node NodeID = "moved"
			require.NoError(t, idx.IndexNode(t.Context(), node, tc.seed))
			require.Equal(t, []NodeID{node}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), tc.seedLookup...)))
			// Another node keeps the same key populated, so the whole population
			// stays observable across the cancelled write.
			require.NoError(t, idx.IndexNode(t.Context(), "kept", tc.seed))
			published := snapshotCompositeIndex(idx)

			t.Run("canceled lifetime", func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				err := idx.ReplaceNode(ctx, node, tc.seed, tc.live)
				require.ErrorIs(t, err, context.Canceled, "a canceled lifetime files no replacement")
				require.Equal(t, published, snapshotCompositeIndex(idx), "a canceled lifetime moves nothing")
			})

			for _, side := range []string{"old", "new"} {
				t.Run("cancel preparing the "+side+" image", func(t *testing.T) {
					base, cancel := context.WithCancel(t.Context())
					defer cancel()
					var reached atomic.Bool
					hook := cancelOnKeyPreparation{cancel: cancel, reached: &reached}
					old, live := tc.seed, tc.live
					if side == "old" {
						old = withIndexedValue(tc.seed, tc.key, hook)
					} else {
						live = withIndexedValue(tc.live, tc.key, hook)
					}
					err := idx.ReplaceNode(base, node, old, live)
					require.ErrorIs(t, err, context.Canceled)
					require.True(t, reached.Load(), "the cancellation landed in key preparation, after admission")
					require.Equal(t, published, snapshotCompositeIndex(idx), "preparation is complete before the first posting changes")
				})
			}

			require.NoError(t, idx.ReplaceNode(t.Context(), node, tc.seed, tc.live))
			require.Equal(t, []NodeID{node}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), tc.liveLookup...)))
			require.Equal(t, []NodeID{"kept"}, checkedIndexRead[[]NodeID](t)(idx.LookupFull(t.Context(), tc.seedLookup...)), "a live retry moves only its own node")
		})
	}
}

func TestReplaceNodeIndexEntriesFollowsLabels(t *testing.T) {
	engine := createTestBadgerEngine(t)
	sm := engine.GetSchemaForNamespace("nornic")
	require.NoError(t, sm.AddPropertyIndex("doc_id", "Doc", []string{"id"}))
	require.NoError(t, sm.AddPropertyIndex("arc_id", "Archive", []string{"id"}))
	docs, _ := sm.GetCompositeIndex("doc_id")
	archive, _ := sm.GetCompositeIndex("arc_id")

	const id NodeID = "nornic:replace-follows-labels"

	doc := &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "k"}}
	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpCreateNode, Node: doc}}))
	require.Equal(t, []NodeID{id}, checkedIndexRead[[]NodeID](t)(docs.LookupFull(t.Context(), "k")))

	// Moving the node from Doc to Archive moves its entry between the indexes.
	archived := &Node{ID: id, Labels: []string{"Archive"}, Properties: map[string]interface{}{"id": "k"}}
	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpUpdateNode, OldNode: doc, Node: archived}}))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(docs.LookupFull(t.Context(), "k")))
	require.Equal(t, []NodeID{id}, checkedIndexRead[[]NodeID](t)(archive.LookupFull(t.Context(), "k")))

	engine.publishIndexMembership(commitStatesFor(t, []Operation{{Type: OpDeleteNode, NodeID: id, OldNode: archived}}))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(archive.LookupFull(t.Context(), "k")))
}
