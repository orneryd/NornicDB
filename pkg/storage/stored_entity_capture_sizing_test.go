package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// storedEntityCaptureFixture stages one node update and keeps one unchanged
// edge, so a capture resolves a distinct before and after image for the node and
// the same stored image on both cuts for the edge. The commit's publication keys
// interleave those two entity keys with the AffectedFact records that dependency
// owners consume; this capture reads none of the facts.
type storedEntityCaptureFixture struct {
	engine     *BadgerEngine
	before     storedEntityView
	after      storedEntityView
	entityKeys []any
	updated    *Node
	storedEdge *Edge
}

func newStoredEntityCaptureFixture(t testing.TB) storedEntityCaptureFixture {
	t.Helper()
	engine := newReadSnapshotTestEngine(t, false)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"Doc", "Other"}, Properties: map[string]any{"v": "old"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:b"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), testEdge("test:r", "test:a", "test:b", "R")))
	stored, err := engine.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	storedEdge, err := engine.GetEdge(t.Context(), "test:r")
	require.NoError(t, err)

	// Both cuts belong to one commit: the prior reader is pinned at the write
	// transaction's read timestamp, and the after reader sees that transaction's
	// own staged node image.
	staged, readTs, err := engine.db.beginTxn(true)
	require.NoError(t, err)
	prior := engine.db.newTxnAt(readTs, false)
	t.Cleanup(func() {
		staged.Discard()
		prior.Discard()
		engine.db.endRead(readTs)
	})
	updated := CopyNode(stored)
	updated.Properties["v"] = "new"
	data, _, err := engine.encodeNode(staged, updated)
	require.NoError(t, err)
	require.NoError(t, staged.Set(nodeKey(updated.ID), data))

	return storedEntityCaptureFixture{
		engine:     engine,
		before:     storedEntityView{read: embeddingRecordReader{txn: prior}},
		after:      storedEntityView{read: embeddingRecordReader{txn: staged}},
		entityKeys: []any{updated.ID, storedEdge.ID},
		updated:    updated,
		storedEdge: storedEdge,
	}
}

func (f storedEntityCaptureFixture) capture(ctx context.Context, keys []any, before storedEntityView) ([]StoredEntityChange, error) {
	return f.engine.captureStoredEntityChanges(ctx, storedEntityCapture{keys: keys, before: before, after: f.after})
}

// factInflatedEntityKeys reproduces the publication key list
// appendOperationPublicationKeys builds for the fixture's commit: the entity keys
// in commit order, interleaved with one AffectedFact per label, relationship
// endpoint and constraint dependency. Those facts are not stored entities, so
// they must not reach the captured change list or its capacity.
func factInflatedEntityKeys(facts int) []any {
	keys := make([]any, 0, facts+2)
	keys = append(keys, NodeID("test:a"))
	for i := 0; i < facts; i++ {
		keys = append(keys, AffectedFact{NodeID: "test:a", Label: fmt.Sprintf("scope-%d", i), Before: i%2 == 0})
	}
	keys = append(keys, EdgeID("test:r"))
	return keys
}

func TestStoredEntityCaptureSizesToEntityKeysOnly(t *testing.T) {
	fixture := newStoredEntityCaptureFixture(t)
	plain, err := fixture.capture(t.Context(), fixture.entityKeys, fixture.before)
	require.NoError(t, err)
	require.Len(t, plain, 2, "one change per touched stored entity")
	require.Equal(t, len(fixture.entityKeys), cap(plain), "the change list is sized from the entity keys")

	t.Run("stored images and order", func(t *testing.T) {
		require.NotNil(t, plain[0].BeforeNode)
		require.NotNil(t, plain[0].AfterNode)
		require.Equal(t, NodeID("test:a"), plain[0].BeforeNode.ID)
		require.Equal(t, "old", plain[0].BeforeNode.Properties["v"])
		require.Equal(t, "new", plain[0].AfterNode.Properties["v"])
		assertStoredNodeImage(t, fixture.updated, plain[0].AfterNode)
		require.Nil(t, plain[0].BeforeEdge)
		require.Nil(t, plain[0].AfterEdge)

		require.NotNil(t, plain[1].BeforeEdge)
		require.NotNil(t, plain[1].AfterEdge)
		require.Equal(t, EdgeID("test:r"), plain[1].BeforeEdge.ID)
		require.Equal(t, fixture.storedEdge, plain[1].BeforeEdge)
		require.Equal(t, fixture.storedEdge, plain[1].AfterEdge)
		require.Nil(t, plain[1].BeforeNode)
		require.Nil(t, plain[1].AfterNode)
	})

	t.Run("non-entity fact volume does not inflate capacity", func(t *testing.T) {
		// 514 publication keys name only two stored entities. Sizing from len(keys)
		// reserved a 514-entry seen set and change list; the capture must not grow
		// with the facts, and the images must stay exactly the same delta.
		inflated, err := fixture.capture(t.Context(), factInflatedEntityKeys(512), fixture.before)
		require.NoError(t, err)
		require.Equal(t, plain, inflated, "facts cannot change the captured images or their order")
		require.Equal(t, cap(plain), cap(inflated), "facts must not size the entity change list")
	})

	t.Run("duplicate entity keys keep first-occurrence order", func(t *testing.T) {
		keys := []any{
			NodeID("test:a"),
			AffectedFact{NodeID: "test:a", Label: "Doc"},
			NodeID("test:a"),
			EdgeID("test:r"),
			AffectedFact{EdgeID: "test:r", Type: "R"},
		}
		changes, err := fixture.capture(t.Context(), keys, fixture.before)
		require.NoError(t, err)
		require.Equal(t, plain, changes, "a repeated entity key is read and emitted once")
		require.LessOrEqual(t, cap(changes), 3, "only the three entity keys present may reserve capacity")
	})

	t.Run("facts alone capture no entity change", func(t *testing.T) {
		changes, err := fixture.capture(t.Context(), []any{
			AffectedFact{NodeID: "test:a", Label: "Doc"},
			AffectedFact{EdgeID: "test:r", Type: "R"},
		}, fixture.before)
		require.NoError(t, err)
		require.Empty(t, changes)
		require.Zero(t, cap(changes), "no entity key reserves no change capacity")
	})

	t.Run("canceled context keeps its error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		changes, err := fixture.capture(ctx, factInflatedEntityKeys(512), fixture.before)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, changes)
	})

	t.Run("read boundary failure is unchanged", func(t *testing.T) {
		injected := errors.New("snapshot-bound read failed")
		var reads []StoredEntityReadKind
		registry := storedEntityObserverRegistry{"test": {boundary: storedReadTestBoundary{read: func(kind StoredEntityReadKind, id string, read func() error) error {
			reads = append(reads, kind)
			if kind == StoredEntityNodePriorRead {
				return injected
			}
			return read()
		}}}}
		before := storedEntityView{read: fixture.before.read, boundaries: &registry}
		changes, err := fixture.capture(t.Context(), factInflatedEntityKeys(512), before)
		require.ErrorIs(t, err, injected)
		require.Nil(t, changes)
		require.Equal(t, []StoredEntityReadKind{StoredEntityNodePriorRead}, reads, "facts never reach the read boundary")
	})
}

// BenchmarkStoredEntityCaptureFactVolume measures one commit's capture while its
// publication key list carries 0, 64 or 512 non-entity AffectedFact records the
// capture never reads. The allocation columns are the measurement: entity-sized
// preallocation keeps them flat as fact volume grows.
func BenchmarkStoredEntityCaptureFactVolume(b *testing.B) {
	fixture := newStoredEntityCaptureFixture(b)
	for _, facts := range []int{0, 64, 512} {
		keys := factInflatedEntityKeys(facts)
		b.Run(fmt.Sprintf("facts=%d", facts), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				changes, err := fixture.capture(b.Context(), keys, fixture.before)
				if err != nil {
					b.Fatal(err)
				}
				if len(changes) != 2 {
					b.Fatalf("captured %d changes, want 2", len(changes))
				}
			}
		})
	}
}
