package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// openBackfillTestEngine opens the store in dir, with or without the
// write-behind buffer.
func openBackfillTestEngine(t *testing.T, dir string, writeBehind bool) *BadgerEngine {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: writeBehind, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

// implicitWrite runs write in an autocommit transaction of namespace test,
// the path a query's write takes: with write-behind on it is acknowledged
// buffered, and applied later. It reports whether it was.
func implicitWrite(b *BadgerEngine, write func(tx *BadgerTransaction) error) (buffered bool, err error) {
	tx, err := b.BeginTransaction()
	if err != nil {
		return false, err
	}
	if err := tx.SetNamespace("test"); err != nil {
		return false, err
	}
	if err := tx.SetImplicit(true); err != nil {
		return false, err
	}
	if err := write(tx); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return tx.AcknowledgedBuffered(), nil
}

// indexKeys returns the stored keys and values of the given key spaces.
func indexKeys(t *testing.T, b *BadgerEngine, prefixes ...byte) map[string]string {
	t.Helper()
	keys := map[string]string{}
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		for _, prefix := range prefixes {
			it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefix}))
			for it.Rewind(); it.ValidForPrefix([]byte{prefix}); it.Next() {
				value, err := it.Item().ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}
				keys[string(it.Item().KeyCopy(nil))] = string(value)
			}
			it.Close()
		}
		return nil
	}))
	return keys
}

// requireIndexMatchesStorage checks an index against the one a rebuild from
// the stored records writes, with nothing running beside it.
func requireIndexMatchesStorage(t *testing.T, b *BadgerEngine, rebuild func(context.Context) (int, error), prefixes ...byte) {
	t.Helper()
	published := indexKeys(t, b, prefixes...)
	_, err := rebuild(context.Background())
	require.NoError(t, err)
	require.Equal(t, indexKeys(t, b, prefixes...), published, "the index published ready disagrees with the stored records")
}

// labelled reports whether a label lookup finds the node.
func labelled(t *testing.T, b *BadgerEngine, label string, id NodeID) bool {
	t.Helper()
	nodes, err := b.GetNodesByLabel(label)
	require.NoError(t, err)
	for _, node := range nodes {
		if node.ID == id {
			return true
		}
	}
	return false
}

// TestIndexBackfillKeepsWritesLandingMidChunk: the startup backfills of the
// label and edge-between indexes run while the engine serves writes. A write
// lands inside a backfill chunk, after the chunk read its records and before
// it commits, through an autocommit transaction: applied there (write-behind
// off, or its buffer flushed there) or applied after the chunk commits
// (write-behind on). Every mutation kind
// keeps the index right, and the index published ready is the one the stored
// records give.
func TestIndexBackfillKeepsWritesLandingMidChunk(t *testing.T) {
	type mutation struct {
		name  string
		write func(tx *BadgerTransaction) error
		check func(t *testing.T, b *BadgerEngine)
	}
	labelMutations := []mutation{
		{"create", func(tx *BadgerTransaction) error {
			_, err := tx.CreateNode(&Node{ID: "test:n", Labels: []string{"Doc"}})
			return err
		}, func(t *testing.T, b *BadgerEngine) {
			require.True(t, labelled(t, b, "Doc", "test:n"), "a node created mid-backfill isn't found by its label")
		}},
		{"update", func(tx *BadgerTransaction) error {
			return tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:a", Labels: []string{"Topic"}}, Policy: ClientNodeEdits})
		}, func(t *testing.T, b *BadgerEngine) {
			require.False(t, labelled(t, b, "Doc", "test:a"), "a label removed mid-backfill keeps its entry")
			require.True(t, labelled(t, b, "Topic", "test:a"), "a label added mid-backfill has no entry")
		}},
		{"delete", func(tx *BadgerTransaction) error {
			return tx.DeleteGraphNode("test:d", CascadeEdges)
		}, func(t *testing.T, b *BadgerEngine) {
			require.False(t, labelled(t, b, "Doc", "test:d"), "a node deleted mid-backfill keeps its label entry")
		}},
	}
	betweenMutations := []mutation{
		{"create", func(tx *BadgerTransaction) error {
			return tx.CreateEdge(&Edge{ID: "test:ca", StartNode: "test:c", EndNode: "test:a", Type: "LINK"})
		}, func(t *testing.T, b *BadgerEngine) {
			edges, err := b.GetEdgesBetween("test:c", "test:a")
			require.NoError(t, err)
			require.Len(t, edges, 1)
		}},
		{"delete", func(tx *BadgerTransaction) error {
			return tx.DeleteEdge("test:ab")
		}, func(t *testing.T, b *BadgerEngine) {
			edges, err := b.GetEdgesBetween("test:a", "test:b")
			require.NoError(t, err)
			require.Empty(t, edges)
		}},
	}
	indexes := []struct {
		name      string
		marker    []byte
		ready     func(*BadgerEngine) (bool, error)
		rebuild   func(*BadgerEngine) func(context.Context) (int, error)
		prefixes  []byte
		mutations []mutation
	}{
		{"label", labelIndexReadyKey, func(b *BadgerEngine) (bool, error) { return b.indexBackfillReady(startupLabelIndex) },
			func(b *BadgerEngine) func(context.Context) (int, error) { return b.rebuildLabelIndex },
			[]byte{prefixLabelIndex}, labelMutations},
		{"between", edgeBetweenIndexReadyKey, func(b *BadgerEngine) (bool, error) { return b.indexBackfillReady(startupEdgeBetweenIndex) },
			func(b *BadgerEngine) func(context.Context) (int, error) {
				return func(ctx context.Context) (int, error) {
					return b.rebuildRelationshipIndexes(ctx, rebuildEdgeBetweenOnly)
				}
			},
			[]byte{prefixEdgeBetweenIndex, prefixEdgeBetweenHead}, betweenMutations},
	}
	modes := []struct {
		name        string
		writeBehind bool
		flushInside bool
	}{
		{"write-behind off", false, false},
		{"write-behind on, applied inside the chunk", true, true},
		{"write-behind on, applied after the chunk", true, false},
	}
	for _, index := range indexes {
		for _, m := range index.mutations {
			for _, mode := range modes {
				t.Run(fmt.Sprintf("%s/%s/%s", index.name, m.name, mode.name), func(t *testing.T) {
					dir := t.TempDir()
					seed := openBackfillTestEngine(t, dir, false)
					for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d"} {
						_, err := seed.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
						require.NoError(t, err)
					}
					require.NoError(t, seed.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
					require.NoError(t, seed.CreateEdge(&Edge{ID: "test:bc", StartNode: "test:b", EndNode: "test:c", Type: "LINK"}))
					require.NoError(t, seed.withUpdate(updateRequest{}, func(txn *badger.Txn) error { return txn.Delete(index.marker) }))
					require.NoError(t, seed.Close())

					paused, resume := make(chan struct{}), make(chan struct{})
					var once sync.Once
					restore := setMaintenancePauseHook(func(point string) {
						if point == pauseIndexBackfillChunk {
							once.Do(func() {
								close(paused)
								<-resume
							})
						}
					})
					defer restore()
					b := openBackfillTestEngine(t, dir, mode.writeBehind)
					select {
					case <-paused:
					case <-time.After(30 * time.Second):
						t.Fatal("the backfill never reached a chunk")
					}
					buffered, err := implicitWrite(b, m.write)
					require.NoError(t, err)
					require.Equal(t, mode.writeBehind, buffered, "the write is acknowledged buffered exactly when write-behind is on")
					if mode.flushInside {
						require.NoError(t, b.FlushWriteBehind(nil))
					}
					close(resume)
					require.Eventually(t, func() bool {
						ready, err := index.ready(b)
						return err == nil && ready
					}, 30*time.Second, 10*time.Millisecond, "the backfill never published the index ready")
					restore()
					if mode.writeBehind {
						require.NoError(t, b.FlushWriteBehind(nil))
					}
					m.check(t, b)
					requireIndexMatchesStorage(t, b, index.rebuild(b), index.prefixes...)
				})
			}
		}
	}
}
