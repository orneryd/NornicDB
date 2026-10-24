package storage

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func implicitCommit(t *testing.T, b *BadgerEngine, write func(*BadgerTransaction) error) {
	t.Helper()
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, write(tx))
	require.NoError(t, tx.Commit())
}

// Whether a deletion was published before or after the writer began decides
// its classification, never whether the seed or the tombstone has meanwhile
// been flushed and its buffer generation retired.
func TestVanishedWriteTargetDeletionPublication(t *testing.T) {
	for _, syncWrites := range []bool{false, true} {
		for _, bufferedSeed := range []bool{false, true} {
			for _, beforeBegin := range []bool{false, true} {
				for _, flushDelete := range []bool{false, true} {
					name := fmt.Sprintf("sync=%t/bufferSeed=%t/preBegin=%t/flushDelete=%t", syncWrites, bufferedSeed, beforeBegin, flushDelete)
					t.Run(name, func(t *testing.T) {
						b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, SyncWrites: syncWrites})
						require.NoError(t, err)
						defer b.Close()
						implicitCommit(t, b, func(tx *BadgerTransaction) error {
							for _, id := range []NodeID{"test:a", "test:b"} {
								if _, err := tx.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"id": string(id)}}); err != nil {
									return err
								}
							}
							return tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"w": 1}})
						})
						if !bufferedSeed {
							require.NoError(t, b.FlushWriteBehind())
						}
						drop := func() {
							implicitCommit(t, b, func(tx *BadgerTransaction) error {
								if err := tx.DeleteEdge("test:r"); err != nil {
									return err
								}
								return tx.DeleteNode("test:a")
							})
						}
						if beforeBegin {
							drop()
						}
						tx, err := b.BeginTransaction()
						require.NoError(t, err)
						defer tx.Rollback()
						require.NoError(t, tx.SetImplicit(true))
						// No target read before this peer commit.
						if !beforeBegin {
							drop()
						}
						if flushDelete {
							require.NoError(t, b.FlushWriteBehind())
						}
						writes := []struct {
							name  string
							write func() error
						}{
							{"node", func() error {
								return tx.UpdateNode(&Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"note": "late"}})
							}},
							{"observed", func() error {
								return tx.UpdateNodeObserved(&Node{ID: "test:a"}, &Node{ID: "test:a", Properties: map[string]any{"note": "late"}})
							}},
							{"edge", func() error {
								return tx.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"})
							}},
							{"endpoint", func() error {
								return tx.CreateEdge(&Edge{ID: "test:new", StartNode: "test:b", EndNode: "test:a", Type: "R"})
							}},
						}
						want := ErrConflict
						if beforeBegin {
							want = ErrNotFound
						}
						for _, w := range writes {
							if err := w.write(); !errors.Is(err, want) {
								t.Errorf("%s: wanted %v, got %v", w.name, want, err)
							}
						}
					})
				}
			}
		}
	}
}

// The transaction's own delete stays an ordinary result while a peer's delete
// published after begin is a conflict, in the same transaction.
func TestVanishedWriteTargetInterleavedOwnPeerDelete(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: async, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer b.Close()
			for _, id := range []NodeID{"test:own", "test:peer"} {
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"id": string(id)}})
				require.NoError(t, err)
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetImplicit(true))
			own, err := tx.GetNode("test:own")
			require.NoError(t, err)
			peer, err := tx.GetNode("test:peer")
			require.NoError(t, err)
			require.NoError(t, tx.DeleteNode("test:own"))
			implicitCommit(t, b, func(p *BadgerTransaction) error { return p.DeleteNode("test:peer") })
			if async {
				require.NoError(t, b.FlushWriteBehind())
			}
			require.NotErrorIs(t, tx.UpdateNodeObserved(own, copyNode(own)), ErrConflict)
			err = tx.UpdateNodeObserved(peer, copyNode(peer))
			if err == nil {
				err = tx.Commit()
			}
			require.ErrorIs(t, err, ErrConflict)
		})
	}
}

// A seed flush racing the writer's begin cannot change the classification of
// a peer delete committed after begin returned.
func TestVanishedWriteTargetFlushRacingBegin(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	for i := 0; i < 12; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			id := NodeID(fmt.Sprintf("test:race%d", i))
			implicitCommit(t, b, func(tx *BadgerTransaction) error {
				_, err := tx.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
				return err
			})
			flushed := make(chan error, 1)
			go func() { flushed <- b.FlushWriteBehind() }()
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, <-flushed)
			implicitCommit(t, b, func(p *BadgerTransaction) error { return p.DeleteNode(id) })
			require.NoError(t, b.FlushWriteBehind())
			require.ErrorIs(t, tx.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}}), ErrConflict, "the peer committed after BeginTransaction returned")
		})
	}
}
