package storage

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A write target deleted by a commit after the writer's snapshot is a
// concurrent modification; one deleted before the snapshot, or never created,
// stays not found, also for blind writes and in write-behind mode.
func TestVanishedWriteTargetClassification(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: async, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			flush := func() {
				if async {
					require.NoError(t, b.FlushWriteBehind())
				}
			}
			seed := func(id string) {
				// Direct seed writes start from a drained buffer.
				flush()
				_, err := b.CreateNode(&Node{ID: NodeID("test:" + id), Labels: []string{"Doc"}, Properties: map[string]any{"id": id}})
				require.NoError(t, err)
				_, err = b.CreateNode(&Node{ID: NodeID("test:" + id + "-end"), Labels: []string{"Doc"}})
				require.NoError(t, err)
				require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID("test:" + id + "-r"), StartNode: NodeID("test:" + id), EndNode: NodeID("test:" + id + "-end"), Type: "R", Properties: map[string]any{"w": int64(1)}}))
				flush()
			}
			peerDelete := func(id string) {
				peer, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, peer.SetImplicit(true))
				require.NoError(t, peer.DeleteEdge(EdgeID("test:"+id+"-r")))
				require.NoError(t, peer.DeleteNode(NodeID("test:"+id)))
				require.NoError(t, peer.Commit())
			}
			writes := map[string]func(tx *BadgerTransaction, id string) error{
				"update_node": func(tx *BadgerTransaction, id string) error {
					return tx.UpdateNode(&Node{ID: NodeID("test:" + id), Labels: []string{"Doc"}, Properties: map[string]any{"id": id, "note": "w"}})
				},
				"observed_update": func(tx *BadgerTransaction, id string) error {
					before := &Node{ID: NodeID("test:" + id), Labels: []string{"Doc"}, Properties: map[string]any{"id": id}}
					return tx.UpdateNodeObserved(before, &Node{ID: before.ID, Labels: []string{"Doc"}, Properties: map[string]any{"id": id, "note": "w"}})
				},
				"update_edge": func(tx *BadgerTransaction, id string) error {
					return tx.UpdateEdge(&Edge{ID: EdgeID("test:" + id + "-r"), StartNode: NodeID("test:" + id), EndNode: NodeID("test:" + id + "-end"), Type: "R", Properties: map[string]any{"w": int64(2)}})
				},
				"create_edge": func(tx *BadgerTransaction, id string) error {
					return tx.CreateEdge(&Edge{ID: EdgeID("test:" + id + "-new"), StartNode: NodeID("test:" + id + "-end"), EndNode: NodeID("test:" + id), Type: "R"})
				},
			}
			for name, write := range writes {
				t.Run(name, func(t *testing.T) {
					// Deleted after the writer began: an ordinary conflict, while
					// staging under write-behind's read-committed reads or at
					// commit under the pinned snapshot.
					late := name + "-late"
					seed(late)
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetNamespace("test"))
					peerDelete(late)
					err = write(tx, late)
					if err == nil {
						require.False(t, async, "write-behind staging must classify the vanished target")
						err = tx.Commit()
					} else {
						require.NoError(t, tx.Rollback())
					}
					require.ErrorIs(t, err, ErrConflict)
					require.NotErrorIs(t, err, ErrNotFound)

					// Deleted before the writer began, or never created: not found.
					early := name + "-early"
					seed(early)
					peerDelete(early)
					flush()
					tx, err = b.BeginTransaction()
					require.NoError(t, err)
					for _, id := range []string{early, name + "-never"} {
						err := write(tx, id)
						require.ErrorIs(t, err, ErrNotFound, id)
						require.False(t, errors.Is(err, ErrConflict), "%s: %v", id, err)
					}
					require.NoError(t, tx.Rollback())

					if name == "create_edge" {
						// An endpoint this transaction deleted stays not found.
						own := name + "-own"
						seed(own)
						tx, err = b.BeginTransaction()
						require.NoError(t, err)
						require.NoError(t, tx.DeleteEdge(EdgeID("test:"+own+"-r")))
						require.NoError(t, tx.DeleteNode(NodeID("test:"+own)))
						err = write(tx, own)
						require.ErrorIs(t, err, ErrNotFound)
						require.False(t, errors.Is(err, ErrConflict), "%v", err)
						require.NoError(t, tx.Rollback())
					}

					// Deleted after the writer began and already flushed: the
					// MVCC tombstone still classifies it.
					flushed := name + "-flushed"
					seed(flushed)
					tx, err = b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetNamespace("test"))
					peerDelete(flushed)
					flush()
					err = write(tx, flushed)
					if err == nil {
						require.False(t, async, "write-behind staging must classify the vanished target")
						err = tx.Commit()
					} else {
						require.NoError(t, tx.Rollback())
					}
					require.ErrorIs(t, err, ErrConflict)
				})
			}
		})
	}
}
