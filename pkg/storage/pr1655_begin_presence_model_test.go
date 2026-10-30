package storage

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestPR1655BeginPresenceSnapshotModel(t *testing.T) {
	for _, syncWrites := range []bool{false, true} {
		for seedIndex := 0; seedIndex < 4; seedIndex++ {
			seed := int64(165500 + seedIndex)
			t.Run(fmt.Sprintf("sync=%t/seed=%d", syncWrites, seed), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))
				b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, SyncWrites: syncWrites}))
				require.NoError(t, err)
				defer b.Close()
				type modelTx struct {
					tx    *BadgerTransaction
					begin map[NodeID]bool
				}
				live := map[NodeID]bool{}
				persisted := map[NodeID]bool{} // in Badger, not only in the buffer
				var everCreated []NodeID
				var open []*modelTx
				next := 0
				buffered := func(write func(*BadgerTransaction) error) {
					tx := beginPR1655AcceptanceWriter(t, b)
					require.NoError(t, tx.SetImplicit(true))
					require.NoError(t, write(tx))
					require.NoError(t, tx.Commit())
				}
				synchronous := func(write func(*BadgerTransaction) error) {
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, write(tx))
					require.NoError(t, tx.Commit())
				}
				for step := 0; step < 160; step++ {
					switch op := rng.Intn(10); {
					case op < 3: // create
						id := NodeID(fmt.Sprintf("test:m%d", next))
						next++
						switch rng.Intn(3) {
						case 0:
							buffered(func(tx *BadgerTransaction) error {
								_, err := tx.CreateNode(&Node{ID: id})
								return err
							})
						case 1:
							synchronous(func(tx *BadgerTransaction) error {
								_, err := tx.CreateNode(&Node{ID: id})
								return err
							})
							persisted[id] = true
						default:
							_, err := b.CreateNode(&Node{ID: id})
							require.NoError(t, err)
							persisted[id] = true
						}
						live[id] = true
						everCreated = append(everCreated, id)
					case op < 6: // delete a live target
						var candidates []NodeID
						for _, id := range everCreated {
							if live[id] {
								candidates = append(candidates, id)
							}
						}
						if len(candidates) == 0 {
							continue
						}
						id := candidates[rng.Intn(len(candidates))]
						producer := rng.Intn(3)
						if !persisted[id] {
							producer = 0 // only the buffer sees a buffered-only node
						}
						switch producer {
						case 0:
							buffered(func(tx *BadgerTransaction) error { return tx.DeleteGraphNode(id, CascadeEdges) })
						case 1:
							synchronous(func(tx *BadgerTransaction) error { return tx.DeleteGraphNode(id, CascadeEdges) })
						default:
							require.NoError(t, b.DeleteNode(id))
						}
						delete(live, id)
					case op < 8: // begin
						if len(open) >= 4 {
							continue
						}
						tx, err := b.BeginTransaction()
						require.NoError(t, err)
						require.NoError(t, tx.SetImplicit(true))
						require.NoError(t, tx.SetNamespace("test"))
						begin := make(map[NodeID]bool, len(live))
						for id := range live {
							begin[id] = true
						}
						open = append(open, &modelTx{tx: tx, begin: begin})
					default: // flush and retire
						require.NoError(t, b.FlushWriteBehind())
						for id := range live {
							persisted[id] = true
						}
					}
				}
				if rng.Intn(2) == 0 {
					require.NoError(t, b.FlushWriteBehind())
				}
				checked := 0
				for _, m := range open {
					for _, id := range everCreated {
						if live[id] {
							continue
						}
						err := m.tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: id}, Policy: ClientNodeEdits})
						if m.begin[id] {
							require.ErrorIs(t, err, ErrConflict, "%s was live at begin and deleted after it", id)
						} else {
							require.ErrorIs(t, err, ErrNotFound, "%s was not live at begin", id)
							require.NotErrorIs(t, err, ErrConflict)
						}
						checked++
					}
					require.NoError(t, m.tx.Rollback())
				}
				t.Logf("seed %d: %d classifications across %d transactions", seed, checked, len(open))
			})
		}
	}
}
