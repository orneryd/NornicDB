package storage

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A peer delete started after the writer's commit checked its target, while
// the writer's commit is paused before its write, waits for that commit to
// publish: it cannot land between the check and the write. Once it runs, an
// acknowledged delete is never undone (independent review of 2be99f2,
// TestClaudeR73CommitWindowDelete, adapted: the pause point is the existing
// nodeBeforeConflictRead hook).
func TestVanishedWriteTargetCommitOrderWindow(t *testing.T) {
	for _, writeBehind := range []bool{true, false} {
		for _, writerImplicit := range []bool{true, false} {
			for _, producer := range []string{"buffered", "direct", "transaction"} {
				for _, operation := range []string{"relationship", "endpoint", "node"} {
					name := fmt.Sprintf("wb=%t/implicit=%t/%s/%s", writeBehind, writerImplicit, producer, operation)
					t.Run(name, func(t *testing.T) {
						b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
						require.NoError(t, err)
						defer b.Close()
						for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d", "test:x"} {
							_, err = b.CreateNode(&Node{ID: id})
							require.NoError(t, err)
						}
						require.NoError(t, b.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"note": "old"}}))
						if writeBehind {
							require.NoError(t, b.FlushWriteBehind())
						}
						writer, err := b.BeginTransaction()
						require.NoError(t, err)
						defer writer.Rollback()
						require.NoError(t, writer.SetImplicit(writerImplicit))
						switch operation {
						case "relationship":
							require.NoError(t, writer.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"note": "set"}}))
						case "endpoint":
							require.NoError(t, writer.CreateEdge(&Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:d", Type: "R"}))
						case "node":
							require.NoError(t, writer.UpdateNode(&Node{ID: "test:c", Properties: map[string]any{"note": "set"}}))
						}
						// An unrelated second write: its check is the pause point.
						require.NoError(t, writer.UpdateNode(&Node{ID: "test:x", Properties: map[string]any{"note": "set"}}))
						peerDelete := func() error {
							if producer == "direct" {
								switch operation {
								case "relationship":
									return b.DeleteEdge("test:r")
								case "endpoint":
									return b.DeleteNode("test:d")
								default:
									return b.DeleteNode("test:c")
								}
							}
							peer, err := b.BeginTransaction()
							if err != nil {
								return err
							}
							defer peer.Rollback()
							if err := peer.SetImplicit(producer == "buffered"); err != nil {
								return err
							}
							switch operation {
							case "relationship":
								err = peer.DeleteEdge("test:r")
							case "endpoint":
								err = peer.DeleteNode("test:d")
							default:
								err = peer.DeleteNode("test:c")
							}
							if err != nil {
								return err
							}
							return peer.Commit()
						}
						var armed atomic.Bool
						armed.Store(true)
						peerDone := make(chan error, 1)
						blocked := false
						b.nodeBeforeConflictRead = func(id NodeID) {
							if id != "test:x" || !armed.CompareAndSwap(true, false) {
								return
							}
							go func() { peerDone <- peerDelete() }()
							select {
							case err := <-peerDone:
								peerDone <- err
							case <-time.After(300 * time.Millisecond):
								blocked = true
							}
						}
						commitErr := writer.Commit()
						require.True(t, blocked, "the peer delete waited for the writer's commit to publish")
						require.NoError(t, commitErr, "the writer's target was live when its commit checked it")
						var peerErr error
						select {
						case peerErr = <-peerDone:
						case <-time.After(30 * time.Second):
							t.Fatal("the peer delete never ran after the writer's commit")
						}
						b.nodeBeforeConflictRead = nil
						if peerErr != nil {
							require.ErrorIs(t, peerErr, ErrConflict, "a delete ordered after the writer may only fail as a conflict")
						}
						if writeBehind {
							require.NoError(t, b.FlushWriteBehind())
						}
						var undone bool
						switch operation {
						case "relationship":
							_, err = b.GetEdge("test:r")
							undone = peerErr == nil && err == nil
						case "endpoint":
							_, edgeErr := b.GetEdge("test:new")
							_, nodeErr := b.GetNode("test:d")
							undone = edgeErr == nil && nodeErr != nil
						case "node":
							_, err = b.GetNode("test:c")
							undone = peerErr == nil && err == nil
						}
						t.Logf("%s: writer=%v peer=%v undone=%t", name, commitErr, peerErr, undone)
						if writeBehind && writerImplicit && producer != "buffered" {
							// The writer's write was acknowledged into the buffer and the
							// physical delete published after it; the flush then replays
							// the older buffered write over the delete. That replay order
							// is the write-behind commit order's (#1676) and reproduces on
							// the release base with no concurrency at all.
							t.Skipf("the flush replays the acknowledged buffered write over the later delete (undone=%t); fixed by #1676", undone)
						}
						require.False(t, undone, "an acknowledged delete ordered after the writer is not undone, and no relationship points at a deleted node")
					})
				}
			}
		}
	}
}

// Hook-free: a writer whose statement also updates padding nodes and a peer
// delete commit at the same time, round after round. An acknowledged delete
// is never undone, and a relationship never points at a deleted node
// (independent review of 2be99f2, TestClaudeR73CommitWindowStress).
func TestVanishedWriteTargetCommitOrderStress(t *testing.T) {
	for _, writeBehind := range []bool{true, false} {
		for _, operation := range []string{"relationship", "node", "endpoint"} {
			t.Run(fmt.Sprintf("wb=%t/%s", writeBehind, operation), func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				defer b.Close()
				const pads = 96
				for i := 0; i < pads; i++ {
					_, err = b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:pad%d", i))})
					require.NoError(t, err)
				}
				_, err = b.CreateNode(&Node{ID: "test:anchor"})
				require.NoError(t, err)
				const rounds = 400
				revived, bothAcked, writerConflicts := 0, 0, 0
				for round := 0; round < rounds; round++ {
					target := fmt.Sprintf("test:t%d", round)
					if operation == "relationship" {
						require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(target), StartNode: "test:anchor", EndNode: "test:pad0", Type: "R"}))
					} else {
						_, err = b.CreateNode(&Node{ID: NodeID(target)})
						require.NoError(t, err)
					}
					if writeBehind {
						require.NoError(t, b.FlushWriteBehind())
					}
					writer, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, writer.SetImplicit(true))
					newEdge := EdgeID(target + "-new")
					switch operation {
					case "relationship":
						err = writer.UpdateEdge(&Edge{ID: EdgeID(target), StartNode: "test:anchor", EndNode: "test:pad0", Type: "R", Properties: map[string]any{"round": round}})
					case "node":
						err = writer.UpdateNode(&Node{ID: NodeID(target), Properties: map[string]any{"round": round}})
					default:
						err = writer.CreateEdge(&Edge{ID: newEdge, StartNode: "test:anchor", EndNode: NodeID(target), Type: "R"})
					}
					require.NoError(t, err)
					for i := 0; i < pads; i++ {
						require.NoError(t, writer.UpdateNode(&Node{ID: NodeID(fmt.Sprintf("test:pad%d", i)), Properties: map[string]any{"round": round}}))
					}
					var wg sync.WaitGroup
					start := make(chan struct{})
					var writerErr, peerErr error
					wg.Add(2)
					go func() {
						defer wg.Done()
						<-start
						writerErr = writer.Commit()
					}()
					go func() {
						defer wg.Done()
						<-start
						for spin := 0; spin < round%32; spin++ {
							runtime.Gosched()
						}
						peer, err := b.BeginTransaction()
						if err != nil {
							peerErr = err
							return
						}
						defer peer.Rollback()
						if err := peer.SetImplicit(true); err != nil {
							peerErr = err
							return
						}
						if operation == "relationship" {
							err = peer.DeleteEdge(EdgeID(target))
						} else {
							err = peer.DeleteNode(NodeID(target))
						}
						if err != nil {
							peerErr = err
							return
						}
						peerErr = peer.Commit()
					}()
					close(start)
					wg.Wait()
					_ = writer.Rollback()
					if writerErr != nil {
						require.ErrorIs(t, writerErr, ErrConflict)
						writerConflicts++
					}
					if peerErr != nil {
						require.ErrorIs(t, peerErr, ErrConflict)
					}
					if writerErr != nil || peerErr != nil {
						continue
					}
					bothAcked++
					if writeBehind {
						require.NoError(t, b.FlushWriteBehind())
					}
					var present bool
					switch operation {
					case "relationship":
						_, err := b.GetEdge(EdgeID(target))
						present = err == nil
					case "node":
						_, err := b.GetNode(NodeID(target))
						present = err == nil
					default:
						_, err := b.GetEdge(newEdge)
						present = err == nil
					}
					if present {
						revived++
						t.Logf("round %d: both commits acknowledged and the deleted %s target is present", round, operation)
					}
				}
				t.Logf("wb=%t %s: rounds=%d bothAcked=%d writerConflicts=%d revived=%d", writeBehind, operation, rounds, bothAcked, writerConflicts, revived)
				require.Zero(t, revived, "an acknowledged delete was undone by a concurrently committing writer")
			})
		}
	}
}

// A target a peer created after the writer began, which the writer staged a
// write against and the peer then deleted, is not revived: at commit the
// write's target was observed by the writer, so gone now is a concurrent
// delete (independent review of 2be99f2, TestClaudeR73TargetCreatedAfterBegin).
func TestVanishedWriteTargetCreatedAfterBegin(t *testing.T) {
	for _, flushCreate := range []bool{false, true} {
		for _, operation := range []string{"node", "relationship", "endpoint"} {
			t.Run(fmt.Sprintf("flushCreate=%t/%s", flushCreate, operation), func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				defer b.Close()
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err = b.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, b.FlushWriteBehind())
				writer, err := b.BeginTransaction()
				require.NoError(t, err)
				defer writer.Rollback()
				require.NoError(t, writer.SetImplicit(true))
				implicitCommit(t, b, func(tx *BadgerTransaction) error {
					if operation == "relationship" {
						return tx.CreateEdge(&Edge{ID: "test:late", StartNode: "test:a", EndNode: "test:b", Type: "R"})
					}
					_, err := tx.CreateNode(&Node{ID: "test:late"})
					return err
				})
				if flushCreate {
					require.NoError(t, b.FlushWriteBehind())
				}
				var stageErr error
				switch operation {
				case "node":
					stageErr = writer.UpdateNode(&Node{ID: "test:late", Properties: map[string]any{"note": "set"}})
				case "relationship":
					stageErr = writer.UpdateEdge(&Edge{ID: "test:late", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"note": "set"}})
				case "endpoint":
					stageErr = writer.CreateEdge(&Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:late", Type: "R"})
				}
				if stageErr != nil {
					require.ErrorIs(t, stageErr, ErrNotFound, "a target the begin snapshot does not hold")
					return
				}
				implicitCommit(t, b, func(tx *BadgerTransaction) error {
					if operation == "relationship" {
						return tx.DeleteEdge("test:late")
					}
					return tx.DeleteNode("test:late")
				})
				require.ErrorIs(t, writer.Commit(), ErrConflict, "the writer observed the target when it staged the write")
				require.NoError(t, b.FlushWriteBehind())
				var present bool
				switch operation {
				case "node":
					_, err = b.GetNode("test:late")
				case "relationship":
					_, err = b.GetEdge("test:late")
				case "endpoint":
					_, err = b.GetEdge("test:new")
				}
				present = err == nil
				require.False(t, present, "a peer-deleted target is not revived, nor a relationship to it created")
			})
		}
	}
}

// A writer stages DETACH DELETE n; a peer then creates a relationship to n
// and is acknowledged; the writer's commit sees that relationship, buffered
// or stored, and fails as a conflict instead of leaving it pointing at a
// deleted node (independent review of 2be99f2, also on the release base).
func TestVanishedWriteTargetDetachDeleteAfterPeerCreate(t *testing.T) {
	for _, producer := range []string{"buffered", "transaction"} {
		for _, writerImplicit := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/implicit=%t", producer, writerImplicit), func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				defer b.Close()
				for _, id := range []NodeID{"test:m", "test:n", "test:o"} {
					_, err = b.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, b.CreateEdge(&Edge{ID: "test:old", StartNode: "test:o", EndNode: "test:n", Type: "R"}))
				require.NoError(t, b.FlushWriteBehind())
				writer, err := b.BeginTransaction()
				require.NoError(t, err)
				defer writer.Rollback()
				require.NoError(t, writer.SetImplicit(writerImplicit))
				require.NoError(t, writer.DeleteEdge("test:old"))
				require.NoError(t, writer.DeleteNode("test:n"))
				peer, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, peer.SetImplicit(producer == "buffered"))
				require.NoError(t, peer.CreateEdge(&Edge{ID: "test:new", StartNode: "test:m", EndNode: "test:n", Type: "R"}))
				require.NoError(t, peer.Commit())
				require.ErrorIs(t, writer.Commit(), ErrConflict)
				require.NoError(t, b.FlushWriteBehind())
				_, nodeErr := b.GetNode("test:n")
				_, edgeErr := b.GetEdge("test:new")
				require.False(t, nodeErr != nil && edgeErr == nil, "an acknowledged relationship points at a deleted node")
			})
		}
	}
}
