package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Author: PR #1670 implementation lane (Codex). Public storage consumers
// exercise the committed node endpoints at physical publication, including
// buffered drain, recovery and replay. Cache/count/notification assertions are
// integration controls; ACK-only lookup and relationship integrity are separate.
func TestCommitIndexMembershipStorageOrigins(t *testing.T) {
	for _, mode := range []string{"synchronous", "flush", "recovery", "replay"} {
		for _, final := range []string{"stable", "changed"} {
			t.Run(mode+"/"+final, func(t *testing.T) {
				dir := t.TempDir()
				options := writeBehindTestOptions{dataDir: dir, synchronous: mode == "synchronous", manualDrain: true, wal: mode == "recovery" || mode == "replay"}
				engine := newWriteBehindTestEngine(t, time.Hour, options)
				schema := engine.GetSchemaForNamespace("test")
				require.NoError(t, schema.AddPropertyIndex("doc_id", "Doc", []string{"id"}))
				require.NoError(t, schema.AddCompositeIndex("doc_key", "Doc", []string{"tenant", "id"}))
				require.NoError(t, schema.AddUniqueConstraint("doc_uid", "Doc", "uid"))
				require.NoError(t, schema.AddPropertyIndex("aux_gate", "Aux", []string{"gate"}))
				_, err := engine.CreateNode(context.Background(), &Node{ID: "test:doc", Labels: []string{"Doc", "Aux"}, Properties: map[string]any{"id": "stable", "uid": "stable", "tenant": "t", "gate": "g", "c": int64(0)}})
				require.NoError(t, err)
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err = engine.CreateNode(context.Background(), &Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(context.Background(), &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]any{"id": "stable", "c": int64(0)}}))
				schema.MarkPropertyIndexesFilled()
				var eventsMu sync.Mutex
				var nodes, edges int
				observe := func(b *BadgerEngine) {
					b.OnNodeUpdated(func(*Node) { eventsMu.Lock(); nodes++; eventsMu.Unlock() })
					b.OnEdgeUpdated(func(*Edge) { eventsMu.Lock(); edges++; eventsMu.Unlock() })
				}
				assertEvents := func() {
					eventsMu.Lock()
					n, e := nodes, edges
					eventsMu.Unlock()
					require.Equal(t, 2, n, "notification multiplicity remains operation-based")
					require.Equal(t, 2, e)
				}
				observe(engine)
				tx, err := engine.BeginTransaction(context.Background())
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(mode != "synchronous"))
				for i, value := range []string{"temporary", final} {
					labels := []string{"Temporary"}
					if i == 1 {
						labels = []string{"Doc", "Aux"}
					}
					require.NoError(t, tx.UpdateGraphNode(context.Background(), NodeUpdateRequest{After: &Node{ID: "test:doc", Labels: labels, Properties: map[string]any{"id": value, "uid": value, "tenant": "t", "gate": "g", "c": int64(i + 1)}}, Policy: ClientNodeEdits}))
					require.NoError(t, tx.UpdateEdge(context.Background(), EdgeUpdateRequest{After: &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]any{"c": int64(i + 1)}}}))
				}
				var publish func() error
				if mode == "synchronous" {
					publish = tx.Commit
				} else {
					require.NoError(t, tx.Commit())
					require.True(t, tx.AcknowledgedBuffered())
					if mode == "flush" {
						publish = func() error { return engine.FlushWriteBehind(context.Background()) }
					} else {
						wal := engine.bufferedCommitWAL
						entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
						require.NoError(t, err)
						crashClose(t, engine, wal)
						engine = newWriteBehindTestEngine(t, time.Hour, options)
						schema = engine.GetSchemaForNamespace("test")
						observe(engine)
						if mode == "recovery" {
							publish = func() error {
								n, err := RecoverBufferedCommits(engine, engine.bufferedCommitWAL)
								if err == nil && n != 1 {
									return fmt.Errorf("recovered %d commits, want 1", n)
								}
								return err
							}
						} else {
							publish = func() error {
								for _, entry := range entries {
									if entry.Operation == OpBufferedCommit {
										if err := ReplayWALEntry(engine, entry); err != nil {
											return err
										}
									}
								}
								return nil
							}
						}
					}
				}
				gates := schema.GetCompositeIndexesForLabel("Aux")
				require.Len(t, gates, 1)
				release, waiting := HoldIndexReadForTesting(gates[0])
				var once sync.Once
				open := func() { once.Do(release) }
				defer open()
				var worker publicationOrderWorker
				worker.start(publish)
				require.Eventually(t, waiting, 15*time.Second, time.Millisecond)
				// Only the node indexes preceding Aux have published at this point.
				require.Equal(t, []NodeID{"test:doc"}, checkedIndexRead[[]NodeID](t)(schema.PropertyIndexLookup(context.Background(), "Doc", "id", final)))
				composite, ok := schema.GetCompositeIndex("doc_key")
				require.True(t, ok)
				require.Equal(t, []NodeID{"test:doc"}, checkedIndexRead[[]NodeID](t)(composite.LookupFull(context.Background(), "t", final)))
				require.Empty(t, checkedIndexRead[[]NodeID](t)(schema.PropertyIndexLookup(context.Background(), "Doc", "id", "temporary")))
				open()
				worker.join(t)
				require.NoError(t, worker.err)
				assertCommitMembershipPublic(t, engine, final)
				assertEvents()
			})
		}
	}
}

func assertCommitMembershipPublic(t *testing.T, engine *BadgerEngine, final string) {
	t.Helper()
	schema := engine.GetSchemaForNamespace("test")
	require.Equal(t, []NodeID{"test:doc"}, checkedIndexRead[[]NodeID](t)(schema.PropertyIndexLookup(context.Background(), "Doc", "id", final)))
	require.Empty(t, checkedIndexRead[[]NodeID](t)(schema.PropertyIndexLookup(context.Background(), "Doc", "id", "temporary")))
	// This is the uniqueness-owned posting, not the separate unique-value claim.
	var unique *CompositeIndex
	for _, idx := range schema.GetCompositeIndexesForLabel("Doc") {
		if idx.OwningConstraint == "doc_uid" {
			unique = idx
		}
	}
	require.NotNil(t, unique)
	require.Equal(t, []NodeID{"test:doc"}, checkedIndexRead[[]NodeID](t)(unique.LookupFull(context.Background(), final)))
	nodes, err := engine.GetNodesByLabel(context.Background(), "Doc")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, final, nodes[0].Properties["id"])
	require.True(t, compareValues(int64(2), nodes[0].Properties["c"]))
	nodes, err = engine.GetNodesByLabel(context.Background(), "Temporary")
	require.NoError(t, err)
	require.Empty(t, nodes)
	edge, err := engine.GetEdge(context.Background(), "test:e")
	require.NoError(t, err)
	require.True(t, compareValues(int64(2), edge.Properties["c"]))
	n, err := engine.NodeCount(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 3, n)
	n, err = engine.EdgeCount(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	composite, ok := schema.GetCompositeIndex("doc_key")
	require.True(t, ok)
	require.Equal(t, []NodeID{"test:doc"}, checkedIndexRead[[]NodeID](t)(composite.LookupFull(context.Background(), "t", final)))
	require.Equal(t, []NodeID{"test:doc"}, checkedIndexRead[[]NodeID](t)(composite.LookupPrefix(context.Background(), "t")))
}
