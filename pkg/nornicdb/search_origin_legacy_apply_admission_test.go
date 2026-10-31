package nornicdb

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1665 implementation lane. The public native write boundary is held
// after legacy apply admission and before storage can create its own origin.
type originLegacyApplyGate struct {
	storage.Engine
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

// This holds the actual public Replicator handoff after the engine selected
// and encoded its mutation, before leader preparation can choose an origin.
type originCommandApplyGate struct {
	replication.Replicator
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *originCommandApplyGate) Apply(cmd *replication.Command, timeout time.Duration) error {
	g.once.Do(func() { close(g.entered); <-g.release })
	return g.Replicator.Apply(cmd, timeout)
}

func (g *originLegacyApplyGate) GetInnerEngine() storage.Engine { return g.Engine }

func (g *originLegacyApplyGate) CreateNode(node *storage.Node) (storage.NodeID, error) {
	g.once.Do(func() { close(g.entered); <-g.release })
	return g.Engine.CreateNode(node)
}
func (g *originLegacyApplyGate) UpdateNode(updateRequest storage.NodeUpdateRequest) error {
	g.once.Do(func() { close(g.entered); <-g.release })
	return g.Engine.UpdateNode(updateRequest)
}
func (g *originLegacyApplyGate) CreateEdge(edge *storage.Edge) error {
	g.once.Do(func() { close(g.entered); <-g.release })
	return g.Engine.CreateEdge(edge)
}
func (g *originLegacyApplyGate) UpdateEdge(updateRequest storage.EdgeUpdateRequest) error {
	g.once.Do(func() { close(g.entered); <-g.release })
	return g.Engine.UpdateEdge(updateRequest)
}
func (g *originLegacyApplyGate) BulkCreateNodes(nodes []*storage.Node) error {
	g.once.Do(func() { close(g.entered); <-g.release })
	return g.Engine.BulkCreateNodes(nodes)
}
func (g *originLegacyApplyGate) BulkCreateEdges(edges []*storage.Edge) error {
	g.once.Do(func() { close(g.entered); <-g.release })
	return g.Engine.BulkCreateEdges(edges)
}

func TestSearchOriginLegacyReplicationApplyDrop(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		kind                          replication.CommandType
		edge, bulk, update, composite bool
	}{
		{"create_node", replication.CmdCreateNode, false, false, false, false},
		{"update_node", replication.CmdUpdateNode, false, false, true, false},
		{"bulk_nodes", replication.CmdBulkCreateNodes, false, true, false, false},
		{"create_edge", replication.CmdCreateEdge, true, false, false, false},
		{"update_edge", replication.CmdUpdateEdge, true, false, true, false},
		{"bulk_edges", replication.CmdBulkCreateEdges, true, true, false, false},
		{"composite_create_node", replication.CmdCreateNode, false, false, false, true},
		{"composite_update_node", replication.CmdUpdateNode, false, false, true, true},
		{"composite_bulk_nodes", replication.CmdBulkCreateNodes, false, true, false, true},
		{"composite_create_edge", replication.CmdCreateEdge, true, false, false, true},
		{"composite_update_edge", replication.CmdUpdateEdge, true, false, true, true},
		{"composite_bulk_edges", replication.CmdBulkCreateEdges, true, true, false, true},
		{"snapshot_node", replication.CmdCreateNode, false, false, false, false},
		{"snapshot_edge", replication.CmdCreateEdge, true, false, false, false},
		{"wal_create_node", replication.CmdCreateNode, false, false, false, false},
		{"wal_update_node", replication.CmdUpdateNode, false, false, true, false},
		{"wal_bulk_nodes", replication.CmdBulkCreateNodes, false, true, false, false},
		{"wal_create_edge", replication.CmdCreateEdge, true, false, false, false},
		{"wal_update_edge", replication.CmdUpdateEdge, true, false, true, false},
		{"wal_bulk_edges", replication.CmdBulkCreateEdges, true, true, false, false},
		{"command_create_node", replication.CmdCreateNode, false, false, false, false},
		{"command_update_node", replication.CmdUpdateNode, false, false, true, false},
		{"command_bulk_nodes", replication.CmdBulkCreateNodes, false, true, false, false},
		{"command_create_edge", replication.CmdCreateEdge, true, false, false, false},
		{"command_update_edge", replication.CmdUpdateEdge, true, false, true, false},
		{"command_bulk_edges", replication.CmdBulkCreateEdges, true, true, false, false},
	} {
		for _, recreate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", tc.name, recreate), func(t *testing.T) {
				db, _ := openConsumerDB(t, false)
				ctx := context.Background()
				const namespace = "legacy_apply_origin"
				base := db.GetBaseStorageForManager()
				store := storage.NewNamespacedEngine(base, namespace)
				svc, err := db.EnsureSearchIndexes(ctx, namespace, store, SearchIndexBuildWait)
				require.NoError(t, err)
				exec := cypher.NewStorageExecutor(store)
				exec.SetSearchService(svc)
				create := "CREATE VECTOR INDEX legacy_origin_vec FOR (n:Doc) ON (n.embedding)"
				if tc.edge {
					create = "CREATE VECTOR INDEX legacy_origin_vec FOR ()-[r:LINK]-() ON (r.embedding)"
				}
				_, err = exec.Execute(ctx, create, nil)
				require.NoError(t, err)
				vector := make([]float64, svc.VectorIndexDimensions())
				query := make([]float32, len(vector))
				vector[0], query[0] = 1, 1
				var data []byte
				if tc.edge {
					for _, id := range []storage.NodeID{"a", "b"} {
						_, err = store.CreateNode(&storage.Node{ID: id})
						require.NoError(t, err)
					}
					if tc.update {
						require.NoError(t, store.CreateEdge(&storage.Edge{ID: "selected", StartNode: "a", EndNode: "b", Type: "LINK"}))
					}
					data, err = storage.EncodeEdgePayload(&storage.Edge{ID: namespace + ":selected", StartNode: namespace + ":a", EndNode: namespace + ":b", Type: "LINK", Properties: map[string]any{"embedding": vector}})
				} else {
					if tc.update {
						_, err = store.CreateNode(&storage.Node{ID: "selected", Labels: []string{"Doc"}})
						require.NoError(t, err)
					}
					data, err = storage.EncodeNodePayload(&storage.Node{ID: namespace + ":selected", Labels: []string{"Doc"}, Properties: map[string]any{"embedding": vector, "content": "legacyapplyword"}})
				}
				require.NoError(t, err)
				walMode := strings.HasPrefix(tc.name, "wal_")
				commandMode := strings.HasPrefix(tc.name, "command_")
				var walOperation storage.Operation
				if walMode || commandMode {
					kind := storage.EntityCommandKind(tc.kind)
					if tc.bulk {
						if tc.edge {
							kind = storage.EntityCreateEdge
						} else {
							kind = storage.EntityCreateNode
						}
					}
					walOperation, err = storage.DecodeEntityChange(kind, data)
					require.NoError(t, err)
				}
				if tc.bulk {
					data, err = storage.EncodeCommandPayload([][]byte{data})
					require.NoError(t, err)
				}
				snapshot := tc.name == "snapshot_node" || tc.name == "snapshot_edge"
				if snapshot {
					op, decodeErr := storage.DecodeEntityChange(storage.EntityCommandKind(tc.kind), data)
					require.NoError(t, decodeErr)
					// The snapshot owner's typed wire image, not an anonymous struct.
					image := storage.ReplicationSnapshot{}
					if tc.edge {
						image.Edges = []*storage.Edge{op.Edge}
					} else {
						image.Nodes = []*storage.Node{op.Node}
					}
					data, err = storage.EncodeCommandPayload(image)
					require.NoError(t, err)
				}
				gate := &originLegacyApplyGate{Engine: base, entered: make(chan struct{}), release: make(chan struct{})}
				var once sync.Once
				unblock := func() { once.Do(func() { close(gate.release) }) }
				t.Cleanup(unblock)
				var walWriter *storage.WALEngine
				if walMode {
					journal, journalErr := storage.NewWAL(t.TempDir(), nil)
					require.NoError(t, journalErr)
					t.Cleanup(func() { unblock(); _ = journal.Close() })
					walWriter = storage.NewWALEngine(gate, journal)
				}
				var target storage.Engine = gate
				if commandMode {
					target = base
				}
				if tc.composite {
					target = storage.NewCompositeEngine(map[string]storage.Engine{"only": gate}, map[string]string{"only": namespace}, map[string]string{"only": "read_write"})
				}
				adapter, err := replication.NewStorageAdapterWithWAL(target, t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { unblock(); _ = adapter.Close() })
				var commandWriter storage.Engine
				if commandMode {
					leader := replication.NewStandaloneReplicator(&replication.Config{NodeID: "origin-command"}, adapter)
					require.NoError(t, leader.Start(ctx))
					t.Cleanup(func() { unblock(); _ = leader.Shutdown() })
					handoff := &originCommandApplyGate{Replicator: leader, entered: gate.entered, release: gate.release}
					commandWriter = replication.NewReplicatedEngine(base, handoff, 5*time.Second)
				}
				delivered := make(chan struct{})
				var deliveredOnce sync.Once
				nodeDelivered := func(node *storage.Node) {
					if node.ID == namespace+":selected" {
						deliveredOnce.Do(func() { close(delivered) })
					}
				}
				edgeDelivered := func(edge *storage.Edge) {
					if edge.ID == namespace+":selected" {
						deliveredOnce.Do(func() { close(delivered) })
					}
				}
				require.NoError(t, storage.SubscribeEvents(base, storage.EventHandlers{NodeCreated: nodeDelivered, NodeUpdated: nodeDelivered, EdgeCreated: edgeDelivered, EdgeUpdated: edgeDelivered}))
				done := make(chan error, 1)
				go func() {
					if commandMode {
						if tc.bulk && tc.edge {
							done <- commandWriter.BulkCreateEdges([]*storage.Edge{walOperation.Edge})
						} else if tc.bulk {
							done <- commandWriter.BulkCreateNodes([]*storage.Node{walOperation.Node})
						} else {
							done <- storage.ApplyEntityChange(commandWriter, walOperation, commandWriter.UpdateNode, commandWriter.DeleteNode)
						}
						return
					}
					if walMode {
						if tc.bulk && tc.edge {
							done <- walWriter.BulkCreateEdges([]*storage.Edge{walOperation.Edge})
						} else if tc.bulk {
							done <- walWriter.BulkCreateNodes([]*storage.Node{walOperation.Node})
						} else {
							done <- storage.ApplyEntityChange(walWriter, walOperation, walWriter.UpdateNode, walWriter.DeleteNode)
						}
						return
					}
					if snapshot {
						done <- adapter.RestoreSnapshot(bytes.NewReader(data))
						return
					}
					done <- adapter.ApplyCommand(&replication.Command{Type: tc.kind, Data: data, Timestamp: time.Now()})
				}()
				select {
				case <-gate.entered:
				case err := <-done:
					require.NoError(t, err, "legacy command rejected before its native write boundary")
					t.Fatal("legacy command skipped its native write boundary")
				case <-time.After(5 * time.Second):
					t.Fatal("legacy native write boundary not reached")
				}
				_, err = exec.Execute(ctx, "DROP INDEX legacy_origin_vec", nil)
				require.NoError(t, err)
				if recreate {
					_, err = exec.Execute(ctx, create, nil)
					require.NoError(t, err)
				}
				unblock()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("legacy apply did not finish")
				}
				select {
				case <-delivered:
				case <-time.After(5 * time.Second):
					t.Fatal("legacy mutation listener did not finish")
				}
				entry := db.searchServices[namespace]
				require.Eventually(t, func() bool {
					entry.pendingFlushMu.Lock()
					running := entry.pendingFlushRunning
					entry.pendingFlushMu.Unlock()
					entry.pendingMu.Lock()
					idle := len(entry.pendingOps) == 0 && len(entry.refreshRetries) == 0
					entry.pendingMu.Unlock()
					return !running && idle
				}, 5*time.Second, time.Millisecond)
				if tc.edge {
					hits, err := svc.VectorQueryRelationships(ctx, query, search.RelationshipVectorQuerySpec{Type: "LINK", Property: "embedding", Limit: 10})
					require.NoError(t, err)
					require.Empty(t, hits, "legacy relationship command rebound admission at the native leaf")
				} else {
					require.Zero(t, originVectorHits(t, svc, query), "legacy command rebound admission at the native leaf")
				}
				if recreate {
					// A later mutation using a freshly read body belongs to the new
					// descriptor. Transient argument metadata must not survive in
					// the native cache and suppress this independent admission.
					if tc.edge {
						fresh, readErr := store.GetEdge("selected")
						require.NoError(t, readErr)
						require.NoError(t, store.UpdateEdge(storage.EdgeUpdateRequest{After: fresh}))
						require.Eventually(t, func() bool {
							hits, queryErr := svc.VectorQueryRelationships(ctx, query, search.RelationshipVectorQuerySpec{Type: "LINK", Property: "embedding", Limit: 10})
							return queryErr == nil && len(hits) == 1
						}, 5*time.Second, time.Millisecond)
					} else {
						fresh, readErr := store.GetNode("selected")
						require.NoError(t, readErr)
						require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: fresh}))
						require.Eventually(t, func() bool { return originVectorHits(t, svc, query) == 1 }, 5*time.Second, time.Millisecond)
					}
				}
			})
		}
	}
}
