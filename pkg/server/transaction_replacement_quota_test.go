package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Generate route x quota x visibility x entity x write-behind at the public
// commit boundary. Each cell shrinks/removes a payload, retries unchanged at
// exact capacity, then checks that an insertion/growth is rejected atomically.
func TestTransactionReplacementRoutesAtQuota(t *testing.T) {
	for _, route := range []string{"http", "bolt", "http-explicit", "bolt-explicit"} {
		for _, buffered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/buffered=%v", route, buffered), func(t *testing.T) {
				h := openHiddenDeleteServer(t, t.TempDir(), buffered, route)
				t.Cleanup(func() { h.shutdown(t) })
				// Keep production policy flushing active throughout every cell.
				// TestAccountingWithActivePolicyFlusher also forces its body rewrite
				// at capacity, rather than relying on the scheduler to hit a cell.
				require.NotNil(t, h.db.GetAccessFlusher())
				store, err := h.server.dbManager.GetStorage("nornic")
				require.NoError(t, err)
				flush := func() {
					t.Helper()
					engine := h.db.GetBaseStorageForManager()
					for {
						if base, ok := engine.(*storage.BadgerEngine); ok {
							require.NoError(t, base.FlushWriteBehind())
							return
						}
						wrapper, ok := engine.(storage.EngineUnwrapper)
						require.True(t, ok)
						engine = wrapper.GetInnerEngine()
					}
				}
				base := h.db.GetBaseStorageForManager()
				for {
					if _, ok := base.(*storage.BadgerEngine); ok {
						break
					}
					base = base.(storage.EngineUnwrapper).GetInnerEngine()
				}
				badger := base.(*storage.BadgerEngine)
				schema := badger.GetSchemaForNamespace("nornic")
				for _, entity := range []string{"node", "edge"} {
					scope := knowledgepolicy.ScopeNode
					binding := knowledgepolicy.DecayProfileBinding{Name: "quota_" + entity, ProfileRef: "quota_" + entity, TargetLabels: []string{"QuotaDecayed"}}
					if entity == "edge" {
						scope = knowledgepolicy.ScopeEdge
						binding = knowledgepolicy.DecayProfileBinding{Name: "quota_edge", ProfileRef: "quota_edge", TargetEdgeType: "DECAYED_LINK", IsEdge: true}
					}
					require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "quota_" + entity, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
					require.NoError(t, schema.CreateDecayProfileBinding(binding))
				}
				var policyBytes int64
				checkBytes := func(t *testing.T) int64 {
					t.Helper()
					deadline := time.Now().Add(time.Second)
					for {
						var nodeBytes, edgeBytes int64
						policyBytes = 0
						measure := func(entity any) int64 {
							encoded, err := msgpack.Marshal(entity)
							require.NoError(t, err)
							return int64(len(encoded))
						}
						nodes, err := storage.CollectStoredNodes(context.Background(), store)
						require.NoError(t, err)
						for _, node := range nodes {
							{
								metadata := node.RuntimeMetadata()
								metadata.Database = ""
								node.SetRuntimeMetadata(metadata)
							}
							if len(node.Properties) == 0 {
								node.Properties = nil
							}
							size := measure(node)
							nodeBytes += size
							{
								metadata := node.RuntimeMetadata()
								metadata.VisibilitySuppressed = false
								node.SetRuntimeMetadata(metadata)
							}
							policyBytes += size - measure(node)
						}
						edges, err := storage.CollectStoredEdges(context.Background(), store)
						require.NoError(t, err)
						for _, edge := range edges {
							edge.Database = ""
							if len(edge.Properties) == 0 {
								edge.Properties = nil
							}
							size := measure(edge)
							edgeBytes += size
							edge.VisibilitySuppressed = false
							policyBytes += size - measure(edge)
						}
						total, gotNodes, gotEdges := h.server.dbManager.GetStorageSize(context.Background(), "nornic")
						if nodeBytes != gotNodes || edgeBytes != gotEdges {
							// A stored scan and cache read are separate observations.
							// A live policy publication may fall between them. Retry
							// that sample without stopping or replacing the worker;
							// a lost delta remains different and fails at the deadline.
							if time.Now().Before(deadline) {
								time.Sleep(time.Millisecond)
								continue
							}
						}
						require.Equal(t, nodeBytes, gotNodes)
						require.Equal(t, edgeBytes, gotEdges)
						require.Equal(t, nodeBytes+edgeBytes, total)
						return total
					}
				}
				for _, visibility := range []string{"visible", "suppressed", "decayed"} {
					for _, entity := range []string{"node", "edge"} {
						for _, operation := range []string{"merge-create", "merge-match", "set", "remove", "direct-update", "delete-recreate"} {
							for _, quota := range []string{"count", "bytes"} {
								t.Run(visibility+"/"+entity+"/"+operation+"/"+quota, func(t *testing.T) {
									require.NoError(t, h.server.dbManager.SetDatabaseLimits(t.Context(), "nornic", nil))
									request := func(query string) hiddenDeleteOutcome {
										t.Helper()
										deadline := time.Now().Add(time.Second)
										for {
											prior := quotaStoredBodies(t, store)
											outcome := h.run(t, query)
											if outcome.code != "Neo.TransientError.Transaction.Outdated" || !time.Now().Before(deadline) {
												return outcome
											}
											// The live worker can invalidate an explicit snapshot.
											// Prove that known rollback is atomic before retrying;
											// every actual policy-field byte is still checked.
											require.Equal(t, prior, quotaStoredBodies(t, store), "conflicted request changed stored bodies: %s", query)
											checkBytes(t)
											time.Sleep(time.Millisecond)
										}
									}
									run := func(query string) {
										t.Helper()
										require.Empty(t, request(query).code, query)
									}
									run("MATCH (n) WHERE reveal(n) IS NOT NULL DETACH DELETE n")
									pattern, target := "(n:Doc {id: 'n'})", "n"
									if entity == "node" && visibility == "decayed" {
										pattern = "(n:Doc:QuotaDecayed {id: 'n'})"
									}
									prefix := ""
									if entity == "edge" {
										run("CREATE (:Source {id: 'a'}), (:Target {id: 'b'})")
										prefix = "MATCH (a:Source {id: 'a'}), (b:Target {id: 'b'}) "
										pattern, target = "(a)-[r:LINK {id: 'e'}]->(b)", "r"
										if visibility == "decayed" {
											pattern = "(a)-[r:DECAYED_LINK {id: 'e'}]->(b)"
										}
									}
									merge := prefix + "MERGE " + pattern
									run(merge + " ON CREATE SET " + target + ".payload = '" + strings.Repeat("x", 2048) + "'")
									flush()
									old := time.Now().Add(-72 * time.Hour)
									if entity == "node" {
										nodes, err := storage.GetStoredNodesByLabel(store, "Doc")
										require.NoError(t, err)
										require.Len(t, nodes, 1)
										nodes[0].CreatedAt, nodes[0].UpdatedAt = old, old
										require.NoError(t, store.UpdateNode(nodes[0]))
									} else {
										edges, err := storage.CollectStoredEdges(context.Background(), store)
										require.NoError(t, err)
										require.Len(t, edges, 1)
										edges[0].CreatedAt, edges[0].UpdatedAt = old, old
										require.NoError(t, store.UpdateEdge(edges[0]))
									}
									if visibility == "suppressed" {
										run(prefix + "MATCH " + pattern + " CALL kp.suppress(" + target + ")")
									}
									flush()
									visible := int64(1)
									if visibility != "visible" {
										visible = 0
									}
									require.Equal(t, [][]interface{}{{visible}}, h.run(t, prefix+"MATCH "+pattern+" RETURN count("+target+")").rows)
									// Initialize once from stored data; every later write must
									// maintain the same byte model incrementally.
									checkBytes(t)
									nodes, err := store.NodeCount()
									require.NoError(t, err)
									edges, err := store.EdgeCount()
									require.NoError(t, err)
									for attempt := 0; attempt < 2; attempt++ {
										before := checkBytes(t)
										beforePolicy := policyBytes
										limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: nodes, MaxEdges: edges}}
										if quota == "bytes" {
											limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: before}}
										}
										require.NoError(t, h.server.dbManager.SetDatabaseLimits(t.Context(), "nornic", limits))
										query := merge + " ON MATCH SET " + target + ".payload = 'small' RETURN reveal(" + target + ")"
										switch operation {
										case "merge-create":
											// Keep ordinary visibility: a hidden edge replaces its
											// deterministic ID; a hidden node gets a new ID.
											query = merge + " ON CREATE SET " + target + ".payload = 'small'"
										case "set":
											query = prefix + "MATCH " + pattern + " SET " + target + ".payload = 'small' RETURN reveal(" + target + ")"
										case "remove":
											query = prefix + "MATCH " + pattern + " REMOVE " + target + ".payload RETURN reveal(" + target + ")"
										case "delete-recreate":
											created, binding := "(m:Doc {id: 'n'})", "m"
											if visibility == "decayed" {
												created = "(m:Doc:QuotaDecayed {id: 'n'})"
											}
											if entity == "edge" {
												created, binding = "(a)-[s:LINK {id: 'e'}]->(b)", "s"
												if visibility == "decayed" {
													created = "(a)-[s:DECAYED_LINK {id: 'e'}]->(b)"
												}
											}
											query = prefix + "MATCH " + pattern + " DELETE " + target + " CREATE " + created + " SET " + binding + ".payload = 'small' RETURN reveal(" + binding + ")"
										}
										if operation == "direct-update" {
											require.NoError(t, updateQuotaPayload(store, entity, "small"))
										} else if visibility != "visible" && operation == "merge-create" {
											// Hidden nodes require a new physical identity; a different
											// same-ID hidden edge CREATE is a permanent identity conflict.
											require.NotEmpty(t, request(query).code)
										} else {
											run(query)
										}
										flush()
										after := checkBytes(t)
										wantPayload := any("small")
										if operation == "remove" {
											wantPayload = nil
										} else if operation == "merge-create" {
											wantPayload = strings.Repeat("x", 2048)
										}
										if entity == "node" {
											stored, err := storage.GetStoredNodesByLabel(store, "Doc")
											require.NoError(t, err)
											require.Len(t, stored, 1)
											require.Equal(t, wantPayload, stored[0].Properties["payload"])
										} else {
											stored, err := storage.CollectStoredEdges(context.Background(), store)
											require.NoError(t, err)
											require.Len(t, stored, 1)
											require.Equal(t, wantPayload, stored[0].Properties["payload"])
										}
										// The active policy worker may independently toggle the
										// persisted flag while this request runs. checkBytes
										// compares ALL actual bytes with the cache; only this
										// payload comparison removes the policy field's size.
										require.LessOrEqual(t, after-policyBytes, before-beforePolicy)
										if attempt == 1 {
											require.Equal(t, before-beforePolicy, after-policyBytes)
										}
										gotNodes, err := store.NodeCount()
										require.NoError(t, err)
										require.Equal(t, nodes, gotNodes)
										gotEdges, err := store.EdgeCount()
										require.NoError(t, err)
										require.Equal(t, edges, gotEdges)
									}
									before := checkBytes(t)
									beforePolicy := policyBytes
									if quota == "bytes" {
										require.NoError(t, h.server.dbManager.SetDatabaseLimits(t.Context(), "nornic", &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: before}}))
										require.ErrorIs(t, updateQuotaPayload(store, entity, strings.Repeat("x", 8192)), multidb.ErrStorageLimitExceeded)
									} else if entity == "node" {
										require.ErrorIs(t, store.UpdateNode(&storage.Node{ID: "overflow"}), multidb.ErrStorageLimitExceeded)
									} else {
										edges, err := storage.CollectStoredEdges(context.Background(), store)
										require.NoError(t, err)
										require.ErrorIs(t, store.CreateEdge(&storage.Edge{ID: "overflow", StartNode: edges[0].StartNode, EndNode: edges[0].EndNode, Type: "LINK"}), multidb.ErrStorageLimitExceeded)
									}
									flush()
									after := checkBytes(t)
									require.Equal(t, before-beforePolicy, after-policyBytes, "rejection publishes no usage")
								})
							}
						}
					}
				}
			})
		}
	}
}

// Only the worker's independently maintained visibility flag may change while
// a request rolls back. Keep every identity, property, timestamp and sidecar in
// this comparison; checkBytes separately checks the actual flag bytes as well.
func quotaStoredBodies(t *testing.T, store storage.Engine) storage.TransactionChanges {
	t.Helper()
	nodes, err := storage.CollectStoredNodes(context.Background(), store)
	require.NoError(t, err)
	edges, err := storage.CollectStoredEdges(context.Background(), store)
	require.NoError(t, err)
	result := storage.TransactionChanges{Nodes: make(map[storage.NodeID]*storage.Node), Edges: make(map[storage.EdgeID]*storage.Edge)}
	for _, node := range nodes {
		{
			metadata := node.RuntimeMetadata()
			metadata.VisibilitySuppressed = false
			node.SetRuntimeMetadata(metadata)
		}
		result.Nodes[node.ID] = node
	}
	for _, edge := range edges {
		edge.VisibilitySuppressed = false
		result.Edges[edge.ID] = edge
	}
	return result
}

func updateQuotaPayload(store storage.Engine, entity, payload string) error {
	if entity == "node" {
		nodes, err := storage.GetStoredNodesByLabel(store, "Doc")
		if err != nil {
			return err
		}
		if len(nodes) != 1 {
			return storage.ErrNotFound
		}
		if nodes[0].Properties == nil {
			nodes[0].Properties = make(map[string]any)
		}
		nodes[0].Properties["payload"] = payload
		return store.UpdateNode(nodes[0])
	}
	edges, err := storage.CollectStoredEdges(context.Background(), store)
	if err != nil {
		return err
	}
	if len(edges) != 1 {
		return storage.ErrNotFound
	}
	if edges[0].Properties == nil {
		edges[0].Properties = make(map[string]any)
	}
	edges[0].Properties["payload"] = payload
	return store.UpdateEdge(edges[0])
}
