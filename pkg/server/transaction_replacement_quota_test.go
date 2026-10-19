package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Keep parser/protocol coverage at the public commit boundary. Hidden MERGE
// replacements are covered separately; SET/REMOVE and both MERGE actions here
// exercise actual mutations, followed by the same operation at exact capacity.
func TestTransactionReplacementRoutesAtQuota(t *testing.T) {
	for _, route := range []string{"http", "bolt", "http-explicit", "bolt-explicit"} {
		for _, buffered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/buffered=%v", route, buffered), func(t *testing.T) {
				h := openHiddenDeleteServer(t, t.TempDir(), buffered, route)
				t.Cleanup(func() { h.shutdown(t) })
				store, err := h.server.dbManager.GetStorage("nornic")
				require.NoError(t, err)
				run := func(query string) {
					t.Helper()
					require.Empty(t, h.run(t, query).code, query)
				}
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
				checkBytes := func() int64 {
					t.Helper()
					var nodeBytes, edgeBytes int64
					measure := func(entity any) int64 {
						var buf bytes.Buffer
						require.NoError(t, gob.NewEncoder(&buf).Encode(entity))
						return int64(buf.Len())
					}
					nodes, err := storage.CollectStoredNodes(context.Background(), store)
					require.NoError(t, err)
					for _, node := range nodes {
						if len(node.Properties) == 0 {
							node.Properties = nil
						}
						nodeBytes += measure(node)
					}
					edges, err := storage.CollectStoredEdges(context.Background(), store)
					require.NoError(t, err)
					for _, edge := range edges {
						if len(edge.Properties) == 0 {
							edge.Properties = nil
						}
						edgeBytes += measure(edge)
					}
					total, gotNodes, gotEdges := h.server.dbManager.GetStorageSize("nornic")
					require.Equal(t, nodeBytes, gotNodes)
					require.Equal(t, edgeBytes, gotEdges)
					require.Equal(t, nodeBytes+edgeBytes, total)
					return total
				}
				for _, entity := range []string{"node", "edge"} {
					for _, operation := range []string{"merge-create", "merge-match", "set", "remove", "delete-recreate"} {
						for _, quota := range []string{"count", "bytes"} {
							t.Run(entity+"/"+operation+"/"+quota, func(t *testing.T) {
								require.NoError(t, h.server.dbManager.SetDatabaseLimits("nornic", nil))
								run("MATCH (n) DETACH DELETE n")
								pattern, target := "(n:Doc {id: 'n'})", "n"
								prefix := ""
								if entity == "edge" {
									run("CREATE (:Source {id: 'a'}), (:Target {id: 'b'})")
									prefix = "MATCH (a:Source {id: 'a'}), (b:Target {id: 'b'}) "
									pattern, target = "(a)-[r:LINK {id: 'e'}]->(b)", "r"
								}
								merge := prefix + "MERGE " + pattern
								run(merge + " ON CREATE SET " + target + ".payload = '" + strings.Repeat("x", 2048) + "'")
								flush()
								// Initialize once from stored data; every later write must
								// maintain the same byte model incrementally.
								checkBytes()
								nodes, err := store.NodeCount()
								require.NoError(t, err)
								edges, err := store.EdgeCount()
								require.NoError(t, err)
								for attempt := 0; attempt < 2; attempt++ {
									before := checkBytes()
									limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: nodes, MaxEdges: edges}}
									if quota == "bytes" {
										limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: before}}
									}
									require.NoError(t, h.server.dbManager.SetDatabaseLimits("nornic", limits))
									query := merge + " ON MATCH SET " + target + ".payload = 'small'"
									switch operation {
									case "merge-create":
										// Visible MERGE takes ON MATCH; CREATE actions must
										// not run or add bytes to the existing body.
										query = merge + " ON CREATE SET " + target + ".payload = 'small'"
									case "set":
										query = prefix + "MATCH " + pattern + " SET " + target + ".payload = 'small'"
									case "remove":
										query = prefix + "MATCH " + pattern + " REMOVE " + target + ".payload"
									case "delete-recreate":
										created, binding := "(m:Doc {id: 'n'})", "m"
										if entity == "edge" {
											created, binding = "(a)-[s:LINK {id: 'e'}]->(b)", "s"
										}
										query = prefix + "MATCH " + pattern + " DELETE " + target + " CREATE " + created + " SET " + binding + ".payload = 'small'"
									}
									run(query)
									flush()
									after := checkBytes()
									require.LessOrEqual(t, after, before)
									if attempt == 1 {
										require.Equal(t, before, after)
									}
									gotNodes, err := store.NodeCount()
									require.NoError(t, err)
									require.Equal(t, nodes, gotNodes)
									gotEdges, err := store.EdgeCount()
									require.NoError(t, err)
									require.Equal(t, edges, gotEdges)
								}
							})
						}
					}
				}
			})
		}
	}
}
