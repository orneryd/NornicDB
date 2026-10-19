package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// MERGE cannot see the suppressed relationship, so it stages a same-ID CREATE
// replacement. Admission must subtract its stored prior body, just as for an
// explicit update, at both the physical edge-count and logical byte capacity.
func TestSuppressedRelationshipReMergeAtQuota(t *testing.T) {
	for _, quota := range []string{"edges", "bytes"} {
		for _, route := range []string{"bolt", "http", "bolt-explicit", "http-explicit"} {
			for _, buffered := range []bool{false, true} {
				mode := "synchronous"
				if buffered {
					mode = "buffered"
				}
				t.Run(quota+"/"+route+"/"+mode, func(t *testing.T) {
					h := openHiddenDeleteServer(t, t.TempDir(), buffered, route)
					t.Cleanup(func() { h.shutdown(t) })
					run := func(query string) hiddenDeleteOutcome {
						t.Helper()
						outcome := h.run(t, query)
						require.Empty(t, outcome.code, query)
						return outcome
					}
					flush := func() {
						t.Helper()
						engine := h.db.GetBaseStorageForManager()
						for {
							if badger, ok := engine.(*storage.BadgerEngine); ok {
								require.NoError(t, badger.FlushWriteBehind())
								return
							}
							wrapper, ok := engine.(storage.EngineUnwrapper)
							require.True(t, ok)
							engine = wrapper.GetInnerEngine()
						}
					}
					store, err := h.server.dbManager.GetStorage("nornic")
					require.NoError(t, err)
					storedEdge := func() *storage.Edge {
						t.Helper()
						edges, err := storage.CollectStoredEdges(context.Background(), store)
						require.NoError(t, err)
						require.Len(t, edges, 1, "replacement must not add a physical record")
						return edges[0]
					}
					logicalBytes := func(edge *storage.Edge) int64 {
						t.Helper()
						// Quota bytes are the documented gob logical size, not the
						// physical Badger size. Measure stored bodies independently
						// of the incrementally maintained manager counters.
						var encoded bytes.Buffer
						require.NoError(t, gob.NewEncoder(&encoded).Encode(edge))
						return int64(encoded.Len())
					}
					run("CREATE (:Source {id: 'a'}), (:Target {id: 'b'})")
					const merge = "MATCH (a:Source {id: 'a'}), (b:Target {id: 'b'}) MERGE (a)-[r:INGEST {id: 'same'}]->(b)"
					run(merge + " ON CREATE SET r.payload = '" + strings.Repeat("x", 2048) + "'")
					flush()
					run("MATCH ()-[r:INGEST]->() CALL kp.suppress(r)")
					flush()
					initial := storedEdge()
					require.Equal(t, [][]interface{}{{int64(0)}}, run("MATCH ()-[r:INGEST]->() RETURN count(r)").rows)
					_, err = store.GetEdge(initial.ID)
					require.ErrorIs(t, err, storage.ErrNotFound, "prior body is physically present and suppressed")
					// First replace with a smaller body, then replace it unchanged.
					// Reset byte capacity to the current exact usage each time.
					for attempt := 0; attempt < 2; attempt++ {
						prior := storedEdge()
						before, beforeNodes, beforeEdges := h.server.dbManager.GetStorageSize("nornic")
						require.Positive(t, beforeEdges, "byte accounting must be initialized")
						limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxEdges: 1}}
						if quota == "bytes" {
							limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: before}}
						}
						require.NoError(t, h.server.dbManager.SetDatabaseLimits("nornic", limits))
						run(merge + " ON CREATE SET r.payload = 'small'")
						flush()
						after := storedEdge()
						require.Equal(t, initial.ID, after.ID, "same stored relationship ID")
						require.Equal(t, initial.StartNode, after.StartNode, "unchanged start")
						require.Equal(t, initial.EndNode, after.EndNode, "unchanged end")
						require.Equal(t, "small", after.Properties["payload"], "hidden edge took the CREATE replacement path")
						count, err := store.EdgeCount()
						require.NoError(t, err)
						require.EqualValues(t, 1, count)
						total, nodes, edges := h.server.dbManager.GetStorageSize("nornic")
						delta := logicalBytes(after) - logicalBytes(prior)
						require.Equal(t, beforeNodes, nodes)
						require.Equal(t, beforeEdges+delta, edges, "subtract the hidden prior bytes")
						require.Equal(t, before+delta, total)
						require.LessOrEqual(t, total, before, "same-ID replacement must fit at capacity")
						require.Equal(t, [][]interface{}{{int64(0)}}, run("MATCH ()-[r:INGEST]->() RETURN count(r)").rows)
					}
				})
			}
		}
	}
}
