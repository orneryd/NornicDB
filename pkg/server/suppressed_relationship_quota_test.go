package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A MERGE cannot see a suppressed relationship. Repeating its exact stored
// identity is nevertheless an idempotent storage CREATE when the final image,
// including ON CREATE SET, matches the stored body. It succeeds without
// becoming visible, consuming quota or publishing CREATE/SET stats. A final
// image with different properties is a permanent client error. Timestamps and
// derived visibility bookkeeping are not identity because each retry produces
// fresh timestamps and suppression belongs to the stored record, not the
// caller's graph image.
func TestSuppressedRelationshipIdenticalReMergeIsNoOpAtQuota(t *testing.T) {
	previousParser := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previousParser) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, quota := range []string{"edges", "bytes"} {
			for _, route := range []string{"bolt", "http"} {
				for _, buffered := range []bool{false, true} {
					mode := "sync"
					if buffered {
						mode = "write-behind"
					}
					t.Run(parser+"/"+quota+"/"+route+"/"+mode, func(t *testing.T) {
						config.SetParserType(parser)
						dir := t.TempDir()
						h := openHiddenDeleteServer(t, dir, buffered, route)
						t.Cleanup(func() {
							if h != nil {
								h.shutdown(t)
							}
						})
						storedEdge := func() *storage.Edge {
							t.Helper()
							store, err := h.server.dbManager.GetStorage("nornic")
							require.NoError(t, err)
							edges, err := storage.CollectStoredEdges(context.Background(), store)
							require.NoError(t, err)
							require.Len(t, edges, 1, "no second physical relationship")
							return edges[0]
						}

						h.requireSuccess(t, "CREATE (:Source {id: 'a'}), (:Target {id: 'b'})")
						const merge = "MATCH (a:Source {id: 'a'}), (b:Target {id: 'b'}) MERGE (a)-[r:INGEST {id: 'same'}]->(b)"
						const identical = merge + " ON CREATE SET r.payload = 'stable'"
						created := h.requireSuccess(t, identical)
						require.Equal(t, 1, created.relationshipsCreated)
						h.flush(t)
						h.requireSuccess(t, "MATCH ()-[r:INGEST]->() CALL kp.suppress(r)")
						h.flush(t)
						beforeBody := storedEdge()
						store, err := h.server.dbManager.GetStorage("nornic")
						require.NoError(t, err)
						_, err = store.GetEdge(beforeBody.ID)
						require.ErrorIs(t, err, storage.ErrNotFound, "the authoritative visibility state hides the stored relationship")
						beforeTotal, beforeNodes, beforeEdges := h.server.dbManager.GetStorageSize(context.Background(), "nornic")
						require.Positive(t, beforeEdges)
						limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxEdges: 1}}
						if quota == "bytes" {
							limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: beforeTotal}}
						}
						require.NoError(t, h.server.dbManager.SetDatabaseLimits(t.Context(), "nornic", limits))

						noOp := h.requireSuccess(t, identical)
						require.Zero(t, noOp.relationshipsCreated)
						require.Zero(t, noOp.propertiesSet, "an identical final image publishes no SET work")
						h.flush(t)
						require.Equal(t, beforeBody, storedEdge(), "the authoritative stored body and suppression survive")
						total, nodes, edges := h.server.dbManager.GetStorageSize(context.Background(), "nornic")
						require.Equal(t, beforeTotal, total)
						require.Equal(t, beforeNodes, nodes)
						require.Equal(t, beforeEdges, edges)
						require.Equal(t, [][]interface{}{{int64(0)}}, h.requireSuccess(t, "MATCH ()-[r:INGEST]->() RETURN count(r)").rows)
						require.Equal(t, [][]interface{}{{"same", "stable"}}, h.requireSuccess(t, "MATCH ()-[r:INGEST]->() WHERE reveal(r) IS NOT NULL RETURN r.id, r.payload").rows,
							"type/property/search-facing reads retain the original indexed image")

						different := h.run(t, merge+" ON CREATE SET r.payload = 'different'")
						require.True(t, strings.HasPrefix(different.code, "Neo.ClientError."), "a different final image is one permanent client error: %#v", different)
						h.flush(t)
						require.Equal(t, beforeBody, storedEdge(), "the rejected final image publishes nothing")

						fresh := h.run(t, "MATCH (a:Source {id: 'a'}), (b:Target {id: 'b'}) MERGE (a)-[:INGEST {id: 'fresh'}]->(b)")
						require.NotEmpty(t, fresh.code, "a distinct relationship still consumes quota")
						require.Equal(t, beforeBody, storedEdge())

						h.shutdown(t)
						h = openHiddenDeleteServer(t, dir, buffered, route)
						require.Equal(t, beforeBody, storedEdge(), "reopen keeps the no-op post-image")
						require.Equal(t, [][]interface{}{{int64(0)}}, h.requireSuccess(t, "MATCH ()-[r:INGEST]->() RETURN count(r)").rows)
					})
				}
			}
		}
	}
}

func TestSuppressedRelationshipRepeatedMergeIsNoOp(t *testing.T) {
	previousParser := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previousParser) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, route := range []string{"bolt", "http"} {
			for _, buffered := range []bool{false, true} {
				for _, payload := range []string{"'stored'", "['stored', 'x']"} {
					t.Run(fmt.Sprintf("%s/%s/write-behind=%v/payload=%s", parser, route, buffered, payload), func(t *testing.T) {
						config.SetParserType(parser)
						h := openHiddenDeleteServer(t, t.TempDir(), buffered, route)
						t.Cleanup(func() { h.shutdown(t) })

						h.requireSuccess(t, "CREATE (:A {id:'a'}), (:B {id:'b'})")
						const merge = "MATCH (a:A), (b:B) MERGE (a)-[r:LINK {key:'same'}]->(b)"
						h.requireSuccess(t, merge+" ON CREATE SET r.payload="+payload)
						h.flush(t)
						h.requireSuccess(t, "MATCH ()-[r:LINK]->() CALL kp.suppress(r)")
						h.flush(t)
						store, err := h.server.dbManager.GetStorage("nornic")
						require.NoError(t, err)
						before, err := storage.CollectStoredEdges(context.Background(), store)
						require.NoError(t, err)
						require.Len(t, before, 1)

						outcome := h.requireSuccess(t, "UNWIND [1,2] AS i "+merge+
							" ON CREATE SET r.payload="+payload+" ON MATCH SET r.payload='MATCH-FIRED'")
						require.Zero(t, outcome.relationshipsCreated)
						require.Zero(t, outcome.propertiesSet, "the second row must not treat the hidden candidate as an ON MATCH target")
						h.flush(t)
						after, err := storage.CollectStoredEdges(context.Background(), store)
						require.NoError(t, err)
						require.Len(t, after, 1)
						require.Equal(t, before[0].Properties, after[0].Properties)
						_, err = store.GetEdge(before[0].ID)
						require.ErrorIs(t, err, storage.ErrNotFound)
					})
				}
			}
		}
	}
}
