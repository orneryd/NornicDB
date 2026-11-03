// Author: Codex, NOR-180 protocol aggregation contract, 29 October 2026.
package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	gql "github.com/99designs/gqlgen/graphql"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// The managed database owner discovers this supported admission capability on
// its authoritative submitter,after the logical BEGIN and before native commit.
// The test closes only the independent logical WAL;native publication is real.
type walProtocolAggregationEngine struct {
	*walterminal.Engine
	commitAdmission func() error
}

func (e *walProtocolAggregationEngine) CheckEntityCommitAdmission() error {
	if e.commitAdmission != nil {
		return e.commitAdmission()
	}
	return nil
}

func TestGraphQLWALTerminalProtocolAggregation(t *testing.T) {
	cases := []struct {
		name, query, countQuery string
		wantCount, wantTotal    int64
	}{
		{"create_nodes", `mutation { bulkCreateNodes(input:{nodes:[{labels:["WalWire"],properties:{id:"new"}}]}) { created skipped errors } }`, "MATCH (n:WalWire) RETURN count(n)", 4, 1},
		{"delete_nodes", `mutation { bulkDeleteNodes(ids:["victim"]) { deleted notFound } }`, "MATCH (n:WalWire) RETURN count(n)", 2, 1},
		{"create_relationships", `mutation { bulkCreateRelationships(input:{relationships:[{startNodeId:"source",endNodeId:"target",type:"WAL_EDGE",properties:{id:"new-edge"}}]}) { created skipped errors } }`, "MATCH ()-[r:WAL_EDGE]->() RETURN count(r)", 2, 1},
		{"delete_relationships", `mutation { bulkDeleteRelationships(ids:["victim-edge"]) { deleted notFound } }`, "MATCH ()-[r:WAL_EDGE]->() RETURN count(r)", 0, 1},
	}
	for _, tc := range cases {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed=%v", tc.name, closed), func(t *testing.T) {
				engine := &walProtocolAggregationEngine{Engine: walterminal.New(t)}
				exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
				_, err := exec.Execute(context.Background(), "CREATE (:WalWire {id:'source'}),(:WalWire {id:'target'}),(:WalWire {id:'victim'})", nil)
				require.NoError(t, err)
				_, err = exec.Execute(context.Background(), "MATCH (a:WalWire {id:'source'}),(b:WalWire {id:'target'}) CREATE (a)-[:WAL_EDGE {id:'victim-edge'}]->(b)", nil)
				require.NoError(t, err)
				manager, err := multidb.NewDatabaseManager(context.Background(), engine, nil)
				require.NoError(t, err)
				var seamCalls atomic.Int64
				var once sync.Once
				var faultErr error
				engine.commitAdmission = func() error {
					once.Do(func() {
						seamCalls.Add(1)
						if closed {
							faultErr = engine.WAL.Close()
						}
					})
					return faultErr
				}
				h := newHandler(nil, manager, nil)
				var presented *gqlerror.Error
				h.server.AroundResponses(func(ctx context.Context, next gql.ResponseHandler) *gql.Response {
					response := next(ctx)
					if len(response.Errors) > 0 {
						presented = response.Errors[0]
					}
					return response
				})
				payload, err := json.Marshal(map[string]string{"query": tc.query})
				require.NoError(t, err)
				request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(payload))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				h.ServeHTTP(response, request)
				require.Equal(t, http.StatusOK, response.Code)
				// Verify native truth before the response assertion: a failed
				// logical marker cannot turn this committed mutation into rollback.
				read := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine.MemoryEngine, "nornic"))
				result, err := read.Execute(context.Background(), tc.countQuery, nil)
				require.NoError(t, err)
				require.Equal(t, tc.wantCount, result.Rows[0][0])
				require.EqualValues(t, 1, seamCalls.Load(), "managed admission seam actually ran in both fault/control modes")
				var wire struct {
					Errors []struct{ Message string } `json:"errors"`
					Data   map[string]struct {
						Created, Skipped, Deleted int64
						Errors, NotFound          []string
					} `json:"data"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &wire))
				if closed {
					require.NotEmpty(t, wire.Errors, "a terminal failure cannot be reduced to success/count/not-found")
					require.ErrorIs(t, presented, storage.ErrWALClosed)
					walterminal.RequireCommitMarkerFailure(t, wire.Errors[0].Message)
					return
				}
				require.Empty(t, wire.Errors)
				require.Len(t, wire.Data, 1)
				for _, value := range wire.Data {
					require.Equal(t, tc.wantTotal, value.Created+value.Deleted)
					require.Zero(t, value.Skipped)
					require.Empty(t, value.Errors)
					require.Empty(t, value.NotFound)
				}
			})
		}
	}
}
