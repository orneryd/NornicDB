package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func fabricLifetimeBolt(t *testing.T, server *Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, server.dbManager)
	done := make(chan error, 1)
	go func() { done <- peer.Serve(listener) }()
	t.Cleanup(func() { require.NoError(t, peer.Close()); require.NoError(t, <-done) })
	return "bolt://" + listener.Addr().String()
}

// Each request has ended before the next starts. Exercise real HTTP and Bolt
// coordinators and remote participants, then verify the durable remote outcome.
func TestFabricRemoteTransactionWorkflow(t *testing.T) {
	ctx := context.Background()
	remote, remoteAuth := setupTestServer(t)
	remoteToken := "Bearer " + getAuthToken(t, remoteAuth, "admin")
	remoteRouter := remote.buildRouter()
	remoteHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Authentication is a fixture concern here; exercise completion without
		// depending on credential forwarding between independent user stores.
		r.Header.Set("Authorization", remoteToken)
		remoteRouter.ServeHTTP(w, r)
	}))
	t.Cleanup(remoteHTTP.Close)
	remoteBolt := fabricLifetimeBolt(t, remote)
	coordinator, authenticator := setupTestServer(t)
	localHTTP := httptest.NewServer(coordinator.buildRouter())
	t.Cleanup(localHTTP.Close)
	localBolt := fabricLifetimeBolt(t, coordinator)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	client := &http.Client{Timeout: 10 * time.Second}
	request := func(t *testing.T, method, endpoint, query string) (TransactionResponse, string) {
		t.Helper()
		statements := []map[string]string{}
		if query != "" {
			statements = append(statements, map[string]string{"statement": query})
		}
		payload, err := json.Marshal(map[string]interface{}{"statements": statements})
		require.NoError(t, err)
		req, err := http.NewRequest(method, endpoint, bytes.NewReader(payload))
		require.NoError(t, err)
		req.Header.Set("Authorization", token)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Contains(t, []int{http.StatusOK, http.StatusCreated}, response.StatusCode)
		var result TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
		require.Empty(t, result.Errors)
		return result, response.Header.Get("Location")
	}
	for _, transport := range []struct{ name, uri string }{{"http", remoteHTTP.URL}, {"bolt", remoteBolt}} {
		dbName := "lifetime_" + transport.name
		require.NoError(t, coordinator.dbManager.CreateCompositeDatabase(dbName, []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: transport.uri, AuthMode: "oidc_forwarding"}}))
		for _, ingress := range []string{"http", "bolt"} {
			for _, action := range []string{"commit", "rollback", "implicit"} {
				t.Run(transport.name+"/"+ingress+"/"+action, func(t *testing.T) {
					id := transport.name + "_" + ingress + "_" + action
					query := fmt.Sprintf("USE %s.r CREATE (n:LifetimeWorkflow {id:'%s'}) RETURN n.id AS id", dbName, id)
					if ingress == "http" {
						if action == "implicit" {
							result, _ := request(t, http.MethodPost, localHTTP.URL+"/db/"+dbName+"/tx/commit", query)
							require.Len(t, result.Results[0].Data, 1)
						} else {
							begin, location := request(t, http.MethodPost, localHTTP.URL+"/db/"+dbName+"/tx", "")
							require.NotEmpty(t, location)
							result, _ := request(t, http.MethodPost, location, query)
							require.Equal(t, id, result.Results[0].Data[0].Row[0])
							method, endpoint := http.MethodPost, begin.Commit
							if action == "rollback" {
								method, endpoint = http.MethodDelete, location
							}
							request(t, method, endpoint, "")
						}
					} else {
						driver, err := neo4j.NewDriverWithContext(localBolt, neo4j.NoAuth())
						require.NoError(t, err)
						defer driver.Close(ctx)
						session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: dbName})
						defer session.Close(ctx)
						if action == "implicit" {
							result, err := session.Run(ctx, query, nil)
							require.NoError(t, err)
							records, err := result.Collect(ctx)
							require.NoError(t, err)
							require.Equal(t, id, records[0].Values[0])
						} else {
							tx, err := session.BeginTransaction(ctx)
							require.NoError(t, err)
							defer tx.Close(ctx)
							result, err := tx.Run(ctx, query, nil)
							require.NoError(t, err)
							records, err := result.Collect(ctx)
							require.NoError(t, err)
							require.Equal(t, id, records[0].Values[0])
							if action == "commit" {
								err = tx.Commit(ctx)
							} else {
								err = tx.Rollback(ctx)
							}
							require.NoError(t, err)
						}
					}
					result, _ := request(t, http.MethodPost, remoteHTTP.URL+"/db/nornic/tx/commit", fmt.Sprintf("MATCH (n:LifetimeWorkflow {id:'%s'}) RETURN count(n) AS c", id))
					want := float64(1)
					if action == "rollback" {
						want = 0
					}
					require.Equal(t, want, result.Results[0].Data[0].Row[0])
				})
			}
		}
	}
}
