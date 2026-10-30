package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

func fabricLifetimeBolt(t *testing.T, server *Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, server.dbManager)
	done := make(chan error, 1)
	go func() { done <- peer.Serve(listener) }()
	fixtureowner.Own(t, fixtureowner.CloseFunc(func() error { return errors.Join(peer.Close(), <-done) }))
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
		require.NoError(t, coordinator.dbManager.CreateCompositeDatabase(t.Context(), dbName, []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: transport.uri, AuthMode: "oidc_forwarding"}}))
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

// An HTTP transaction idle past the session manager's TTL is never left
// addressable with participants that can no longer complete: either it is
// retired and rolled back, or a later statement and its COMMIT or ROLLBACK
// complete, with local and remote participants.
func TestFabricIdleHTTPTransactionCompletes(t *testing.T) {
	remote, remoteAuth := setupTestServer(t)
	remoteToken := "Bearer " + getAuthToken(t, remoteAuth, "admin")
	remoteRouter := remote.buildRouter()
	remoteHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Authorization", remoteToken)
		remoteRouter.ServeHTTP(w, r)
	}))
	t.Cleanup(remoteHTTP.Close)
	coordinator, authenticator := setupTestServer(t)
	const ttl = 50 * time.Millisecond
	coordinator.txSessions = txsession.NewManager(ttl, coordinator.newExecutorForDatabase)
	localHTTP := httptest.NewServer(coordinator.buildRouter())
	t.Cleanup(localHTTP.Close)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	client := &http.Client{Timeout: 10 * time.Second}
	send := func(t *testing.T, method, endpoint, query string) (TransactionResponse, string) {
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
		var result TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
		return result, response.Header.Get("Location")
	}
	request := func(t *testing.T, method, endpoint, query string) (TransactionResponse, string) {
		t.Helper()
		result, location := send(t, method, endpoint, query)
		require.Empty(t, result.Errors)
		return result, location
	}
	const dbName = "idle_lifetime"
	require.NoError(t, coordinator.dbManager.CreateCompositeDatabase(t.Context(), dbName, []multidb.ConstituentRef{
		{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: remoteHTTP.URL, AuthMode: "oidc_forwarding"},
		{Alias: "l", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
	}))
	for _, participant := range []string{"r", "l"} {
		for _, action := range []string{"commit", "rollback"} {
			t.Run(participant+"/"+action, func(t *testing.T) {
				id := "idle_" + participant + "_" + action
				begin, location := request(t, http.MethodPost, localHTTP.URL+"/db/"+dbName+"/tx", "")
				require.NotEmpty(t, location)
				result, _ := request(t, http.MethodPost, location, fmt.Sprintf("USE %s.%s CREATE (n:IdleLifetime {id:'%s'}) RETURN n.id AS id", dbName, participant, id))
				require.Equal(t, id, result.Results[0].Data[0].Row[0])
				time.Sleep(4 * ttl)
				want := float64(1)
				result, _ = send(t, http.MethodPost, location, fmt.Sprintf("USE %s.%s RETURN 1 AS k", dbName, participant))
				if len(result.Errors) > 0 {
					// Retired: the transaction is gone and its write rolled back.
					require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", result.Errors[0].Code)
					want = 0
				} else {
					require.Equal(t, float64(1), result.Results[0].Data[0].Row[0])
					time.Sleep(4 * ttl)
					method, endpoint := http.MethodPost, begin.Commit
					if action == "rollback" {
						method, endpoint = http.MethodDelete, location
						want = 0
					}
					started := time.Now()
					request(t, method, endpoint, "")
					require.Less(t, time.Since(started), 5*time.Second)
				}
				result, _ = request(t, http.MethodPost, localHTTP.URL+"/db/"+dbName+"/tx/commit", fmt.Sprintf("USE %s.%s MATCH (n:IdleLifetime {id:'%s'}) RETURN count(n) AS c", dbName, participant, id))
				require.Equal(t, want, result.Results[0].Data[0].Row[0])
			})
		}
	}
}

// A unit CALL around a nested unit CALL keeps one row per input row through
// the HTTP and Bolt protocols, in autocommit and explicit transactions.
func TestFabricNestedUnitCallPublic(t *testing.T) {
	ctx := context.Background()
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "nestedone"}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "nestedcomp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "nestedone", Type: "local", AccessMode: "read_write"}}))
	localHTTP := httptest.NewServer(server.buildRouter())
	t.Cleanup(localHTTP.Close)
	localBolt := fabricLifetimeBolt(t, server)
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
		var result TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
		require.Empty(t, result.Errors)
		return result, response.Header.Get("Location")
	}
	driver, err := neo4j.NewDriverWithContext(localBolt, neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(ctx)
	for _, kind := range []string{"CALL", "OPTIONAL CALL"} {
		query := "USE nestedcomp UNWIND [1,2] AS x " + kind + " { WITH x UNWIND [1,2] AS y CALL { WITH x, y USE nestedcomp.one CREATE (:NestedUnitPublic {x: x, y: y}) } } RETURN x ORDER BY x"
		for _, ingress := range []string{"http", "bolt"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/explicit=%v", kind, ingress, explicit), func(t *testing.T) {
					var rows [][]interface{}
					if ingress == "http" {
						endpoint := localHTTP.URL + "/db/nestedcomp/tx/commit"
						if explicit {
							_, location := request(t, http.MethodPost, localHTTP.URL+"/db/nestedcomp/tx", "")
							require.NotEmpty(t, location)
							endpoint = location
							defer request(t, http.MethodDelete, location, "")
						}
						result, _ := request(t, http.MethodPost, endpoint, query)
						require.Len(t, result.Results, 1)
						for _, row := range result.Results[0].Data {
							rows = append(rows, row.Row)
						}
					} else {
						session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nestedcomp"})
						defer session.Close(ctx)
						var result neo4j.ResultWithContext
						if explicit {
							tx, err := session.BeginTransaction(ctx)
							require.NoError(t, err)
							defer tx.Rollback(ctx)
							result, err = tx.Run(ctx, query, nil)
							require.NoError(t, err)
						} else {
							result, err = session.Run(ctx, query, nil)
							require.NoError(t, err)
						}
						records, err := result.Collect(ctx)
						require.NoError(t, err)
						for _, record := range records {
							rows = append(rows, record.Values)
						}
					}
					encoded, err := json.Marshal(rows)
					require.NoError(t, err)
					require.JSONEq(t, "[[1],[2]]", string(encoded))
				})
			}
		}
	}
}
