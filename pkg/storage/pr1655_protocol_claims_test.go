package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/stretchr/testify/require"
)

type pr1655PublicClient struct {
	run           func(string) ([][]interface{}, error)
	begin, commit func() error
	rollback      func()
}

type pr1655HTTPFailure struct{ code, message string }

func (e *pr1655HTTPFailure) Error() string { return e.code + ": " + e.message }
func pr1655PublicStatus(err error) string {
	var wire *pr1655HTTPFailure
	if errors.As(err, &wire) {
		return wire.code
	}
	var boltError *neo4j.Neo4jError
	if errors.As(err, &boltError) {
		return boltError.Code
	}
	code, _ := nornicerrors.Neo4jCommitStatus(err)
	return code
}

// Each adapter drives its real public transaction boundary. HTTP errors below
// are decoded responses, not replacements for storage errors or injected faults.
func pr1655OpenPublicClient(t *testing.T, srv *server.Server, route string) *pr1655PublicClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	switch route {
	case "Execute":
		store, err := srv.GetDatabaseManager().GetStorage("nornic")
		require.NoError(t, err)
		exec := cypher.NewStorageExecutor(store)
		run := func(query string) ([][]interface{}, error) {
			result, err := exec.Execute(ctx, query, nil)
			if err != nil {
				return nil, err
			}
			return result.Rows, nil
		}
		client := &pr1655PublicClient{run: run, begin: func() error { _, err := run("BEGIN"); return err }, commit: func() error { _, err := run("COMMIT"); return err }, rollback: func() { _, _ = run("ROLLBACK") }}
		t.Cleanup(client.rollback)
		return client
	case "Bolt":
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, srv.GetDatabaseManager())
		done := make(chan error, 1)
		go func() { done <- peer.Serve(listener) }()
		t.Cleanup(func() {
			require.NoError(t, peer.Close())
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Error("Bolt Serve did not terminate")
			}
		})
		driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		t.Cleanup(func() { require.NoError(t, session.Close(ctx)) })
		var tx neo4j.ExplicitTransaction
		client := &pr1655PublicClient{}
		client.begin = func() error { var err error; tx, err = session.BeginTransaction(ctx); return err }
		client.run = func(query string) ([][]interface{}, error) {
			var result neo4j.ResultWithContext
			var err error
			if tx != nil {
				result, err = tx.Run(ctx, query, nil)
			} else {
				result, err = session.Run(ctx, query, nil)
			}
			if err != nil {
				return nil, err
			}
			var rows [][]interface{}
			for result.Next(ctx) {
				rows = append(rows, result.Record().Values)
			}
			if err = result.Err(); err != nil {
				return nil, err
			}
			_, err = result.Consume(ctx)
			return rows, err
		}
		client.commit = func() error {
			current := tx
			tx = nil
			if current == nil {
				return errors.New("test client has no open transaction")
			}
			defer current.Close(ctx)
			return current.Commit(ctx)
		}
		client.rollback = func() {
			if tx != nil {
				_ = tx.Rollback(ctx)
				_ = tx.Close(ctx)
				tx = nil
			}
		}
		t.Cleanup(client.rollback)
		return client
	case "HTTP":
		client := &http.Client{Timeout: 15 * time.Second}
		t.Cleanup(client.CloseIdleConnections)
		root := "http://" + srv.Addr() + "/db/nornic/tx"
		openURL := ""
		post := func(url, query string) (server.TransactionResponse, error) {
			statements := []map[string]interface{}{}
			if query != "" {
				statements = append(statements, map[string]interface{}{"statement": query})
			}
			body, err := json.Marshal(map[string]interface{}{"statements": statements})
			if err != nil {
				return server.TransactionResponse{}, err
			}
			response, err := client.Post(url, "application/json", bytes.NewReader(body))
			if err != nil {
				return server.TransactionResponse{}, err
			}
			defer response.Body.Close()
			var decoded server.TransactionResponse
			err = json.NewDecoder(response.Body).Decode(&decoded)
			if err != nil {
				return decoded, err
			}
			if len(decoded.Errors) > 0 {
				return decoded, &pr1655HTTPFailure{decoded.Errors[0].Code, decoded.Errors[0].Message}
			}
			if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusAccepted {
				return decoded, fmt.Errorf("HTTP status %d", response.StatusCode)
			}
			return decoded, nil
		}
		public := &pr1655PublicClient{}
		public.begin = func() error {
			response, err := post(root, "")
			if err == nil {
				openURL = strings.TrimSuffix(response.Commit, "/commit")
				if openURL == "" {
					return errors.New("HTTP BEGIN omitted commit URL")
				}
			}
			return err
		}
		public.run = func(query string) ([][]interface{}, error) {
			url := root + "/commit"
			if openURL != "" {
				url = openURL
			}
			response, err := post(url, query)
			if err != nil {
				return nil, err
			}
			var rows [][]interface{}
			for _, result := range response.Results {
				for _, record := range result.Data {
					rows = append(rows, record.Row)
				}
			}
			return rows, nil
		}
		public.commit = func() error {
			url := openURL
			openURL = ""
			if url == "" {
				return errors.New("test client has no HTTP transaction")
			}
			_, err := post(url+"/commit", "")
			return err
		}
		public.rollback = func() {
			if openURL != "" {
				request, _ := http.NewRequest(http.MethodDelete, openURL, nil)
				response, err := client.Do(request)
				if err == nil {
					response.Body.Close()
				}
				openURL = ""
			}
		}
		t.Cleanup(public.rollback)
		return public
	}
	t.Fatalf("unknown route %s", route)
	return nil
}

func (pr *pr1655PublicClient) statement(query string, explicit bool) ([][]interface{}, error) {
	if explicit {
		if err := pr.begin(); err != nil {
			return nil, err
		}
	}
	rows, err := pr.run(query)
	if explicit {
		if err == nil {
			err = pr.commit()
		} else {
			pr.rollback()
		}
	}
	return rows, err
}

func TestPR1655PublicUniqueClaimLifecycle(t *testing.T) {
	for _, wb := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, route := range []string{"Execute", "Bolt", "HTTP"} {
				t.Run(fmt.Sprintf("wb=%v/explicit=%v/%s", wb, explicit, route), func(t *testing.T) {
					srv, fault := openPR1655ProtocolFixture(t, wb)
					client := pr1655OpenPublicClient(t, srv, route)
					run := func(query string) {
						t.Helper()
						_, err := client.statement(query, explicit)
						require.NoError(t, err, query)
					}
					reject := func(query string) {
						t.Helper()
						_, err := client.statement(query, explicit)
						require.Error(t, err, query)
						require.Equal(t, nornicerrors.ConstraintValidationFailed, pr1655PublicStatus(err), query)
					}
					// Constraint creation must backfill nodes that already exist.
					run("CREATE (:ClaimDoc {id:'old', tag:'a'}), (:ClaimDoc {id:'held', tag:'keeper'})")
					run("CREATE CONSTRAINT claim_id FOR (n:ClaimDoc) REQUIRE n.id IS UNIQUE")
					run("CREATE CONSTRAINT claim_tag FOR (n:ClaimDoc) REQUIRE n.tag IS UNIQUE")
					reject("CREATE (:ClaimDoc:Rejected {id:'old', tag:'rejected'})")
					run("MATCH (n:ClaimDoc {id:'old'}) SET n.id='renamed'")
					run("CREATE (:ClaimDoc {id:'old', tag:'b'})")
					run("MATCH (n:ClaimDoc {tag:'b'}) DETACH DELETE n")
					run("CREATE (:ClaimDoc {id:'old', tag:'c'})")
					run("DROP CONSTRAINT claim_id")
					run("MATCH (n:ClaimDoc {tag:'a'}) SET n.id='after-drop'")
					run("CREATE (:ClaimDoc {id:'renamed', tag:'d'})")
					run("CREATE CONSTRAINT claim_id FOR (n:ClaimDoc) REQUIRE n.id IS UNIQUE")
					for _, value := range []string{"after-drop", "old", "renamed", "held"} {
						reject("CREATE (:ClaimDoc:Rejected {id:'" + value + "', tag:'rejected'})")
					}
					run("DROP CONSTRAINT claim_id")
					reject("CREATE (:ClaimDoc:Rejected {id:'otherwise-free', tag:'a'})")
					run("CREATE CONSTRAINT claim_id FOR (n:ClaimDoc) REQUIRE n.id IS UNIQUE")
					rows, err := client.statement("MATCH (n:ClaimDoc) RETURN n.id,n.tag ORDER BY n.tag", explicit)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"after-drop", "a"}, {"old", "c"}, {"renamed", "d"}, {"held", "keeper"}}, rows)
					rows, err = client.statement("MATCH (n:Rejected) RETURN n", explicit)
					require.NoError(t, err)
					require.Empty(t, rows, "every rejected duplicate is atomic")
					require.False(t, fault.fired.Load(), "no corruption fixture was armed")
					require.NoError(t, fault.engine.FlushWriteBehind(nil))
				})
			}
		}
	}
}

func TestPR1655PublicStagedEndpointDeletion(t *testing.T) {
	for _, wb := range []bool{false, true} {
		for _, route := range []string{"Execute", "Bolt", "HTTP"} {
			for _, mutation := range []string{"CREATE", "MERGE"} {
				t.Run(fmt.Sprintf("wb=%v/%s/%s", wb, route, mutation), func(t *testing.T) {
					srv, fault := openPR1655ProtocolFixture(t, wb)
					client := pr1655OpenPublicClient(t, srv, route)
					_, err := client.run("CREATE (:Endpoint {id:'a'}), (:Endpoint {id:'b'})")
					require.NoError(t, err)
					require.NoError(t, client.begin())
					_, err = client.run("CREATE (:EndpointMarker) WITH 1 AS ignored MATCH (a:Endpoint {id:'a'}),(b:Endpoint {id:'b'}) " + mutation + " (a)-[:ENDPOINT_NEW]->(b)")
					require.NoError(t, err, "writer must actually stage its new relationship")
					store, err := srv.GetDatabaseManager().GetStorage("nornic")
					require.NoError(t, err)
					_, err = cypher.NewStorageExecutor(store).Execute(context.Background(), "MATCH (b:Endpoint {id:'b'}) DETACH DELETE b", nil)
					require.NoError(t, err, "peer deletion must be acknowledged before COMMIT")
					err = client.commit()
					require.Error(t, err)
					require.Equal(t, nornicerrors.TransientOutdated, pr1655PublicStatus(err))
					require.NoError(t, fault.engine.FlushWriteBehind(nil))
					for _, query := range []string{"MATCH (n:EndpointMarker) RETURN n", "MATCH ()-[r:ENDPOINT_NEW]->() RETURN r", "MATCH (n:Endpoint {id:'b'}) RETURN n"} {
						rows, err := client.run(query)
						require.NoError(t, err)
						require.Empty(t, rows, query)
					}
					require.False(t, fault.fired.Load())
				})
			}
		}
	}
}
