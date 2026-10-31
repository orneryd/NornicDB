package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hiddenDeleteOutcome is one statement's answer over either protocol.
type hiddenDeleteOutcome struct {
	rows                 [][]interface{}
	nodesDeleted         int
	relationshipsDeleted int
	relationshipsCreated int
	propertiesSet        int
	code                 string
}

// hiddenDeleteServer is a server over a database directory that survives a
// restart, run through Bolt or HTTP.
type hiddenDeleteServer struct {
	db     *nornicdb.DB
	server *Server
	run    func(t *testing.T, statement string) hiddenDeleteOutcome
	close  func()
}

func openHiddenDeleteServer(t *testing.T, dir string, writeBehind bool, route string) *hiddenDeleteServer {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = true
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = writeBehind
	db, err := nornicdb.Open(dir, config)
	require.NoError(t, err)
	require.Equal(t, writeBehind, db.IsAsyncWritesEnabled())

	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	require.NoError(t, err)
	if _, err := authenticator.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin}); err != nil {
		require.ErrorIs(t, err, auth.ErrUserExists)
	}
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	server, err := New(db, authenticator, serverConfig)
	require.NoError(t, err)

	h := &hiddenDeleteServer{db: db, server: server}
	switch route {
	case "http", "http-explicit":
		token := "Bearer " + getAuthToken(t, authenticator, "admin")
		h.run = func(t *testing.T, statement string) hiddenDeleteOutcome {
			t.Helper()
			endpoint := "/db/nornic/tx/commit"
			if route == "http-explicit" {
				opened := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []any{}}, token)
				var transaction TransactionResponse
				require.NoError(t, json.Unmarshal(opened.Body.Bytes(), &transaction))
				require.Empty(t, transaction.Errors)
				require.NotEmpty(t, transaction.Commit)
				endpoint = transaction.Commit
			}
			recorder := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{
				"statements": []map[string]any{{"statement": statement, "includeStats": true}},
			}, token)
			require.Contains(t, []int{http.StatusOK, http.StatusAccepted}, recorder.Code, recorder.Body.String())
			var response TransactionResponse
			require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
			if len(response.Errors) > 0 {
				return hiddenDeleteOutcome{code: response.Errors[0].Code}
			}
			var outcome hiddenDeleteOutcome
			for _, row := range response.Results[0].Data {
				values := make([]interface{}, len(row.Row))
				for i, value := range row.Row {
					// JSON numbers decode as float64; Bolt returns integers.
					if number, isFloat := value.(float64); isFloat && number == float64(int64(number)) {
						value = int64(number)
					}
					values[i] = value
				}
				outcome.rows = append(outcome.rows, values)
			}
			if stats := response.Results[0].Stats; stats != nil {
				outcome.nodesDeleted, outcome.relationshipsDeleted = stats.NodesDeleted, stats.RelationshipsDeleted
				outcome.relationshipsCreated, outcome.propertiesSet = stats.RelationshipsCreated, stats.PropertiesSet
			}
			return outcome
		}
		h.close = func() {}
	case "bolt", "bolt-explicit":
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		boltServer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, server.dbManager)
		served := make(chan error, 1)
		go func() { served <- boltServer.Serve(listener) }()
		driver, err := neo4jdriver.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4jdriver.NoAuth())
		require.NoError(t, err)
		require.NoError(t, driver.VerifyConnectivity(context.Background()))
		h.run = func(t *testing.T, statement string) hiddenDeleteOutcome {
			t.Helper()
			ctx := context.Background()
			session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
			defer session.Close(ctx)
			var explicit neo4jdriver.ExplicitTransaction
			var result neo4jdriver.ResultWithContext
			var err error
			if route == "bolt-explicit" {
				explicit, err = session.BeginTransaction(ctx)
				require.NoError(t, err)
				defer explicit.Close(ctx)
				result, err = explicit.Run(ctx, statement, nil)
			} else {
				result, err = session.Run(ctx, statement, nil)
			}
			var outcome hiddenDeleteOutcome
			if err == nil {
				for result.Next(ctx) {
					outcome.rows = append(outcome.rows, result.Record().Values)
				}
				var summary neo4jdriver.ResultSummary
				if summary, err = result.Consume(ctx); err == nil {
					outcome.nodesDeleted = summary.Counters().NodesDeleted()
					outcome.relationshipsDeleted = summary.Counters().RelationshipsDeleted()
					outcome.relationshipsCreated = summary.Counters().RelationshipsCreated()
					outcome.propertiesSet = summary.Counters().PropertiesSet()
				}
			}
			if err == nil && explicit != nil {
				err = explicit.Commit(ctx)
			}
			if err != nil {
				var status *neo4jdriver.Neo4jError
				require.True(t, errors.As(err, &status), "%s: %v", statement, err)
				outcome.code = status.Code
			}
			return outcome
		}
		h.close = func() {
			require.NoError(t, driver.Close(context.Background()))
			require.NoError(t, boltServer.Close())
			require.NoError(t, <-served)
		}
	}
	return h
}

func (h *hiddenDeleteServer) shutdown(t *testing.T) {
	t.Helper()
	h.close()
	stopTestServer(t, h.server)
	require.NoError(t, h.db.Close())
}

// requireSuccess executes the public protocol request and asserts its status.
func (h *hiddenDeleteServer) requireSuccess(t *testing.T, statement string) hiddenDeleteOutcome {
	t.Helper()
	outcome := h.run(t, statement)
	require.Empty(t, outcome.code, statement)
	return outcome
}

// flush drains the fixture's physical write-behind owner, regardless of wrappers.
func (h *hiddenDeleteServer) flush(t *testing.T) {
	t.Helper()
	engine := h.db.GetBaseStorageForManager()
	for {
		if badger, ok := engine.(*storage.BadgerEngine); ok {
			require.NoError(t, badger.FlushWriteBehind(nil))
			return
		}
		wrapper, ok := engine.(storage.EngineUnwrapper)
		require.True(t, ok, "database must expose its physical engine")
		engine = wrapper.GetInnerEngine()
	}
}

// Deleting a node removes the relationships the visibility layer hides from
// reads (on-demand suppression, knowledge-policy decay) with it, through Bolt
// and HTTP, with write-behind on and off, and nothing of them survives a
// restart: DETACH DELETE counts them, and a non-DETACH DELETE of a node whose
// only relationship is hidden fails as Neo4j's DeleteConnectedNode does for
// any relationship.
func TestDeleteNodeWithHiddenRelationships(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, route := range []string{"bolt", "http"} {
			name := route + "/write-behind off"
			if writeBehind {
				name = route + "/write-behind on"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				h := openHiddenDeleteServer(t, dir, writeBehind, route)
				t.Cleanup(func() {
					if h != nil {
						h.shutdown(t)
					}
				})
				column := func(t *testing.T, statement string) []interface{} {
					t.Helper()
					var values []interface{}
					for _, row := range h.requireSuccess(t, statement).rows {
						values = append(values, row[0])
					}
					return values
				}

				h.requireSuccess(t, "CREATE DECAY PROFILE stale OPTIONS {halfLifeSeconds: 1, function: 'exponential', scope: 'EDGE', scoreFrom: 'CREATED', visibilityThreshold: 1.0}")
				h.requireSuccess(t, "CREATE DECAY PROFILE stale_edges FOR ()-[r:STALE]-() APPLY {DECAY PROFILE 'stale'}")
				h.requireSuccess(t, "CREATE (h:Hub {id: 'h'}), (o:Hub {id: 'o'}), (p:Peer {id: 'p'}), (q:Peer {id: 'q'}), "+
					"(h)-[:HIDDEN {id: 's'}]->(p), (q)-[:STALE {id: 'd'}]->(h), (h)-[:SEEN {id: 'v'}]->(q), (o)-[:HIDDEN {id: 's2'}]->(p)")
				// kp.suppress marks the stored record, which write-behind lands
				// at its next flush.
				for deadline := time.Now().Add(10 * time.Second); h.run(t, "MATCH ()-[r:HIDDEN]->() CALL kp.suppress(r)").code != ""; {
					require.True(t, time.Now().Before(deadline), "kp.suppress never found the created relationships")
					time.Sleep(50 * time.Millisecond)
				}
				// Any age decays below a visibility threshold of 1.
				time.Sleep(10 * time.Millisecond)
				require.Equal(t, []interface{}{"v"}, column(t, "MATCH (:Hub {id: 'h'})-[r]-() RETURN r.id ORDER BY r.id"), "s and d are hidden")
				require.Equal(t, []interface{}{"d", "s", "s2", "v"}, column(t, "MATCH ()-[r]->() WHERE reveal(r) IS NOT NULL RETURN r.id ORDER BY r.id"))

				assert.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", h.run(t, "MATCH (o:Hub {id: 'o'}) DELETE o").code,
					"o's only relationship is hidden, and it still connects o")

				deleted := h.requireSuccess(t, "MATCH (h:Hub {id: 'h'}) DETACH DELETE h")
				assert.Equal(t, 1, deleted.nodesDeleted)
				assert.Equal(t, 3, deleted.relationshipsDeleted, "s, d and v went with h")

				verify := func(t *testing.T) {
					t.Helper()
					assert.Equal(t, []interface{}{"s2"}, column(t, "MATCH ()-[r]->() WHERE reveal(r) IS NOT NULL RETURN r.id ORDER BY r.id"))
					assert.Equal(t, []interface{}{"o", "p", "q"}, column(t, "MATCH (n) WHERE reveal(n) IS NOT NULL RETURN n.id ORDER BY n.id"))
					assert.Equal(t, []interface{}{"s2"}, column(t, "MATCH ()-[r:HIDDEN]->() WHERE reveal(r) IS NOT NULL RETURN r.id"), "type index")
					assert.Equal(t, []interface{}{int64(0)}, column(t, "MATCH ()-[r:STALE]->() WHERE reveal(r) IS NOT NULL RETURN count(r)"), "type index")
					assert.Equal(t, []interface{}{"s2"}, column(t, "MATCH (:Peer {id: 'p'})<-[r]-() WHERE reveal(r) IS NOT NULL RETURN r.id"), "adjacency index")
					assert.Equal(t, []interface{}{int64(0)}, column(t, "MATCH (:Peer {id: 'q'})-[r]-() WHERE reveal(r) IS NOT NULL RETURN count(r)"), "adjacency index")
					assert.Equal(t, []interface{}{int64(1)}, column(t, "MATCH ()-[r]->() WHERE reveal(r) IS NOT NULL RETURN count(r)"), "relationship count")
				}
				verify(t)

				h.shutdown(t)
				h = nil // closed: a failed reopen leaves nothing to close
				h = openHiddenDeleteServer(t, dir, writeBehind, route)
				verify(t)
			})
		}
	}
}

// Re-ingestion may MERGE the same physical identity while it is suppressed.
// That remains a successful, side-effect-free no-op with one stored relationship.
func TestSuppressedRelationshipReMergeKeepsCounts(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, route := range []string{"bolt", "http"} {
			name := route + "/write-behind off"
			if writeBehind {
				name = route + "/write-behind on"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				h := openHiddenDeleteServer(t, dir, writeBehind, route)
				t.Cleanup(func() {
					if h != nil {
						h.shutdown(t)
					}
				})
				var physicalID storage.EdgeID
				counts := func() {
					t.Helper()
					store := h.db.GetStorage()
					// Enumerate physical records independently of the counters and
					// of the query visibility layer that intentionally hides them.
					stored, err := storage.CollectStoredEdges(context.Background(), store)
					require.NoError(t, err)
					require.Len(t, stored, 1)
					if physicalID == "" {
						physicalID = stored[0].ID
					}
					assert.Equal(t, physicalID, stored[0].ID)
					for _, read := range []func() (int64, error){
						store.EdgeCount,
						func() (int64, error) { return store.EdgeCountByType("INGEST") },
						func() (int64, error) { return store.EdgeCountByStartLabel("Source", "INGEST") },
						func() (int64, error) { return store.EdgeCountByEndLabel("Target", "INGEST") },
					} {
						count, err := read()
						require.NoError(t, err)
						assert.EqualValues(t, 1, count, "stored counters must match the single physical record")
					}
					assert.Equal(t, [][]interface{}{{int64(0)}}, h.requireSuccess(t, "MATCH ()-[r:INGEST]->() RETURN count(r)").rows)
				}
				h.requireSuccess(t, "CREATE (:Source {id: 'a'}), (:Target {id: 'b'})")
				const merge = "MATCH (a:Source {id: 'a'}), (b:Target {id: 'b'}) MERGE (a)-[r:INGEST {id: 'same'}]->(b)"
				h.requireSuccess(t, merge)
				h.flush(t)
				h.requireSuccess(t, "MATCH ()-[r:INGEST]->() CALL kp.suppress(r)")
				counts()
				h.requireSuccess(t, merge)
				counts()
				h.flush(t)
				counts()
				h.shutdown(t)
				h = nil
				h = openHiddenDeleteServer(t, dir, writeBehind, route)
				counts()
			})
		}
	}
}
