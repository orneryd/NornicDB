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
	case "http":
		token := "Bearer " + getAuthToken(t, authenticator, "admin")
		h.run = func(t *testing.T, statement string) hiddenDeleteOutcome {
			t.Helper()
			recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
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
	case "bolt":
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
			result, err := session.Run(ctx, statement, nil)
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
				ok := func(t *testing.T, statement string) hiddenDeleteOutcome {
					t.Helper()
					outcome := h.run(t, statement)
					require.Empty(t, outcome.code, statement)
					return outcome
				}
				column := func(t *testing.T, statement string) []interface{} {
					t.Helper()
					var values []interface{}
					for _, row := range ok(t, statement).rows {
						values = append(values, row[0])
					}
					return values
				}

				ok(t, "CREATE DECAY PROFILE stale OPTIONS {halfLifeSeconds: 1, function: 'exponential', scope: 'EDGE', scoreFrom: 'CREATED', visibilityThreshold: 1.0}")
				ok(t, "CREATE DECAY PROFILE stale_edges FOR ()-[r:STALE]-() APPLY {DECAY PROFILE 'stale'}")
				ok(t, "CREATE (h:Hub {id: 'h'}), (o:Hub {id: 'o'}), (p:Peer {id: 'p'}), (q:Peer {id: 'q'}), "+
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

				deleted := ok(t, "MATCH (h:Hub {id: 'h'}) DETACH DELETE h")
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
