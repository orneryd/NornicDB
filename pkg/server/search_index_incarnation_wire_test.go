package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const incarnationWireOptions = " OPTIONS {indexConfig: {`vector.dimensions`:2048, `vector.similarity_function`:'cosine'}}"

// incarnationWireVector is a 2048-dimension vector with the given leading
// components.
func incarnationWireVector(components ...float64) []float64 {
	vector := make([]float64, 2048)
	copy(vector, components)
	return vector
}

// incarnationWire runs statements over HTTP or Bolt, in auto-commit
// statements or in one explicit transaction, and returns the last statement's
// rows.
type incarnationWire struct {
	t       *testing.T
	route   string
	run     func(statements []string, params map[string]any, explicit bool) ([][]any, error)
	cleanup func()
}

func newIncarnationWire(t *testing.T, route string, server *Server, authenticator *auth.Authenticator, database string) *incarnationWire {
	t.Helper()
	ctx := context.Background()
	wire := &incarnationWire{t: t, route: route}
	if route == "http" {
		token := "Bearer " + getAuthToken(t, authenticator, "admin")
		request := func(endpoint string, statements []map[string]any) (TransactionResponse, error) {
			t.Helper()
			if statements == nil {
				statements = []map[string]any{}
			}
			response := makeRequest(t, server, "POST", endpoint, map[string]any{"statements": statements}, token)
			var decoded TransactionResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded), response.Body.String())
			if len(decoded.Errors) > 0 {
				return decoded, errors.New(decoded.Errors[0].Code + ": " + decoded.Errors[0].Message)
			}
			require.Contains(t, []int{200, 201}, response.Code, response.Body.String())
			return decoded, nil
		}
		wire.run = func(queries []string, params map[string]any, explicit bool) ([][]any, error) {
			var statements []map[string]any
			for _, query := range queries {
				statement := map[string]any{"statement": query}
				if params != nil {
					statement["parameters"] = params
				}
				statements = append(statements, statement)
			}
			endpoint := "/db/" + database + "/tx/commit"
			if explicit {
				endpoint = "/db/" + database + "/tx"
			}
			decoded, err := request(endpoint, statements)
			if err != nil {
				return nil, err
			}
			if explicit {
				require.NotEmpty(t, decoded.Commit)
				if _, err := request(decoded.Commit, nil); err != nil {
					return nil, err
				}
			}
			require.Len(t, decoded.Results, len(queries))
			var rows [][]any
			for _, row := range decoded.Results[len(queries)-1].Data {
				rows = append(rows, row.Row)
			}
			return rows, nil
		}
		return wire
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), searchNodeWireExecutor{server}, server.dbManager)
	served := make(chan error, 1)
	go func() { served <- peer.Serve(listener) }()
	t.Cleanup(func() { require.NoError(t, peer.Close()); require.NoError(t, <-served) })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	t.Cleanup(func() { _ = session.Close(ctx) })
	collect := func(result neo4j.ResultWithContext, err error) ([][]any, error) {
		if err != nil {
			return nil, err
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return nil, err
		}
		var rows [][]any
		for _, record := range records {
			rows = append(rows, record.Values)
		}
		return rows, nil
	}
	wire.run = func(queries []string, params map[string]any, explicit bool) ([][]any, error) {
		var rows [][]any
		if !explicit {
			for _, query := range queries {
				var err error
				if rows, err = collect(session.Run(ctx, query, params)); err != nil {
					return nil, err
				}
			}
			return rows, nil
		}
		tx, err := session.BeginTransaction(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Close(ctx)
		for _, query := range queries {
			if rows, err = collect(tx.Run(ctx, query, params)); err != nil {
				return nil, err
			}
		}
		return rows, tx.Commit(ctx)
	}
	return wire
}

// served is what index serves for the first unit vector over the wire, by
// node id, with scores rounded to 3 decimals.
func (w *incarnationWire) served(index string) (map[any]float64, error) {
	w.t.Helper()
	rows, err := w.run([]string{fmt.Sprintf("CALL db.index.vector.queryNodes('%s', 10, $q) YIELD node, score RETURN node.id, score", index)}, map[string]any{"q": incarnationWireVector(1)}, false)
	if err != nil {
		return nil, err
	}
	scores := map[any]float64{}
	for _, row := range rows {
		score, ok := row[1].(float64)
		require.True(w.t, ok, "score %v", row[1])
		scores[row[0]] = math.Round(score*1000) / 1000
	}
	return scores, nil
}

func (w *incarnationWire) requireServes(index string, want map[any]float64, msg string) {
	w.t.Helper()
	got, err := w.served(index)
	require.NoError(w.t, err, "%s over %s", msg, w.route)
	require.Equal(w.t, want, got, "%s over %s", msg, w.route)
}

func (w *incarnationWire) requireNoIndex(index string) {
	w.t.Helper()
	_, err := w.served(index)
	require.ErrorContains(w.t, err, "There is no such vector schema index", "a query naming %s over %s", index, w.route)
}

func (w *incarnationWire) mustRun(statements []string, explicit bool) {
	w.t.Helper()
	_, err := w.run(statements, nil, explicit)
	require.NoError(w.t, err, "%v over %s", statements, w.route)
}

// NOR-404 over HTTP and Bolt: dropping one of two vector indexes on a
// property leaves the other serving its nodes, after one drop, after the
// dropped index is created again and dropped again, and when both are dropped
// and created again in one explicit transaction; the dropped name is unknown;
// and an index created again serves the stored nodes at once.
func TestNOR404SiblingIndexSurvivesDropWire(t *testing.T) {
	for _, route := range []string{"http", "bolt"} {
		t.Run(route, func(t *testing.T) {
			ctx := context.Background()
			server, authenticator := setupTestServer(t)
			database := server.dbManager.DefaultDatabaseName()
			exec, err := server.newExecutorForDatabase(database)
			require.NoError(t, err)
			docs := "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationWireOptions
			notes := "CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb)" + incarnationWireOptions
			for _, query := range []string{docs, notes, "CREATE (:Doc {id:'d', emb:$d}), (:Note {id:'n', emb:$n}), (:Doc:Note {id:'dn', emb:$dn})"} {
				_, err := exec.Execute(ctx, query, map[string]any{"d": incarnationWireVector(1), "n": incarnationWireVector(0.6, 0.8), "dn": incarnationWireVector(0, 1)})
				require.NoError(t, err)
			}
			engine, err := server.dbManager.GetStorage(database)
			require.NoError(t, err)
			service, err := server.db.GetOrCreateSearchService(database, engine)
			require.NoError(t, err)
			require.NoError(t, service.BuildIndexes(ctx))
			wire := newIncarnationWire(t, route, server, authenticator, database)
			docsServed := map[any]float64{"d": 1, "dn": 0.5}
			notesServed := map[any]float64{"dn": 0.5, "n": 0.8}
			wire.requireServes("docs", docsServed, "before any drop")
			wire.requireServes("notes", notesServed, "before any drop")

			wire.mustRun([]string{"DROP INDEX docs"}, false)
			wire.requireServes("notes", notesServed, "after DROP INDEX docs")
			wire.requireNoIndex("docs")
			wire.mustRun([]string{docs}, false)
			wire.requireServes("docs", docsServed, "recreated")
			wire.mustRun([]string{"DROP INDEX docs"}, false)
			wire.requireServes("notes", notesServed, "after a second DROP INDEX docs")

			wire.mustRun([]string{"DROP INDEX notes", docs, notes}, true)
			wire.requireServes("docs", docsServed, "created again in one transaction")
			wire.requireServes("notes", notesServed, "dropped and created again in one transaction")
			require.Equal(t, 3, service.CountPropertyVectorEntries("emb"))
		})
	}
}

// NOR-386, review round 2 of #1753 (reviewer probe; expectation updated for
// NOR-404): both vector indexes on a property are replaced under their own
// names in one explicit transaction over HTTP and Bolt, with a refresh
// admitted before the transaction. Both replacements serve the stored node
// with its current vector.
func TestReview1753R2SameNameReplacementsWire(t *testing.T) {
	for _, route := range []string{"http", "bolt"} {
		t.Run(route, func(t *testing.T) {
			ctx := context.Background()
			server, authenticator := setupTestServer(t)
			database := server.dbManager.DefaultDatabaseName()
			exec, err := server.newExecutorForDatabase(database)
			require.NoError(t, err)
			docs := "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationWireOptions
			notes := "CREATE VECTOR INDEX notes FOR (n:Note) ON (n.emb)" + incarnationWireOptions
			for _, query := range []string{docs, notes, "CREATE (:Doc:Note {id:'both', emb:$v})"} {
				_, err := exec.Execute(ctx, query, map[string]any{"v": incarnationWireVector(1)})
				require.NoError(t, err)
			}
			engine, err := server.dbManager.GetStorage(database)
			require.NoError(t, err)
			service, err := server.db.GetOrCreateSearchService(database, engine)
			require.NoError(t, err)
			require.NoError(t, service.BuildIndexes(ctx))
			nodes, err := engine.GetNodesByLabel(ctx, "Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			// A write before the transaction whose refresh is still queued.
			changed := storage.CopyNode(nodes[0])
			changed.Properties["emb"] = incarnationWireVector(0.6, 0.8)
			require.NoError(t, engine.UpdateNode(ctx, storage.NodeUpdateRequest{After: changed}))
			mutation := service.ReserveNodeMutation(storage.NodeID(nodes[0].ID))
			wire := newIncarnationWire(t, route, server, authenticator, database)
			wire.mustRun([]string{"DROP INDEX docs", "DROP INDEX notes", docs, notes}, true)
			require.NoError(t, service.RefreshNodeWithMutation(ctx, mutation))
			for _, index := range []string{"docs", "notes"} {
				wire.requireServes(index, map[any]float64{"both": 0.8}, "replacement "+index)
			}
		})
	}
}
