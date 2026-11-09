package storage

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// This file owns the fixtures the remote-engine paths run on. The statement
// reply in remoteEngineMethodReplyHTTPServed is executed by
// TestRemoteEngineEngineMethods and, through remoteEngineMethodReply, by
// TestStatementRemoteEngineRunsEveryOperationThroughItsRunner; the explicit
// transaction wire in newRemoteExplicitTxWire serves both lifecycles of
// TestRemoteEngineHTTPExplicitTxLifecycle.

// remoteEngineMethodReplyHTTPServed answers one Engine statement request over
// HTTP with the row that method decodes, keyed by the statement and its
// parameters. It is the single statement-reply fixture: the statement-runner
// path answers from remoteEngineMethodReply, which reads this reply through the
// engine's own transport.
func remoteEngineMethodReplyHTTPServed(statement string, params map[string]interface{}) *http.Response {
	reply := func(rows [][]interface{}) *http.Response {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(rows, nil)}
	}
	switch {
	case strings.HasPrefix(statement, "CREATE (n"):
		return reply([][]interface{}{
			{map[string]interface{}{"id": "n1", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"name": "Alice"}}},
		})
	case strings.Contains(statement, "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1"):
		if params["id"] == "missing" {
			return reply(nil)
		}
		return reply([][]interface{}{
			{map[string]interface{}{"id": params["id"], "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": params["id"]}}},
		})
	case strings.Contains(statement, "SET n += $props RETURN count(n)"):
		if params["id"] == "missing" {
			return reply([][]interface{}{{float64(0)}})
		}
		return reply([][]interface{}{{float64(1)}})
	case strings.Contains(statement, "MERGE (a)-[r"):
		if params["start"] == "missing" {
			return reply(nil)
		}
		return reply([][]interface{}{{map[string]interface{}{"id": "e1"}}})
	case strings.Contains(statement, "WHERE r.id = $id RETURN r LIMIT 1"):
		if params["id"] == "missing" {
			return reply(nil)
		}
		return reply([][]interface{}{
			{map[string]interface{}{"id": params["id"], "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": params["id"]}}},
		})
	case strings.Contains(statement, "SET r += $props RETURN count(r)"):
		if params["id"] == "missing" {
			return reply([][]interface{}{{float64(0)}})
		}
		return reply([][]interface{}{{float64(1)}})
	case strings.Contains(statement, "RETURN n LIMIT 1"):
		if strings.Contains(statement, "`Missing`") {
			return reply(nil)
		}
		return reply([][]interface{}{
			{map[string]interface{}{"id": "first", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": "first"}}},
		})
	case strings.Contains(statement, "RETURN n"):
		return reply([][]interface{}{
			{map[string]interface{}{"id": "n1", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": "n1"}}},
			{map[string]interface{}{"id": "n2", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": "n2"}}},
		})
	case strings.Contains(statement, "MATCH ()-[r]->() RETURN r"):
		return reply([][]interface{}{
			{map[string]interface{}{"id": "e1", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e1"}}},
		})
	case strings.Contains(statement, "WHERE n.id = $id RETURN r"):
		return reply([][]interface{}{
			{map[string]interface{}{"id": "e2", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e2"}}},
		})
	case strings.Contains(statement, "WHERE a.id = $start AND b.id = $end RETURN r LIMIT 1"):
		return reply([][]interface{}{
			{map[string]interface{}{"id": "e3", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e3"}}},
		})
	case strings.Contains(statement, "WHERE a.id = $start AND b.id = $end RETURN r"):
		return reply([][]interface{}{
			{map[string]interface{}{"id": "e4", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e4"}}},
		})
	case strings.Contains(statement, "MATCH ()-[r:`KNOWS`]->() RETURN r"):
		return reply([][]interface{}{
			{map[string]interface{}{"id": "e5", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e5"}}},
		})
	case strings.Contains(statement, "MATCH ()-[r]->(n) WHERE n.id = $id RETURN count(r)"):
		return reply([][]interface{}{{float64(2)}})
	case strings.Contains(statement, "MATCH (n)-[r]->() WHERE n.id = $id RETURN count(r)"):
		return reply([][]interface{}{{float64(3)}})
	case strings.Contains(statement, "RETURN count(n)"):
		return reply([][]interface{}{{float64(7)}})
	case strings.Contains(statement, "RETURN count(r)"):
		return reply([][]interface{}{{float64(8)}})
	default:
		return reply(nil)
	}
}

// remoteEngineMethodReply is the statement-reply fixture as one statement
// runner returns it: the columns and rows the engine's own HTTP transport
// decodes from the reply for the same statement and parameters. Reading the
// reply through the transport keeps one decoding path and one population.
func remoteEngineMethodReply(t *testing.T, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	t.Helper()
	engine := makeEngineWithTransport(t, "http://remote.example", func(*http.Request) (*http.Response, error) {
		return remoteEngineMethodReplyHTTPServed(statement, params), nil
	})
	t.Cleanup(func() { _ = engine.Close() })
	return engine.QueryCypher(context.Background(), statement, params, RemoteAccessRead)
}

// remoteTxWire records which explicit-transaction wire calls arrived.
type remoteTxWire struct {
	opened     bool
	executed   bool
	committed  bool
	rolledBack bool
}

// newRemoteExplicitTxWire serves the explicit-transaction wire for txID and
// records in ledger which requests arrived. Opening a transaction is a POST to
// the database's tx endpoint, answered with that transaction's commit URL; each
// statement is a POST to the transaction; Commit is a POST to the commit URL;
// Rollback is a DELETE of the transaction. Every other request is a 404.
func newRemoteExplicitTxWire(t *testing.T, txID string, ledger *remoteTxWire) func(*http.Request) (*http.Response, error) {
	t.Helper()
	txURL := "/db/remote_db/tx"
	executeURL := txURL + "/" + txID
	commitURL := executeURL + "/commit"
	answered := func() *http.Response {
		return &http.Response{StatusCode: 200, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`))}
	}
	return func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPost && req.URL.Path == txURL:
			ledger.opened = true
			return &http.Response{StatusCode: 200, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"results":[],"errors":[],"commit":"http://remote.example/db/remote_db/tx/` + txID + `/commit"}`))}, nil
		case req.Method == http.MethodPost && req.URL.Path == executeURL:
			ledger.executed = true
			return &http.Response{StatusCode: 200, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"results":[{"columns":["n"],"data":[{"row":[1]}]}],"errors":[]}`))}, nil
		case req.Method == http.MethodPost && req.URL.Path == commitURL:
			ledger.committed = true
			return answered(), nil
		case req.Method == http.MethodDelete && req.URL.Path == executeURL:
			ledger.rolledBack = true
			return answered(), nil
		default:
			return &http.Response{StatusCode: 404, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`))}, nil
		}
	}
}
