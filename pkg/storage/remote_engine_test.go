package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Compile-time interface assertion (also in remote_engine.go, duplicated here per project convention).
var _ Engine = (*RemoteEngine)(nil)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type errReadCloser struct{}

func (e *errReadCloser) Read(_ []byte) (int, error) { return 0, fmt.Errorf("read failed") }
func (e *errReadCloser) Close() error               { return nil }

type fakeRemoteTransport struct {
	queryFn            func(context.Context, string, map[string]interface{}) ([][]interface{}, error)
	queryWithColumnsFn func(context.Context, string, map[string]interface{}) ([]string, [][]interface{}, error)
	queryBatchFn       func(context.Context, []remoteStatement) error
	beginCypherTxFn    func(context.Context) (RemoteCypherTx, error)
	closeFn            func() error
}

func (f *fakeRemoteTransport) query(ctx context.Context, statement string, params map[string]interface{}) ([][]interface{}, error) {
	if f.queryFn != nil {
		return f.queryFn(ctx, statement, params)
	}
	return nil, nil
}

func (f *fakeRemoteTransport) queryWithColumns(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	if f.queryWithColumnsFn != nil {
		return f.queryWithColumnsFn(ctx, statement, params)
	}
	return nil, nil, nil
}

func (f *fakeRemoteTransport) queryBatch(ctx context.Context, statements []remoteStatement) error {
	if f.queryBatchFn != nil {
		return f.queryBatchFn(ctx, statements)
	}
	return nil
}

func (f *fakeRemoteTransport) beginCypherTx(ctx context.Context) (RemoteCypherTx, error) {
	if f.beginCypherTxFn != nil {
		return f.beginCypherTxFn(ctx)
	}
	return nil, nil
}

func (f *fakeRemoteTransport) close() error {
	if f.closeFn != nil {
		return f.closeFn()
	}
	return nil
}

func makeTXResponse(rows [][]interface{}, errs []map[string]string) io.ReadCloser {
	data := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		data = append(data, map[string]interface{}{"row": row})
	}
	body := map[string]interface{}{
		"results": []map[string]interface{}{
			{"columns": []string{"v"}, "data": data},
		},
		"errors": errs,
	}
	raw, _ := json.Marshal(body)
	return io.NopCloser(bytes.NewReader(raw))
}

func makeEngineWithTransport(t *testing.T, baseURL string, rt roundTripFunc) *RemoteEngine {
	t.Helper()
	engine, err := NewRemoteEngine(RemoteEngineConfig{
		URI:       baseURL,
		Database:  "remote_db",
		AuthToken: "Bearer svc-token",
		HTTPClient: &http.Client{
			Transport: rt,
		},
	})
	if err != nil {
		t.Fatalf("failed to create remote engine: %v", err)
	}
	return engine
}

// ---------------------------------------------------------------------------
// URI scheme auto-detection and config validation
// ---------------------------------------------------------------------------

func TestRemoteEngineURISchemeDetection(t *testing.T) {
	// Empty URI.
	if _, err := NewRemoteEngine(RemoteEngineConfig{URI: "", Database: "x"}); err == nil {
		t.Fatalf("expected empty URI error")
	}
	// Empty database.
	if _, err := NewRemoteEngine(RemoteEngineConfig{URI: "http://x", Database: ""}); err == nil {
		t.Fatalf("expected empty database error")
	}
	// Unsupported scheme.
	if _, err := NewRemoteEngine(RemoteEngineConfig{URI: "ftp://host", Database: "db"}); err == nil {
		t.Fatalf("expected unsupported scheme error")
	}
	if _, err := NewRemoteEngine(RemoteEngineConfig{URI: "://bad", Database: "db"}); err == nil {
		t.Fatalf("expected unsupported scheme error")
	}

	// HTTP schemes should succeed.
	for _, scheme := range []string{"http://host", "https://host"} {
		e, err := NewRemoteEngine(RemoteEngineConfig{URI: scheme, Database: "db"})
		if err != nil {
			t.Fatalf("unexpected error for %s: %v", scheme, err)
		}
		if e == nil {
			t.Fatalf("expected non-nil engine for %s", scheme)
		}
		if _, ok := e.transport.(*httpTransport); !ok {
			t.Fatalf("expected httpTransport for %s, got %T", scheme, e.transport)
		}
	}

	// Bolt schemes should attempt driver creation (will fail without a server, but
	// we verify the code path is selected correctly by checking the error type).
	for _, scheme := range []string{
		"bolt://host:7687",
		"bolt+s://host:7687",
		"bolt+ssc://host:7687",
		"neo4j://host:7687",
		"neo4j+s://host:7687",
		"neo4j+ssc://host:7687",
	} {
		e, err := NewRemoteEngine(RemoteEngineConfig{URI: scheme, Database: "db"})
		// The neo4j driver creates successfully without connecting (lazy connect).
		if err != nil {
			t.Fatalf("unexpected error for %s: %v", scheme, err)
		}
		if _, ok := e.transport.(*boltTransport); !ok {
			t.Fatalf("expected boltTransport for %s, got %T", scheme, e.transport)
		}
		_ = e.Close()
	}
}

func TestRemoteEngineBoltAuthConstruction(t *testing.T) {
	// Basic auth.
	auth := buildNeo4jAuth(RemoteEngineConfig{User: "alice", Password: "secret"})
	if auth.Tokens["scheme"] != "basic" {
		t.Fatalf("expected basic scheme, got %v", auth.Tokens["scheme"])
	}
	if auth.Tokens["principal"] != "alice" {
		t.Fatalf("expected principal alice, got %v", auth.Tokens["principal"])
	}

	// Bearer auth with prefix.
	auth = buildNeo4jAuth(RemoteEngineConfig{AuthToken: "Bearer my-jwt-token"})
	if auth.Tokens["scheme"] != "bearer" {
		t.Fatalf("expected bearer scheme, got %v", auth.Tokens["scheme"])
	}
	if auth.Tokens["credentials"] != "my-jwt-token" {
		t.Fatalf("expected stripped token, got %v", auth.Tokens["credentials"])
	}

	// Bearer auth without prefix.
	auth = buildNeo4jAuth(RemoteEngineConfig{AuthToken: "raw-token"})
	if auth.Tokens["scheme"] != "bearer" {
		t.Fatalf("expected bearer scheme, got %v", auth.Tokens["scheme"])
	}
	if auth.Tokens["credentials"] != "raw-token" {
		t.Fatalf("expected raw token, got %v", auth.Tokens["credentials"])
	}

	// Bearer auth with lowercase prefix (case-insensitive per RFC 7235).
	auth = buildNeo4jAuth(RemoteEngineConfig{AuthToken: "bearer my-jwt-token"})
	if auth.Tokens["scheme"] != "bearer" {
		t.Fatalf("expected bearer scheme for lowercase, got %v", auth.Tokens["scheme"])
	}
	if auth.Tokens["credentials"] != "my-jwt-token" {
		t.Fatalf("expected stripped token for lowercase bearer, got %v", auth.Tokens["credentials"])
	}

	// Bearer auth with mixed case prefix.
	auth = buildNeo4jAuth(RemoteEngineConfig{AuthToken: "BEARER my-jwt-token"})
	if auth.Tokens["scheme"] != "bearer" {
		t.Fatalf("expected bearer scheme for uppercase, got %v", auth.Tokens["scheme"])
	}
	if auth.Tokens["credentials"] != "my-jwt-token" {
		t.Fatalf("expected stripped token for uppercase BEARER, got %v", auth.Tokens["credentials"])
	}

	// No auth.
	auth = buildNeo4jAuth(RemoteEngineConfig{})
	if auth.Tokens["scheme"] != "none" {
		t.Fatalf("expected none scheme, got %v", auth.Tokens["scheme"])
	}

	// User+password takes precedence over token.
	auth = buildNeo4jAuth(RemoteEngineConfig{User: "u", Password: "p", AuthToken: "Bearer tok"})
	if auth.Tokens["scheme"] != "basic" {
		t.Fatalf("expected basic scheme when user/password present, got %v", auth.Tokens["scheme"])
	}
}

// ---------------------------------------------------------------------------
// HTTP transport: config and commit URL
// ---------------------------------------------------------------------------

func TestRemoteEngineConfigAndCommitURL(t *testing.T) {
	e1, err := NewRemoteEngine(RemoteEngineConfig{URI: "http://host", Database: "db"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ht := e1.transport.(*httpTransport)
	if ht.client == nil {
		t.Fatalf("expected default client")
	}
	if got := ht.commitURL(); got != "http://host/db/db/tx/commit" {
		t.Fatalf("unexpected commit URL: %s", got)
	}

	e2, _ := NewRemoteEngine(RemoteEngineConfig{URI: "http://host/db/neo4j", Database: "x"})
	if got := e2.transport.(*httpTransport).commitURL(); got != "http://host/db/neo4j/tx/commit" {
		t.Fatalf("unexpected commit URL: %s", got)
	}

	e3, _ := NewRemoteEngine(RemoteEngineConfig{URI: "http://host/db/neo4j/tx/commit", Database: "x"})
	if got := e3.transport.(*httpTransport).commitURL(); got != "http://host/db/neo4j/tx/commit" {
		t.Fatalf("unexpected commit URL: %s", got)
	}
}

// ---------------------------------------------------------------------------
// HTTP transport: query branches
// ---------------------------------------------------------------------------

func TestRemoteEngineQueryBranches(t *testing.T) {
	// Marshal failure path — inject via a mock transport directly.
	badEngine := &RemoteEngine{
		transport: &httpTransport{
			baseURL:  "http://host",
			database: "db",
			client:   &http.Client{},
		},
		schema: NewSchemaManager(),
	}
	ctx := context.Background()
	if _, err := badEngine.transport.query(ctx, "RETURN 1", map[string]interface{}{"bad": func() {}}); err == nil {
		t.Fatalf("expected marshal error")
	}

	// Bad URL.
	badURLEngine := &RemoteEngine{
		transport: &httpTransport{
			baseURL:  "://bad-url",
			database: "db",
			client:   &http.Client{},
		},
		schema: NewSchemaManager(),
	}
	if _, err := badURLEngine.transport.query(ctx, "RETURN 1", nil); err == nil {
		t.Fatalf("expected bad URL error")
	}

	// HTTP Do error.
	eDoErr := makeEngineWithTransport(t, "http://host", func(_ *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("network down")
	})
	if _, err := eDoErr.transport.query(ctx, "RETURN 1", nil); err == nil {
		t.Fatalf("expected network error")
	}

	// ReadAll error.
	eReadErr := makeEngineWithTransport(t, "http://host", func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body:       &errReadCloser{},
			Header:     make(http.Header),
		}, nil
	})
	if _, err := eReadErr.transport.query(ctx, "RETURN 1", nil); err == nil {
		t.Fatalf("expected read body error")
	}

	// Decode error.
	eDecodeErr := makeEngineWithTransport(t, "http://host", func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("{bad-json")),
			Header:     make(http.Header),
		}, nil
	})
	if _, err := eDecodeErr.transport.query(ctx, "RETURN 1", nil); err == nil {
		t.Fatalf("expected decode error")
	}

	// Cypher error propagation.
	eCypherErr := makeEngineWithTransport(t, "http://host", func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body: makeTXResponse(nil, []map[string]string{
				{"code": "Neo.ClientError", "message": "bad statement"},
			}),
			Header: make(http.Header),
		}, nil
	})
	if _, err := eCypherErr.transport.query(ctx, "RETURN 1", nil); err == nil || !strings.Contains(err.Error(), "bad statement") {
		t.Fatalf("expected cypher error, got: %v", err)
	}

	// Empty results path + auth forwarding.
	eOK := makeEngineWithTransport(t, "http://host", func(req *http.Request) (*http.Response, error) {
		if req.Header.Get("Authorization") != "Bearer svc-token" {
			t.Fatalf("expected auth header forwarded")
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			Header:     make(http.Header),
		}, nil
	})
	rows, err := eOK.transport.query(ctx, "RETURN 1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected empty rows")
	}

	// Explicit user/password should use Basic auth and ignore forwarded token.
	eBasic, err := NewRemoteEngine(RemoteEngineConfig{
		URI:       "http://host",
		Database:  "remote_db",
		AuthToken: "Bearer should-not-be-used",
		User:      "svc-user",
		Password:  "svc-pass",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				gotUser, gotPass, ok := req.BasicAuth()
				if !ok || gotUser != "svc-user" || gotPass != "svc-pass" {
					t.Fatalf("expected basic auth credentials")
				}
				if !strings.HasPrefix(req.Header.Get("Authorization"), "Basic ") {
					t.Fatalf("expected basic authorization header when basic auth is configured")
				}
				if strings.Contains(req.Header.Get("Authorization"), "Bearer ") {
					t.Fatalf("bearer auth must not be used when basic auth is configured")
				}
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
					Header:     make(http.Header),
				}, nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error creating basic auth remote engine: %v", err)
	}
	if _, err := eBasic.transport.query(ctx, "RETURN 1", nil); err != nil {
		t.Fatalf("unexpected error from basic auth query: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Helper conversions
// ---------------------------------------------------------------------------

func TestRemoteEngineHelperConversions(t *testing.T) {
	if got := quoteIdent("a`b"); got != "`a``b`" {
		t.Fatalf("unexpected quoteIdent: %s", got)
	}
	if got := labelsExpr([]string{"Person", "", "User"}); got != ":`Person`:`User`" {
		t.Fatalf("unexpected labelsExpr: %s", got)
	}
	if asMap(7) != nil {
		t.Fatalf("expected nil asMap for non-map")
	}
	if remoteToInt64(3) != 3 || remoteToInt64(int64(4)) != 4 || remoteToInt64(5.8) != 5 || remoteToInt64("x") != 0 {
		t.Fatalf("unexpected remoteToInt64 conversions")
	}

	if _, err := valueToNode(1); err == nil {
		t.Fatalf("expected valueToNode type error")
	}
	node, err := valueToNode(map[string]interface{}{
		"elementId": "n-1",
		"labels":    []interface{}{"A", "B"},
		"properties": map[string]interface{}{
			"id": "n-prop-id",
		},
	})
	if err != nil || node.ID != "n-prop-id" || len(node.Labels) != 2 {
		t.Fatalf("unexpected node conversion: %#v err=%v", node, err)
	}
	nodeByID, err := valueToNode(map[string]interface{}{
		"id":     "n-id",
		"labels": []interface{}{},
	})
	if err != nil || nodeByID.ID != "n-id" {
		t.Fatalf("unexpected node conversion by id: %#v err=%v", nodeByID, err)
	}
	nodeByElementID, err := valueToNode(map[string]interface{}{
		"elementId": "n-element",
	})
	if err != nil || nodeByElementID.ID != "n-element" {
		t.Fatalf("unexpected node conversion by elementId: %#v err=%v", nodeByElementID, err)
	}

	if _, err := valueToEdge(1); err == nil {
		t.Fatalf("expected valueToEdge type error")
	}
	edge, err := valueToEdge(map[string]interface{}{
		"elementId":          "e-1",
		"type":               "REL",
		"startNodeElementId": "s",
		"endNodeElementId":   "e",
		"properties": map[string]interface{}{
			"id": "edge-prop-id",
		},
	})
	if err != nil || edge.ID != "edge-prop-id" || edge.Type != "REL" {
		t.Fatalf("unexpected edge conversion: %#v err=%v", edge, err)
	}
	edgeByID, err := valueToEdge(map[string]interface{}{
		"id": "e-id",
	})
	if err != nil || edgeByID.ID != "e-id" {
		t.Fatalf("unexpected edge conversion by id: %#v err=%v", edgeByID, err)
	}
	edgeByElementID, err := valueToEdge(map[string]interface{}{
		"elementId": "e-element",
	})
	if err != nil || edgeByElementID.ID != "e-element" {
		t.Fatalf("unexpected edge conversion by elementId: %#v err=%v", edgeByElementID, err)
	}
}

// ---------------------------------------------------------------------------
// Bolt value normalization
// ---------------------------------------------------------------------------

func TestNormalizeBoltValue(t *testing.T) {
	// Passthrough for non-db types.
	if v := normalizeBoltValue(42); v != 42 {
		t.Fatalf("expected passthrough for int")
	}
	if v := normalizeBoltValue("hello"); v != "hello" {
		t.Fatalf("expected passthrough for string")
	}
}

func TestAccessModeForStatement(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantWrite bool
	}{
		{name: "read", statement: "MATCH (n) RETURN n", wantWrite: false},
		{name: "create", statement: "CREATE (n:Person)", wantWrite: true},
		{name: "merge", statement: "MERGE (n:Person {id: 1})", wantWrite: true},
		{name: "delete", statement: "MATCH (n) DELETE n", wantWrite: true},
		{name: "set", statement: "MATCH (n) SET n.x = 1", wantWrite: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := accessModeForStatement(tt.statement)
			if tt.wantWrite && got != neo4j.AccessModeWrite {
				t.Fatalf("expected write access mode, got %v", got)
			}
			if !tt.wantWrite && got != neo4j.AccessModeRead {
				t.Fatalf("expected read access mode, got %v", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Engine methods via HTTP transport mock
// ---------------------------------------------------------------------------

func TestRemoteEngineEngineMethods(t *testing.T) {
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		var txReq remoteTxRequest
		if err := json.NewDecoder(req.Body).Decode(&txReq); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}
		stmt := txReq.Statements[0].Statement
		params := txReq.Statements[0].Parameters

		switch {
		case strings.HasPrefix(stmt, "CREATE (n"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": "n1", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"name": "Alice"}}},
			}, nil)}, nil
		case strings.Contains(stmt, "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1"):
			if params["id"] == "missing" {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": params["id"], "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": params["id"]}}},
			}, nil)}, nil
		case strings.Contains(stmt, "SET n += $props RETURN count(n)"):
			if params["id"] == "missing" {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(0)}}, nil)}, nil
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(1)}}, nil)}, nil
		case strings.Contains(stmt, "MERGE (a)-[r"):
			if params["start"] == "missing" {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{map[string]interface{}{"id": "e1"}}}, nil)}, nil
		case strings.Contains(stmt, "WHERE r.id = $id RETURN r LIMIT 1"):
			if params["id"] == "missing" {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": params["id"], "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": params["id"]}}},
			}, nil)}, nil
		case strings.Contains(stmt, "SET r += $props RETURN count(r)"):
			if params["id"] == "missing" {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(0)}}, nil)}, nil
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(1)}}, nil)}, nil
		case strings.Contains(stmt, "RETURN n LIMIT 1"):
			if strings.Contains(stmt, "`Missing`") {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": "first", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": "first"}}},
			}, nil)}, nil
		case strings.Contains(stmt, "RETURN n"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": "n1", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": "n1"}}},
				{map[string]interface{}{"id": "n2", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": "n2"}}},
			}, nil)}, nil
		case strings.Contains(stmt, "MATCH ()-[r]->() RETURN r"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": "e1", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e1"}}},
			}, nil)}, nil
		case strings.Contains(stmt, "WHERE n.id = $id RETURN r"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": "e2", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e2"}}},
			}, nil)}, nil
		case strings.Contains(stmt, "WHERE a.id = $start AND b.id = $end RETURN r LIMIT 1"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": "e3", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e3"}}},
			}, nil)}, nil
		case strings.Contains(stmt, "WHERE a.id = $start AND b.id = $end RETURN r"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": "e4", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e4"}}},
			}, nil)}, nil
		case strings.Contains(stmt, "MATCH ()-[r:`KNOWS`]->() RETURN r"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": "e5", "type": "KNOWS", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e5"}}},
			}, nil)}, nil
		case strings.Contains(stmt, "MATCH ()-[r]->(n) WHERE n.id = $id RETURN count(r)"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(2)}}, nil)}, nil
		case strings.Contains(stmt, "MATCH (n)-[r]->() WHERE n.id = $id RETURN count(r)"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(3)}}, nil)}, nil
		case strings.Contains(stmt, "RETURN count(n)"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(7)}}, nil)}, nil
		case strings.Contains(stmt, "RETURN count(r)"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(8)}}, nil)}, nil
		default:
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
		}
	})

	if _, err := engine.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}); err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}
	if _, err := engine.GetNode("missing"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for missing node, got: %v", err)
	}
	if _, err := engine.GetNode("n1"); err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}
	if err := engine.UpdateNode(&Node{ID: "missing", Properties: map[string]interface{}{"x": 1}}); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound from UpdateNode")
	}
	if err := engine.UpdateNode(&Node{ID: "n1", Properties: map[string]interface{}{"x": 1}}); err != nil {
		t.Fatalf("UpdateNode failed: %v", err)
	}
	if err := engine.DeleteNode("n1"); err != nil {
		t.Fatalf("DeleteNode failed: %v", err)
	}

	if err := engine.CreateEdge(&Edge{ID: "e1", StartNode: "missing", EndNode: "n2", Type: "KNOWS"}); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound from CreateEdge")
	}
	if err := engine.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}); err != nil {
		t.Fatalf("CreateEdge failed: %v", err)
	}
	if _, err := engine.GetEdge("missing"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound from GetEdge")
	}
	if _, err := engine.GetEdge("e1"); err != nil {
		t.Fatalf("GetEdge failed: %v", err)
	}
	if err := engine.UpdateEdge(&Edge{ID: "missing", Properties: map[string]interface{}{"w": 1}}); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound from UpdateEdge")
	}
	if err := engine.UpdateEdge(&Edge{ID: "e1", Properties: map[string]interface{}{"w": 1}}); err != nil {
		t.Fatalf("UpdateEdge failed: %v", err)
	}
	if err := engine.DeleteEdge("e1"); err != nil {
		t.Fatalf("DeleteEdge failed: %v", err)
	}

	if nodes, err := engine.GetNodesByLabel("Person"); err != nil || len(nodes) == 0 {
		t.Fatalf("GetNodesByLabel failed: len=%d err=%v", len(nodes), err)
	}
	if _, err := engine.GetFirstNodeByLabel("Missing"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound from GetFirstNodeByLabel")
	}
	if _, err := engine.GetFirstNodeByLabel("Person"); err != nil {
		t.Fatalf("GetFirstNodeByLabel failed: %v", err)
	}
	if edges, err := engine.GetOutgoingEdges("n1"); err != nil || len(edges) == 0 {
		t.Fatalf("GetOutgoingEdges failed: len=%d err=%v", len(edges), err)
	}
	if edges, err := engine.GetIncomingEdges("n1"); err != nil || len(edges) == 0 {
		t.Fatalf("GetIncomingEdges failed: len=%d err=%v", len(edges), err)
	}
	if edges, err := engine.GetEdgesBetween("n1", "n2"); err != nil || len(edges) == 0 {
		t.Fatalf("GetEdgesBetween failed: len=%d err=%v", len(edges), err)
	}
	if edge := engine.GetEdgeBetween("n1", "n2", "KNOWS"); edge == nil {
		t.Fatalf("GetEdgeBetween failed")
	}
	if edges, err := engine.GetEdgesByType("KNOWS"); err != nil || len(edges) == 0 {
		t.Fatalf("GetEdgesByType failed: len=%d err=%v", len(edges), err)
	}
	if allEdges, err := engine.AllEdges(); err != nil || len(allEdges) == 0 {
		t.Fatalf("AllEdges failed: len=%d err=%v", len(allEdges), err)
	}
	if allNodes := engine.GetAllNodes(); len(allNodes) == 0 {
		t.Fatalf("GetAllNodes should return nodes")
	}
	if got := engine.GetInDegree("n1"); got != 2 {
		t.Fatalf("GetInDegree mismatch: %d", got)
	}
	if got := engine.GetOutDegree("n1"); got != 3 {
		t.Fatalf("GetOutDegree mismatch: %d", got)
	}
	if got := engine.GetSchema(); got == nil {
		t.Fatalf("expected schema")
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close should be nil, got: %v", err)
	}
	if n, err := engine.NodeCount(); err != nil || n != 7 {
		t.Fatalf("NodeCount mismatch: n=%d err=%v", n, err)
	}
	if n, err := engine.EdgeCount(); err != nil || n != 8 {
		t.Fatalf("EdgeCount mismatch: n=%d err=%v", n, err)
	}
	if _, _, err := engine.DeleteByPrefix("x:"); err == nil {
		t.Fatalf("expected unsupported DeleteByPrefix error")
	}
}

// ---------------------------------------------------------------------------
// Bulk operations — batching via queryBatch
// ---------------------------------------------------------------------------

func TestRemoteEngineBulkBatching(t *testing.T) {
	batchCalls := 0
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		var txReq remoteTxRequest
		_ = json.NewDecoder(req.Body).Decode(&txReq)
		batchCalls++
		// Verify batching: multiple statements in one request.
		if len(txReq.Statements) > 1 {
			// Good — this is a batched call.
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
	})

	// Create 3 nodes in a single batch (< remoteBulkChunkSize).
	nodes := []*Node{
		{ID: "n1", Labels: []string{"X"}, Properties: map[string]interface{}{}},
		{ID: "n2", Labels: []string{"X"}, Properties: map[string]interface{}{}},
		{ID: "n3", Labels: []string{"X"}, Properties: map[string]interface{}{}},
	}
	batchCalls = 0
	if err := engine.BulkCreateNodes(nodes); err != nil {
		t.Fatalf("BulkCreateNodes failed: %v", err)
	}
	if batchCalls != 1 {
		t.Fatalf("expected 1 batch HTTP call for 3 nodes, got %d", batchCalls)
	}

	// Create edges in batch.
	edges := []*Edge{
		{ID: "e1", StartNode: "a", EndNode: "b", Type: "REL", Properties: map[string]interface{}{}},
		{ID: "e2", StartNode: "c", EndNode: "d", Type: "REL", Properties: map[string]interface{}{}},
	}
	batchCalls = 0
	if err := engine.BulkCreateEdges(edges); err != nil {
		t.Fatalf("BulkCreateEdges failed: %v", err)
	}
	if batchCalls != 1 {
		t.Fatalf("expected 1 batch HTTP call for 2 edges, got %d", batchCalls)
	}

	// Delete nodes in batch.
	batchCalls = 0
	if err := engine.BulkDeleteNodes([]NodeID{"n1", "n2"}); err != nil {
		t.Fatalf("BulkDeleteNodes failed: %v", err)
	}
	if batchCalls != 1 {
		t.Fatalf("expected 1 batch HTTP call for 2 node deletes, got %d", batchCalls)
	}

	// Delete edges in batch.
	batchCalls = 0
	if err := engine.BulkDeleteEdges([]EdgeID{"e1", "e2"}); err != nil {
		t.Fatalf("BulkDeleteEdges failed: %v", err)
	}
	if batchCalls != 1 {
		t.Fatalf("expected 1 batch HTTP call for 2 edge deletes, got %d", batchCalls)
	}
}

func TestRemoteEngineBulkAndErrorFallbacks(t *testing.T) {
	callCount := 0
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		callCount++
		var txReq remoteTxRequest
		_ = json.NewDecoder(req.Body).Decode(&txReq)
		stmt := txReq.Statements[0].Statement
		if callCount == 1 {
			// First call fails to exercise bulk error short-circuit.
			return nil, fmt.Errorf("boom")
		}
		if strings.Contains(stmt, "RETURN count(n)") || strings.Contains(stmt, "RETURN count(r)") {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
		}
		if strings.Contains(stmt, "MATCH (n) RETURN n") {
			return nil, fmt.Errorf("all nodes failed")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
			{map[string]interface{}{"id": "n1", "labels": []interface{}{"X"}, "properties": map[string]interface{}{"id": "n1"}}},
		}, nil)}, nil
	})

	if err := engine.BulkCreateNodes([]*Node{{ID: "n1"}}); err == nil {
		t.Fatalf("expected BulkCreateNodes error on first failure")
	}
	if err := engine.BulkCreateEdges([]*Edge{{ID: "e1", StartNode: "a", EndNode: "b", Type: "REL"}}); err != nil {
		t.Fatalf("BulkCreateEdges failed: %v", err)
	}
	if err := engine.BulkDeleteNodes([]NodeID{"n1"}); err != nil {
		t.Fatalf("BulkDeleteNodes failed: %v", err)
	}
	if err := engine.BulkDeleteEdges([]EdgeID{"e1"}); err != nil {
		t.Fatalf("BulkDeleteEdges failed: %v", err)
	}
	if got, err := engine.BatchGetNodes([]NodeID{"n1", "n2"}); err != nil {
		t.Fatalf("BatchGetNodes failed: %v", err)
	} else if len(got) == 0 {
		t.Fatalf("expected at least one batch node")
	}

	if nodes := engine.GetAllNodes(); len(nodes) != 0 {
		t.Fatalf("GetAllNodes should swallow errors and return empty slice")
	}
	if n, err := engine.NodeCount(); err != nil || n != 0 {
		t.Fatalf("expected NodeCount fallback to zero with nil error; n=%d err=%v", n, err)
	}
	if n, err := engine.EdgeCount(); err != nil || n != 0 {
		t.Fatalf("expected EdgeCount fallback to zero with nil error; n=%d err=%v", n, err)
	}
}

// ---------------------------------------------------------------------------
// Additional error branches
// ---------------------------------------------------------------------------

func TestRemoteEngineAdditionalErrorBranches(t *testing.T) {
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		var txReq remoteTxRequest
		_ = json.NewDecoder(req.Body).Decode(&txReq)
		stmt := txReq.Statements[0].Statement
		params := txReq.Statements[0].Parameters

		switch {
		case strings.Contains(stmt, "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1"):
			switch params["id"] {
			case "not-found":
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
			case "bad":
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{1}}, nil)}, nil
			default:
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
					{map[string]interface{}{"id": params["id"], "properties": map[string]interface{}{"id": params["id"]}}},
				}, nil)}, nil
			}
		case strings.Contains(stmt, "MATCH (n)-[r]->() WHERE n.id = $id RETURN r"),
			strings.Contains(stmt, "MATCH ()-[r]->(n) WHERE n.id = $id RETURN r"),
			strings.Contains(stmt, "MATCH (a)-[r]->(b) WHERE a.id = $start AND b.id = $end RETURN r"),
			strings.Contains(stmt, "MATCH ()-[r:`REL`]->() RETURN r"),
			strings.Contains(stmt, "MATCH ()-[r]->() RETURN r"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{},
				{1},
			}, nil)}, nil
		case strings.Contains(stmt, "MATCH (a)-[r:`REL`]->(b) WHERE a.id = $start AND b.id = $end RETURN r LIMIT 1"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{1}}, nil)}, nil
		case strings.Contains(stmt, "CREATE (a)-[r"):
			return nil, fmt.Errorf("create edge boom")
		case strings.Contains(stmt, "MATCH ()-[r]->() WHERE r.id = $id DELETE r"):
			return nil, fmt.Errorf("delete edge boom")
		case strings.Contains(stmt, "MATCH (n) WHERE n.id = $id DETACH DELETE n"):
			return nil, fmt.Errorf("delete node boom")
		default:
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
		}
	})

	if _, err := engine.GetNode("bad"); err == nil {
		t.Fatalf("expected node decode error")
	}
	if err := engine.DeleteNode("n1"); err == nil {
		t.Fatalf("expected DeleteNode error")
	}
	if err := engine.DeleteEdge("e1"); err == nil {
		t.Fatalf("expected DeleteEdge error")
	}
	if err := engine.BulkCreateEdges([]*Edge{{ID: "e1", StartNode: "a", EndNode: "b", Type: "REL"}}); err == nil {
		t.Fatalf("expected BulkCreateEdges error")
	}
	if err := engine.BulkDeleteNodes([]NodeID{"n1"}); err == nil {
		t.Fatalf("expected BulkDeleteNodes error")
	}
	if err := engine.BulkDeleteEdges([]EdgeID{"e1"}); err == nil {
		t.Fatalf("expected BulkDeleteEdges error")
	}
	if _, err := engine.GetOutgoingEdges("n1"); err == nil {
		t.Fatalf("expected GetOutgoingEdges conversion error")
	}
	if _, err := engine.GetIncomingEdges("n1"); err == nil {
		t.Fatalf("expected GetIncomingEdges conversion error")
	}
	if _, err := engine.GetEdgesBetween("a", "b"); err == nil {
		t.Fatalf("expected GetEdgesBetween conversion error")
	}
	if _, err := engine.GetEdgesByType("REL"); err == nil {
		t.Fatalf("expected GetEdgesByType conversion error")
	}
	if _, err := engine.AllEdges(); err == nil {
		t.Fatalf("expected AllEdges conversion error")
	}
	if edge := engine.GetEdgeBetween("a", "b", "REL"); edge != nil {
		t.Fatalf("expected nil edge for conversion error")
	}

	got, err := engine.BatchGetNodes([]NodeID{"ok", "not-found", "bad"})
	if err == nil {
		t.Fatalf("expected BatchGetNodes error")
	}
	if got != nil {
		t.Fatalf("expected nil map on BatchGetNodes hard error")
	}
}

func TestRemoteEngineRemainingBranches(t *testing.T) {
	// Query-level error branches across wrappers.
	queryErrEngine := makeEngineWithTransport(t, "http://remote.example", func(_ *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("query boom")
	})
	if _, err := queryErrEngine.GetNode("n1"); err == nil {
		t.Fatalf("expected GetNode query error")
	}
	if err := queryErrEngine.UpdateNode(&Node{ID: "n1"}); err == nil {
		t.Fatalf("expected UpdateNode query error")
	}
	if _, err := queryErrEngine.GetEdge("e1"); err == nil {
		t.Fatalf("expected GetEdge query error")
	}
	if err := queryErrEngine.UpdateEdge(&Edge{ID: "e1"}); err == nil {
		t.Fatalf("expected UpdateEdge query error")
	}
	if _, err := queryErrEngine.GetNodesByLabel("Person"); err == nil {
		t.Fatalf("expected GetNodesByLabel query error")
	}
	if _, err := queryErrEngine.GetOutgoingEdges("n1"); err == nil {
		t.Fatalf("expected GetOutgoingEdges query error")
	}
	if _, err := queryErrEngine.GetIncomingEdges("n1"); err == nil {
		t.Fatalf("expected GetIncomingEdges query error")
	}
	if _, err := queryErrEngine.GetEdgesBetween("a", "b"); err == nil {
		t.Fatalf("expected GetEdgesBetween query error")
	}
	if _, err := queryErrEngine.GetEdgesByType("REL"); err == nil {
		t.Fatalf("expected GetEdgesByType query error")
	}
	if _, err := queryErrEngine.AllEdges(); err == nil {
		t.Fatalf("expected AllEdges query error")
	}
	if _, err := queryErrEngine.GetFirstNodeByLabel("Person"); err == nil {
		t.Fatalf("expected GetFirstNodeByLabel query error")
	}
	if got := queryErrEngine.GetInDegree("n1"); got != 0 {
		t.Fatalf("expected zero indegree on query error")
	}
	if got := queryErrEngine.GetOutDegree("n1"); got != 0 {
		t.Fatalf("expected zero outdegree on query error")
	}
	if got := queryErrEngine.GetEdgeBetween("a", "b", "REL"); got != nil {
		t.Fatalf("expected nil edge on query error")
	}

	// CreateNode no-row and conversion error branches.
	noRowsEngine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		var txReq remoteTxRequest
		_ = json.NewDecoder(req.Body).Decode(&txReq)
		stmt := txReq.Statements[0].Statement
		if strings.HasPrefix(stmt, "CREATE (n") {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse(nil, nil)}, nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(1)}}, nil)}, nil
	})
	if _, err := noRowsEngine.CreateNode(&Node{Labels: []string{"X"}}); err == nil {
		t.Fatalf("expected CreateNode no-row error")
	}
	// Also cover BulkCreateNodes success path + node.ID empty branch in CreateNode.
	successEngine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		var txReq remoteTxRequest
		_ = json.NewDecoder(req.Body).Decode(&txReq)
		stmt := txReq.Statements[0].Statement
		if strings.HasPrefix(stmt, "CREATE (n") || strings.Contains(stmt, "CREATE (a)-[r") {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
				{map[string]interface{}{"id": "created", "properties": map[string]interface{}{"id": "created"}}},
			}, nil)}, nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{float64(1)}}, nil)}, nil
	})
	if err := successEngine.BulkCreateNodes([]*Node{{Labels: []string{"X"}, Properties: map[string]interface{}{}}}); err != nil {
		t.Fatalf("expected BulkCreateNodes success: %v", err)
	}
	if _, err := successEngine.CreateNode(&Node{Labels: []string{"X"}, Properties: map[string]interface{}{}}); err != nil {
		t.Fatalf("expected CreateNode success with empty id: %v", err)
	}
	if err := successEngine.CreateEdge(&Edge{StartNode: "a", EndNode: "b", Type: "REL"}); err != nil {
		t.Fatalf("expected CreateEdge success with empty id: %v", err)
	}
	if err := successEngine.CreateEdge(&Edge{
		ID:         "edge-with-props",
		StartNode:  "a",
		EndNode:    "b",
		Type:       "REL",
		Properties: map[string]interface{}{"weight": 0.5},
	}); err != nil {
		t.Fatalf("expected CreateEdge success with properties: %v", err)
	}

	mixedRowsEngine := makeEngineWithTransport(t, "http://remote.example", func(_ *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{
			{},
			{1},
		}, nil)}, nil
	})
	if _, err := mixedRowsEngine.GetNodesByLabel("Person"); err == nil {
		t.Fatalf("expected GetNodesByLabel conversion error")
	}

	badRowEngine := makeEngineWithTransport(t, "http://remote.example", func(_ *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: makeTXResponse([][]interface{}{{1}}, nil)}, nil
	})
	if _, err := badRowEngine.CreateNode(&Node{ID: "n1"}); err == nil {
		t.Fatalf("expected CreateNode conversion error")
	}
	if _, err := badRowEngine.GetFirstNodeByLabel("Person"); err == nil {
		t.Fatalf("expected GetFirstNodeByLabel conversion error")
	}
}

// ---------------------------------------------------------------------------
// HTTP queryBatch
// ---------------------------------------------------------------------------

func TestRemoteEngineHTTPQueryBatch(t *testing.T) {
	var capturedStmtCount int
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		var txReq remoteTxRequest
		_ = json.NewDecoder(req.Body).Decode(&txReq)
		capturedStmtCount = len(txReq.Statements)
		return &http.Response{StatusCode: 200, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`))}, nil
	})

	stmts := []remoteStatement{
		{Statement: "CREATE (n:A) SET n.x = 1"},
		{Statement: "CREATE (n:B) SET n.x = 2"},
		{Statement: "CREATE (n:C) SET n.x = 3"},
	}
	ctx := context.Background()
	if err := engine.transport.queryBatch(ctx, stmts); err != nil {
		t.Fatalf("queryBatch failed: %v", err)
	}
	if capturedStmtCount != 3 {
		t.Fatalf("expected 3 statements in single HTTP request, got %d", capturedStmtCount)
	}

	// Error propagation.
	errEngine := makeEngineWithTransport(t, "http://remote.example", func(_ *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("batch boom")
	})
	if err := errEngine.transport.queryBatch(ctx, stmts); err == nil {
		t.Fatalf("expected queryBatch error")
	}
}

func TestRemoteEngineHTTPExplicitTxLifecycle(t *testing.T) {
	var opened bool
	var executed bool
	var committed bool
	var rolledBack bool
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx"):
			opened = true
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"results":[],"errors":[],"commit":"http://remote.example/db/remote_db/tx/1/commit"}`,
				)),
			}, nil
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/1"):
			executed = true
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"results":[{"columns":["n"],"data":[{"row":[1]}]}],"errors":[]}`,
				)),
			}, nil
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/1/commit"):
			committed = true
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			}, nil
		case req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/1"):
			rolledBack = true
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			}, nil
		default:
			return &http.Response{
				StatusCode: 404,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			}, nil
		}
	})
	tx, err := engine.BeginCypherTx(context.Background())
	if err != nil {
		t.Fatalf("BeginCypherTx failed: %v", err)
	}
	cols, rows, err := tx.QueryCypher(context.Background(), "RETURN 1 AS n", nil)
	if err != nil {
		t.Fatalf("QueryCypher failed: %v", err)
	}
	if len(cols) != 1 || cols[0] != "n" || len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("unexpected query result: cols=%v rows=%v", cols, rows)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if !opened || !executed || !committed || rolledBack {
		t.Fatalf("unexpected lifecycle flags opened=%v executed=%v committed=%v rolledBack=%v", opened, executed, committed, rolledBack)
	}
}

func TestRemoteEngineHTTPExplicitTxRollbackLifecycle(t *testing.T) {
	var opened bool
	var executed bool
	var committed bool
	var rolledBack bool
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx"):
			opened = true
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"results":[],"errors":[],"commit":"http://remote.example/db/remote_db/tx/2/commit"}`,
				)),
			}, nil
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/2"):
			executed = true
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"results":[{"columns":["n"],"data":[{"row":[1]}]}],"errors":[]}`,
				)),
			}, nil
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/2/commit"):
			committed = true
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			}, nil
		case req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/2"):
			rolledBack = true
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			}, nil
		default:
			return &http.Response{
				StatusCode: 404,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			}, nil
		}
	})
	tx, err := engine.BeginCypherTx(context.Background())
	if err != nil {
		t.Fatalf("BeginCypherTx failed: %v", err)
	}
	if _, _, err := tx.QueryCypher(context.Background(), "RETURN 1 AS n", nil); err != nil {
		t.Fatalf("QueryCypher failed: %v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if !opened || !executed || committed || !rolledBack {
		t.Fatalf("unexpected lifecycle flags opened=%v executed=%v committed=%v rolledBack=%v", opened, executed, committed, rolledBack)
	}
}

func TestRemoteEngineQueryCypherAndHTTPClosedBranches(t *testing.T) {
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/commit"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(
				`{"results":[{"columns":["n"],"data":[{"row":[42]}]}],"errors":[]}`,
			))}, nil
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(
				`{"results":[],"errors":[],"commit":"http://remote.example/db/remote_db/tx/3/commit"}`,
			))}, nil
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/3/commit"):
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`))}, nil
		default:
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`))}, nil
		}
	})

	var nilCtx context.Context
	cols, rows, err := engine.QueryCypher(nilCtx, "RETURN 42 AS n", nil)
	if err != nil {
		t.Fatalf("QueryCypher with nil context failed: %v", err)
	}
	if len(cols) != 1 || cols[0] != "n" || len(rows) != 1 || rows[0][0].(float64) != 42 {
		t.Fatalf("unexpected query result cols=%v rows=%v", cols, rows)
	}

	tx, err := engine.BeginCypherTx(nilCtx)
	if err != nil {
		t.Fatalf("BeginCypherTx with nil context failed: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("closed Commit should be a no-op: %v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("closed Rollback should be a no-op: %v", err)
	}
	if _, _, err := tx.QueryCypher(context.Background(), "RETURN 1", nil); err == nil {
		t.Fatalf("expected QueryCypher on closed transaction to fail")
	}
}

func TestRemoteEngineHTTPBeginCypherTx_ErrorPaths(t *testing.T) {
	t.Run("response body read fails", func(t *testing.T) {
		engine := makeEngineWithTransport(t, "http://remote.example", func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &errReadCloser{}}, nil
		})
		_, err := engine.BeginCypherTx(context.Background())
		if err == nil || !strings.Contains(err.Error(), "read failed") {
			t.Fatalf("expected body read error, got: %v", err)
		}
	})

	t.Run("open response decode fails", func(t *testing.T) {
		engine := makeEngineWithTransport(t, "http://remote.example", func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"commit":`)),
			}, nil
		})
		_, err := engine.BeginCypherTx(context.Background())
		if err == nil || !strings.Contains(err.Error(), "remote tx open decode failed") {
			t.Fatalf("expected decode error, got: %v", err)
		}
	})

	t.Run("open response contains error payload", func(t *testing.T) {
		engine := makeEngineWithTransport(t, "http://remote.example", func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"errors":[{"code":"Neo.ClientError.Statement.InvalidSyntax","message":"bad query"}]}`,
				)),
			}, nil
		})
		_, err := engine.BeginCypherTx(context.Background())
		if err == nil || !strings.Contains(err.Error(), "InvalidSyntax") {
			t.Fatalf("expected open payload error, got: %v", err)
		}
	})

	t.Run("open response missing commit URL", func(t *testing.T) {
		engine := makeEngineWithTransport(t, "http://remote.example", func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[],"commit":"   "}`)),
			}, nil
		})
		_, err := engine.BeginCypherTx(context.Background())
		if err == nil || !strings.Contains(err.Error(), "empty commit URL") {
			t.Fatalf("expected empty commit URL error, got: %v", err)
		}
	})
}

func TestRemoteEngineHTTPQueryCypher_EmptyResultsBranches(t *testing.T) {
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/commit"):
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			}, nil
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx"):
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"results":[],"errors":[],"commit":"http://remote.example/db/remote_db/tx/4/commit"}`,
				)),
			}, nil
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx/4"):
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			}, nil
		default:
			return &http.Response{
				StatusCode: 404,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"results":[],"errors":[]}`)),
			}, nil
		}
	})

	cols, rows, err := engine.QueryCypher(context.Background(), "RETURN 1", nil)
	if err != nil {
		t.Fatalf("QueryCypher failed: %v", err)
	}
	if len(cols) != 0 || len(rows) != 0 {
		t.Fatalf("expected empty result set, got cols=%v rows=%v", cols, rows)
	}

	tx, err := engine.BeginCypherTx(context.Background())
	if err != nil {
		t.Fatalf("BeginCypherTx failed: %v", err)
	}
	cols, rows, err = tx.QueryCypher(context.Background(), "RETURN 1", nil)
	if err != nil {
		t.Fatalf("tx.QueryCypher failed: %v", err)
	}
	if len(cols) != 0 || len(rows) != 0 {
		t.Fatalf("expected empty tx result set, got cols=%v rows=%v", cols, rows)
	}
}

func TestRemoteEngineBatchGetNodes_BranchCoverage(t *testing.T) {
	t.Run("skip not found entries and keep valid node", func(t *testing.T) {
		eng := &RemoteEngine{
			transport: &fakeRemoteTransport{
				queryFn: func(_ context.Context, _ string, params map[string]interface{}) ([][]interface{}, error) {
					id := params["id"].(string)
					switch id {
					case "n1":
						return nil, ErrNotFound
					case "n2":
						return [][]interface{}{
							{map[string]interface{}{"id": "n2", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": "n2"}}},
						}, nil
					default:
						return nil, nil
					}
				},
			},
			schema: NewSchemaManager(),
		}

		nodes, err := eng.BatchGetNodes([]NodeID{"n1", "n2"})
		if err != nil {
			t.Fatalf("BatchGetNodes failed: %v", err)
		}
		if len(nodes) != 1 {
			t.Fatalf("expected exactly one node, got %d", len(nodes))
		}
		if got := nodes["n2"]; got == nil || got.ID != "n2" {
			t.Fatalf("expected node n2, got %#v", got)
		}
	})

	t.Run("propagate non-notfound query error", func(t *testing.T) {
		eng := &RemoteEngine{
			transport: &fakeRemoteTransport{
				queryFn: func(_ context.Context, _ string, _ map[string]interface{}) ([][]interface{}, error) {
					return nil, fmt.Errorf("query exploded")
				},
			},
			schema: NewSchemaManager(),
		}

		_, err := eng.BatchGetNodes([]NodeID{"n1"})
		if err == nil || !strings.Contains(err.Error(), "query exploded") {
			t.Fatalf("expected query error, got: %v", err)
		}
	})
}
