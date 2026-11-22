package server

// v1.5.2 verification (scratch, test-only): shared fixture of the
// v152_pr1826_release tests. Statements run over HTTP and Bolt, auto-commit and
// in explicit transactions.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type p26Env struct {
	t        *testing.T
	db       *nornicdb.DB
	server   *Server
	token    string
	driver   neo4j.DriverWithContext
	database string
}

func p26Bolt(t *testing.T, server *Server, authenticator *auth.Authenticator) neo4j.DriverWithContext {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config := bolt.DefaultConfig()
	adapter := bolt.NewAuthenticatorAdapter(authenticator)
	adapter.SetGetEffectivePermissions(server.GetEffectivePermissions)
	config.Authenticator = adapter
	config.RequireAuth = true
	config.DatabaseExecutorOptions = server.DatabaseScopedExecutorOptions
	boltServer := bolt.NewWithDatabaseManager(config, nil, server.GetDatabaseManager())
	boltServer.SetDatabaseAccessModeResolver(server.GetDatabaseAccessModeForRoles)
	boltServer.SetResolvedAccessResolver(server.GetResolvedAccessForRoles)
	boltServer.SetProcedureGrantResolver(server.ProcedureGrantChecker)
	finished := make(chan error, 1)
	go func() { finished <- boltServer.Serve(listener) }()
	t.Cleanup(func() {
		_ = boltServer.Close()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
		}
	})
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.BasicAuth("admin", "password123", ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return driver
}

func p26Serve(t *testing.T, configureDB func(*nornicdb.Config)) *p26Env {
	t.Helper()
	server, authenticator := setupTestServerWithDatabaseConfig(t, configureDB, nil)
	return &p26Env{t: t, db: server.db, server: server, token: "Bearer " + getAuthToken(t, authenticator, "admin"),
		driver: p26Bolt(t, server, authenticator), database: server.dbManager.DefaultDatabaseName()}
}

// p26Open serves an existing (or new) data directory.
func p26Open(t *testing.T, dir string) (*p26Env, error) {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(dir, config)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = db.Close() })
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")},
		storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticator.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin}); err != nil {
		t.Logf("NOTE admin user: %v", err)
	}
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	server, err := New(db, authenticator, serverConfig)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { stopTestServer(t, server) })
	return &p26Env{t: t, db: db, server: server, token: "Bearer " + getAuthToken(t, authenticator, "admin"),
		driver: p26Bolt(t, server, authenticator), database: server.dbManager.DefaultDatabaseName()}, nil
}

func p26Value(v any) any {
	switch x := v.(type) {
	case dbtype.Node:
		return p26Value(x.Props)
	case dbtype.Relationship:
		return p26Value(x.Props)
	case dbtype.Path:
		nodes := make([]any, len(x.Nodes))
		for i := range x.Nodes {
			nodes[i] = p26Value(x.Nodes[i])
		}
		return nodes
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = p26Value(x[i])
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k := range x {
			out[k] = p26Value(x[k])
		}
		return out
	}
	return v
}

func p26Rows(rows [][]any) string {
	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		encoded, err := json.Marshal(p26Value(row))
		if err != nil {
			encoded = []byte("MARSHAL " + err.Error())
		}
		lines = append(lines, string(encoded))
	}
	sort.Strings(lines)
	return strings.Join(lines, " ")
}

func (e *p26Env) httpPost(path, statement string, params map[string]any) string {
	response := makeRequest(e.t, e.server, http.MethodPost, path, map[string]any{"statements": []any{map[string]any{"statement": statement, "parameters": params}}}, e.token)
	var result TransactionResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		return fmt.Sprintf("UNDECODABLE status=%d body=%.200q", response.Code, response.Body.String())
	}
	if len(result.Errors) > 0 {
		return "ERR " + result.Errors[0].Code + ": " + result.Errors[0].Message
	}
	var rows [][]any
	if len(result.Results) > 0 {
		for _, d := range result.Results[0].Data {
			rows = append(rows, d.Row)
		}
	}
	return p26Rows(rows)
}

func p26Collect(result neo4j.ResultWithContext, err error) string {
	if err != nil {
		return "ERR " + err.Error()
	}
	records, err := result.Collect(context.Background())
	if err != nil {
		return "ERR " + err.Error()
	}
	var rows [][]any
	for _, r := range records {
		rows = append(rows, r.Values)
	}
	return p26Rows(rows)
}

type p26Session struct {
	name string
	run  func(string, map[string]any) string
	end  func()
}

func (e *p26Env) httpAuto() p26Session {
	return p26Session{"http-auto", func(q string, p map[string]any) string { return e.httpPost("/db/"+e.database+"/tx/commit", q, p) }, func() {}}
}

func (e *p26Env) boltAuto() p26Session {
	return p26Session{"bolt-auto", func(q string, p map[string]any) string {
		s := e.driver.NewSession(context.Background(), neo4j.SessionConfig{DatabaseName: e.database})
		defer s.Close(context.Background())
		return p26Collect(s.Run(context.Background(), q, p))
	}, func() {}}
}

func (e *p26Env) boltTx() p26Session {
	e.t.Helper()
	s := e.driver.NewSession(context.Background(), neo4j.SessionConfig{DatabaseName: e.database})
	tx, err := s.BeginTransaction(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return p26Session{"bolt-explicit", func(q string, p map[string]any) string { return p26Collect(tx.Run(context.Background(), q, p)) },
		func() { _ = tx.Rollback(context.Background()); _ = s.Close(context.Background()) }}
}

func (e *p26Env) httpTx() p26Session {
	e.t.Helper()
	opened := makeRequest(e.t, e.server, http.MethodPost, "/db/"+e.database+"/tx", map[string]any{"statements": []any{}}, e.token)
	location, err := url.Parse(opened.Header().Get("Location"))
	if err != nil || location.Path == "" {
		e.t.Fatalf("open http transaction: %v %s", err, opened.Body.String())
	}
	return p26Session{"http-explicit", func(q string, p map[string]any) string { return e.httpPost(location.Path, q, p) },
		func() { makeRequest(e.t, e.server, http.MethodDelete, location.Path, nil, e.token) }}
}

func (e *p26Env) all() []p26Session {
	return []p26Session{e.httpAuto(), e.boltAuto(), e.httpTx(), e.boltTx()}
}

func (e *p26Env) must(q string, p map[string]any) {
	e.t.Helper()
	if out := e.boltAuto().run(q, p); strings.HasPrefix(out, "ERR") {
		e.t.Fatalf("setup %s: %s", q, out)
	}
}

// check compares the statement's answer on every path with want.
func (e *p26Env) check(defect, where, q, want string) {
	e.t.Helper()
	for _, s := range e.all() {
		got := s.run(q, nil)
		s.end()
		verdict := "ok"
		if got != want {
			verdict = "WRONG"
			e.t.Errorf("P26 %s %s %s: %s\n  expected: %s\n  actual:   %.400s", defect, where, s.name, q, want, got)
		}
		e.t.Logf("P26ROW %s %s %s %s %q -> %.200s", defect, where, s.name, verdict, q, got)
	}
}

// boltCommit runs one statement in an explicit transaction and commits it.
func (e *p26Env) boltCommit(q string) string {
	e.t.Helper()
	ctx := context.Background()
	s := e.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: e.database})
	defer s.Close(ctx)
	tx, err := s.BeginTransaction(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	out := p26Collect(tx.Run(ctx, q, nil))
	if err := tx.Commit(ctx); err != nil {
		return out + " COMMIT " + err.Error()
	}
	return out + " committed"
}
