package server

// Batch 3b helpers, copied from batch 2 zz_ei2_server_test.go (research only,
// untracked). Written to compile on both the final and the previous build.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime/pprof"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/stretchr/testify/require"
)

type zz3bOutcome struct {
	Rows string
	Err  string
}

func (o zz3bOutcome) String() string {
	if o.Err != "" {
		return "ERROR " + o.Err
	}
	return o.Rows
}

type zz3bEnv struct {
	t      *testing.T
	peer   *httptest.Server
	token  string
	driver neo4j.DriverWithContext
}

func zz3bStart(t *testing.T) *zz3bEnv {
	t.Helper()
	srv, authenticator := setupTestServer(t)
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	token := getAuthToken(t, authenticator, "admin")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bs := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, srv.GetDatabaseManager())
	done := make(chan error, 1)
	go func() { done <- bs.Serve(listener) }()
	t.Cleanup(func() { _ = bs.Close(); <-done })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return &zz3bEnv{t: t, peer: peer, token: token, driver: driver}
}

type zz3bHTTPResponse struct {
	Commit  string `json:"commit"`
	Errors  []struct{ Code, Message string }
	Results []struct {
		Columns []string
		Data    []struct{ Row []any }
	}
}

func (e *zz3bEnv) post(ctx context.Context, path, q string, params map[string]any) (int, zz3bHTTPResponse, error) {
	statements := []map[string]any{}
	if q != "" {
		statements = append(statements, map[string]any{"statement": q, "parameters": params})
	}
	body, _ := json.Marshal(map[string]any{"statements": statements})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.peer.URL+path, bytes.NewReader(body))
	if err != nil {
		return 0, zz3bHTTPResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.token)
	res, err := e.peer.Client().Do(req)
	if err != nil {
		return 0, zz3bHTTPResponse{}, err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var out zz3bHTTPResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		out.Errors = append(out.Errors, struct{ Code, Message string }{"NON_JSON", strings.TrimSpace(string(raw))})
	}
	return res.StatusCode, out, nil
}

func zz3bHTTPOutcome(status int, out zz3bHTTPResponse, err error) zz3bOutcome {
	if err != nil {
		return zz3bOutcome{Err: "TRANSPORT " + err.Error()}
	}
	if len(out.Errors) > 0 {
		return zz3bOutcome{Err: fmt.Sprintf("HTTP %d %s: %s", status, out.Errors[0].Code, out.Errors[0].Message)}
	}
	rows := [][]any{}
	if len(out.Results) > 0 {
		for _, r := range out.Results[0].Data {
			rows = append(rows, r.Row)
		}
	}
	b, _ := json.Marshal(rows)
	return zz3bOutcome{Rows: string(b)}
}

func (e *zz3bEnv) autocommit(q string, params map[string]any) zz3bOutcome {
	return zz3bHTTPOutcome(e.post(context.Background(), "/db/nornic/tx/commit", q, params))
}

func (e *zz3bEnv) run(route, q string, params map[string]any) zz3bOutcome {
	ctx := context.Background()
	switch route {
	case "HTTP-auto":
		return e.autocommit(q, params)
	case "HTTP-tx":
		status, opened, err := e.post(ctx, "/db/nornic/tx", "", nil)
		if err != nil || len(opened.Errors) > 0 {
			return zz3bHTTPOutcome(status, opened, err)
		}
		u, err := url.Parse(opened.Commit)
		if err != nil {
			return zz3bOutcome{Err: "bad commit url " + opened.Commit}
		}
		return zz3bHTTPOutcome(e.post(ctx, u.Path, q, params))
	case "Bolt-auto", "Bolt-tx":
		session := e.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		defer session.Close(ctx)
		var result neo4j.ResultWithContext
		var err error
		var tx neo4j.ExplicitTransaction
		if route == "Bolt-tx" {
			tx, err = session.BeginTransaction(ctx)
			if err != nil {
				return zz3bBoltError(err)
			}
			defer tx.Close(ctx)
			result, err = tx.Run(ctx, q, params)
		} else {
			result, err = session.Run(ctx, q, params)
		}
		if err != nil {
			return zz3bBoltError(err)
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return zz3bBoltError(err)
		}
		if tx != nil {
			if err := tx.Commit(ctx); err != nil {
				return zz3bBoltError(err)
			}
		}
		rows := [][]any{}
		for _, r := range records {
			rows = append(rows, r.Values)
		}
		b, _ := json.Marshal(rows)
		return zz3bOutcome{Rows: string(b)}
	}
	panic(route)
}

func zz3bBoltError(err error) zz3bOutcome {
	var ne *neo4j.Neo4jError
	if ok := zz3bAsNeo4jError(err, &ne); ok {
		return zz3bOutcome{Err: ne.Code + ": " + ne.Msg}
	}
	return zz3bOutcome{Err: "TRANSPORT " + err.Error()}
}

func zz3bAsNeo4jError(err error, target **neo4j.Neo4jError) bool {
	for err != nil {
		if ne, ok := err.(*neo4j.Neo4jError); ok {
			*target = ne
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}


// zz3bPhase classifies the statement goroutines of one dump.
func zz3bPhase() (string, string) {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	phase, top := "gone", ""
	for _, g := range strings.Split(b.String(), "\n\n") {
		if !strings.Contains(g, "nornicdb/pkg/cypher.") || strings.Contains(g, "zz_3b_") {
			continue
		}
		var frames []string
		committing := false
		for _, line := range strings.Split(g, "\n") {
			l := strings.TrimSpace(line)
			if !strings.HasPrefix(l, "github.com/orneryd/nornicdb/pkg/") {
				continue
			}
			fn := l
			if i := strings.Index(fn, "("); i > 0 {
				fn = fn[:i]
			}
			fn = strings.TrimPrefix(fn, "github.com/orneryd/nornicdb/pkg/")
			if len(frames) < 6 {
				frames = append(frames, fn)
			}
			lower := strings.ToLower(fn)
			inEngine := strings.HasPrefix(fn, "cypher.") || strings.HasPrefix(fn, "storage.") || strings.HasPrefix(fn, "txsession.") || strings.HasPrefix(fn, "txphase.")
			if inEngine && strings.Contains(lower, "commit") && !strings.Contains(lower, "autocommit") {
				committing = true
			}
		}
		if committing {
			return "committing", strings.Join(frames, " < ")
		}
		phase, top = "executing", strings.Join(frames, " < ")
	}
	return phase, top
}

