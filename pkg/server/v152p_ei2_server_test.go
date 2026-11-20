package server

// Batch 2 earlier-issue probes through the public routes (research only,
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
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/stretchr/testify/require"
)

type zzei2Outcome struct {
	Rows string
	Err  string
}

func (o zzei2Outcome) String() string {
	if o.Err != "" {
		return "ERROR " + o.Err
	}
	return o.Rows
}

type zzei2Env struct {
	t      *testing.T
	peer   *httptest.Server
	token  string
	driver neo4j.DriverWithContext
}

func zzei2Start(t *testing.T) *zzei2Env {
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
	return &zzei2Env{t: t, peer: peer, token: token, driver: driver}
}

type zzei2HTTPResponse struct {
	Commit  string `json:"commit"`
	Errors  []struct{ Code, Message string }
	Results []struct {
		Columns []string
		Data    []struct{ Row []any }
	}
}

func (e *zzei2Env) post(ctx context.Context, path, q string, params map[string]any) (int, zzei2HTTPResponse, error) {
	statements := []map[string]any{}
	if q != "" {
		statements = append(statements, map[string]any{"statement": q, "parameters": params})
	}
	body, _ := json.Marshal(map[string]any{"statements": statements})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.peer.URL+path, bytes.NewReader(body))
	if err != nil {
		return 0, zzei2HTTPResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.token)
	res, err := e.peer.Client().Do(req)
	if err != nil {
		return 0, zzei2HTTPResponse{}, err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var out zzei2HTTPResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		out.Errors = append(out.Errors, struct{ Code, Message string }{"NON_JSON", strings.TrimSpace(string(raw))})
	}
	return res.StatusCode, out, nil
}

func zzei2HTTPOutcome(status int, out zzei2HTTPResponse, err error) zzei2Outcome {
	if err != nil {
		return zzei2Outcome{Err: "TRANSPORT " + err.Error()}
	}
	if len(out.Errors) > 0 {
		return zzei2Outcome{Err: fmt.Sprintf("HTTP %d %s: %s", status, out.Errors[0].Code, out.Errors[0].Message)}
	}
	rows := [][]any{}
	if len(out.Results) > 0 {
		for _, r := range out.Results[0].Data {
			rows = append(rows, r.Row)
		}
	}
	b, _ := json.Marshal(rows)
	return zzei2Outcome{Rows: string(b)}
}

func (e *zzei2Env) autocommit(q string, params map[string]any) zzei2Outcome {
	return zzei2HTTPOutcome(e.post(context.Background(), "/db/nornic/tx/commit", q, params))
}

func (e *zzei2Env) run(route, q string, params map[string]any) zzei2Outcome {
	ctx := context.Background()
	switch route {
	case "HTTP-auto":
		return e.autocommit(q, params)
	case "HTTP-tx":
		status, opened, err := e.post(ctx, "/db/nornic/tx", "", nil)
		if err != nil || len(opened.Errors) > 0 {
			return zzei2HTTPOutcome(status, opened, err)
		}
		u, err := url.Parse(opened.Commit)
		if err != nil {
			return zzei2Outcome{Err: "bad commit url " + opened.Commit}
		}
		return zzei2HTTPOutcome(e.post(ctx, u.Path, q, params))
	case "Bolt-auto", "Bolt-tx":
		session := e.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		defer session.Close(ctx)
		var result neo4j.ResultWithContext
		var err error
		var tx neo4j.ExplicitTransaction
		if route == "Bolt-tx" {
			tx, err = session.BeginTransaction(ctx)
			if err != nil {
				return zzei2BoltError(err)
			}
			defer tx.Close(ctx)
			result, err = tx.Run(ctx, q, params)
		} else {
			result, err = session.Run(ctx, q, params)
		}
		if err != nil {
			return zzei2BoltError(err)
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return zzei2BoltError(err)
		}
		if tx != nil {
			if err := tx.Commit(ctx); err != nil {
				return zzei2BoltError(err)
			}
		}
		rows := [][]any{}
		for _, r := range records {
			rows = append(rows, r.Values)
		}
		b, _ := json.Marshal(rows)
		return zzei2Outcome{Rows: string(b)}
	}
	panic(route)
}

func zzei2BoltError(err error) zzei2Outcome {
	var ne *neo4j.Neo4jError
	if ok := zzei2AsNeo4jError(err, &ne); ok {
		return zzei2Outcome{Err: ne.Code + ": " + ne.Msg}
	}
	return zzei2Outcome{Err: "TRANSPORT " + err.Error()}
}

func zzei2AsNeo4jError(err error, target **neo4j.Neo4jError) bool {
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

type zzei2Case struct {
	issue, name string
	setup       []string
	query       string
	params      map[string]any
	check       string
	want        string // exact rows JSON, or "ERROR" for any error
	wantCheck   string
	routes      []string
}

var zzei2AllRoutes = []string{"HTTP-auto", "HTTP-tx", "Bolt-auto", "Bolt-tx"}

func zzei2Cases() []zzei2Case {
	endSeedSetup := []string{
		"CREATE (a:P {id:1})-[:SELF]->(b:P {id:2}), (c:P {id:3})-[:SELF]->(c)",
		"UNWIND range(4, 250) AS id CREATE (:P {id:id})",
		"CREATE INDEX p_id FOR (p:P) ON (p.id)",
	}
	explain := func(name, q, want string) zzei2Case {
		return zzei2Case{issue: "L520", name: name, query: q, want: want, check: "SHOW CONSTRAINTS YIELD name RETURN count(*) AS c", wantCheck: "[[0]]"}
	}
	return []zzei2Case{
		{issue: "L510", name: "foreach-listed", query: "CREATE (n:ZZF {v:0}) WITH n CALL (n) { FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v", want: "[[2]]"},
		{issue: "L510", name: "foreach-all", query: "CREATE (n:ZZF {v:0}) WITH n CALL (*) { FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v", want: "[[2]]"},
		{issue: "L510", name: "foreach-horizon", query: "CREATE (n:ZZF {v:0}) WITH n CALL (n) { WITH 1 AS marker FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v", want: "[[2]]"},
		{issue: "L510", name: "foreach-legacy", query: "CREATE (n:ZZF {v:0}) WITH n CALL { WITH n FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v", want: "[[2]]"},
		{issue: "L511", name: "star-in-transactions", query: "UNWIND [1,2] AS x CALL { WITH * CREATE (:ZZB {v:x}) RETURN x AS value } IN TRANSACTIONS OF 1 ROW RETURN value ORDER BY value", want: "[[1],[2]]", check: "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v", wantCheck: "[[1],[2]]", routes: []string{"HTTP-auto", "Bolt-auto"}},
		{issue: "L511", name: "named-in-transactions(control)", query: "UNWIND [1,2] AS x CALL { WITH x CREATE (:ZZB {v:x}) RETURN x AS value } IN TRANSACTIONS OF 1 ROW RETURN value ORDER BY value", want: "[[1],[2]]", check: "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v", wantCheck: "[[1],[2]]", routes: []string{"HTTP-auto", "Bolt-auto"}},
		{issue: "L511", name: "star-no-batch", query: "UNWIND [1,2] AS x CALL { WITH * CREATE (:ZZB {v:x}) RETURN x AS value } RETURN value ORDER BY value", want: "[[1],[2]]"},
		{issue: "L517", name: "nested-local-union", setup: []string{"CREATE (:ZZN {v:1}), (:ZZN {v:2})"}, query: "MATCH (n:ZZN {v:1}) CALL { CALL () { MATCH (n:ZZN) RETURN count(n) AS k } MATCH (n:ZZN) RETURN n.v AS v UNION WITH n RETURN n.v AS v } RETURN v ORDER BY v", want: "[[1],[2]]"},
		{issue: "L515", name: "end-seed-repeated", setup: endSeedSetup, query: "MATCH (p)-[:SELF]->(p:P) RETURN p.id ORDER BY p.id LIMIT 1", want: "[[3]]"},
		{issue: "L515", name: "start-seed-repeated(control)", setup: endSeedSetup, query: "MATCH (p:P)-[:SELF]->(p) RETURN p.id ORDER BY p.id LIMIT 1", want: "[[3]]"},
		explain("node_key", "EXPLAIN CREATE CONSTRAINT zz_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY", "[]"),
		explain("profile_node_key", "PROFILE CREATE CONSTRAINT zz_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY", "[]"),
		explain("relationship_key", "EXPLAIN CREATE CONSTRAINT zz_key FOR ()-[r:Review]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY", "[]"),
		explain("unique", "EXPLAIN CREATE CONSTRAINT zz_key FOR (n:Review) REQUIRE (n.a, n.b) IS UNIQUE", "[]"),
		explain("missing_options_parameter", "EXPLAIN CREATE CONSTRAINT zz_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY OPTIONS $options", "[]"),
		explain("unevaluated_options", "EXPLAIN CREATE CONSTRAINT zz_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY OPTIONS {indexProvider: toString(1/0)}", "[]"),
		explain("wrong_variable", "EXPLAIN CREATE CONSTRAINT zz_key FOR (n:Review) REQUIRE (m.a, m.b) IS NODE KEY", "ERROR"),
		explain("nonproperty", "EXPLAIN CREATE CONSTRAINT zz_key FOR (n:Review) REQUIRE (n.a, 7) IS NODE KEY", "ERROR"),
		{issue: "L526", name: "periodic-iterate-outer-param", query: "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS i RETURN i', 'CREATE (:ZZIter {i: i, v: $v})', {batchSize: 1, params: {v: $v}}) YIELD total RETURN total", params: map[string]any{"v": 7}, want: "[[3]]", check: "MATCH (n:ZZIter) RETURN n.i AS i, n.v AS v ORDER BY i", wantCheck: "[[1,7],[2,7],[3,7]]"},
		{issue: "L526", name: "periodic-iterate-config-over-outer", query: "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS i RETURN i', 'CREATE (:ZZIter {i: i, v: $v})', {batchSize: 1, params: {v: 7}}) YIELD total, failedBatches, errorMessages RETURN total, failedBatches, errorMessages", params: map[string]any{"v": 1}, want: "[[3,0,{}]]", check: "MATCH (n:ZZIter) RETURN n.i AS i, n.v AS v ORDER BY i", wantCheck: "[[1,7],[2,7],[3,7]]"},
		{issue: "L526", name: "periodic-iterate-config-param-expression", query: "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS i RETURN i', 'CREATE (:ZZIter {i: i, v: $w})', {batchSize: 1, params: {w: $v + 1}}) YIELD total, failedBatches, errorMessages RETURN total, failedBatches, errorMessages", params: map[string]any{"v": 7}, want: "[[3,0,{}]]", check: "MATCH (n:ZZIter) RETURN n.i AS i, n.v AS v ORDER BY i", wantCheck: "[[1,8],[2,8],[3,8]]"},
		{issue: "L526", name: "periodic-iterate-literal(control)", query: "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS i RETURN i', 'CREATE (:ZZIter {i: i, v: $v})', {batchSize: 1, params: {v: 7}}) YIELD total RETURN total", want: "[[3]]", check: "MATCH (n:ZZIter) RETURN n.i AS i, n.v AS v ORDER BY i", wantCheck: "[[1,7],[2,7],[3,7]]"},
	}
}

func TestZZEI2PublicRoutes(t *testing.T) {
	for _, c := range zzei2Cases() {
		routes := c.routes
		if routes == nil {
			routes = zzei2AllRoutes
		}
		for _, route := range routes {
			t.Run(c.issue+"/"+c.name+"/"+route, func(t *testing.T) {
				env := zzei2Start(t)
				for _, s := range c.setup {
					out := env.autocommit(s, nil)
					require.Empty(t, out.Err, s)
				}
				got := env.run(route, c.query, c.params)
				line := fmt.Sprintf("ZZEI2 ROUTE %s %s %s: %s", c.issue, c.name, route, got)
				ok := got.Err == "" && got.Rows == c.want
				if c.want == "ERROR" {
					ok = got.Err != ""
				}
				if c.check != "" {
					checked := env.autocommit(c.check, nil)
					line += " | check " + checked.String()
					if checked.Err != "" || checked.Rows != c.wantCheck {
						ok = false
					}
				}
				t.Log(line)
				if !ok {
					t.Errorf("MISMATCH %s want %s check want %s", line, c.want, c.wantCheck)
				}
			})
		}
	}
}

// L506: HTTP explicit transaction label scan after a peer suppression.
func TestZZEI2HTTPSuppressedLabelScan(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, pending := range []bool{false, true} {
			if pending && !explicit {
				continue
			}
			t.Run(fmt.Sprintf("explicit=%t/pending=%t", explicit, pending), func(t *testing.T) {
				env := zzei2Start(t)
				require.Empty(t, env.autocommit("CREATE (:ZZHidden {id:1})", nil).Err)
				path := "/db/nornic/tx/commit"
				if explicit {
					_, opened, err := env.post(context.Background(), "/db/nornic/tx", "", nil)
					require.NoError(t, err)
					require.Empty(t, opened.Errors)
					u, err := url.Parse(opened.Commit)
					require.NoError(t, err)
					// Keep the transaction open: post statements to the tx path, not commit.
					path = strings.TrimSuffix(u.Path, "/commit")
				}
				request := func(q string) zzei2Outcome {
					return zzei2HTTPOutcome(env.post(context.Background(), path, q, nil))
				}
				if pending {
					require.Empty(t, request("MATCH (n:ZZHidden) SET n.touched=true").Err)
				}
				first := request("MATCH (n:ZZHidden) RETURN n.id")
				require.Equal(t, "[[1]]", first.String())
				suppress := env.autocommit("MATCH (n:ZZHidden) CALL kp.suppress(n)", nil)
				control := env.autocommit("MATCH (n:ZZHidden) RETURN n.id", nil)
				after := request("MATCH (n:ZZHidden) RETURN n.id")
				t.Logf("ZZEI2 L506 HTTP explicit=%t pending=%t: suppress=%s fresh autocommit=%s same-transaction rescan=%s", explicit, pending, suppress, control, after)
				require.Empty(t, suppress.Err)
				require.Equal(t, "[]", control.String(), "fresh autocommit control")
				require.Equal(t, "[]", after.String(), "live suppression must apply to transaction scans")
			})
		}
	}
}

// L527: a DELETE page through /tx/commit whose response is a timeout or whose
// client disconnects; the response and the stored outcome must agree.
func TestZZEI2HTTPDeleteTimeoutOutcome(t *testing.T) {
	type tcase struct {
		name          string
		serverTimeout string
		clientTimeout time.Duration
		spin          int
		nodes         int
	}
	cases := []tcase{
		{"server-timeout-after-delete", "300ms", 0, 6000000, 2000},
		{"client-disconnect-after-delete", "", 300 * time.Millisecond, 6000000, 2000},
		{"server-timeout-large-delete", "100ms", 0, 0, 60000},
		{"client-disconnect-large-delete", "", 100 * time.Millisecond, 0, 60000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.serverTimeout != "" {
				t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", c.serverTimeout)
			}
			env := zzei2Start(t)
			// Seed through Bolt: the short HTTP transaction timeout under test
			// must not apply to the setup.
			seed := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": c.nodes})
			require.Empty(t, seed.Err)
			count := func() string { return env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String() }
			require.Equal(t, fmt.Sprintf("[[%d]]", c.nodes), count())
			q := "MATCH (n:ZZDel) WITH n LIMIT 2000 DELETE n RETURN count(*) AS deleted"
			if c.nodes > 2000 {
				q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
			}
			if c.spin > 0 {
				q = "MATCH (n:ZZDel) WITH n LIMIT 2000 DELETE n WITH count(*) AS deleted UNWIND range(1, $spin) AS x WITH deleted, count(x) AS spun RETURN deleted, spun"
			}
			ctx := context.Background()
			var cancel context.CancelFunc = func() {}
			if c.clientTimeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, c.clientTimeout)
			}
			start := time.Now()
			status, out, err := env.post(ctx, "/db/nornic/tx/commit", q, map[string]any{"spin": c.spin})
			cancel()
			elapsed := time.Since(start)
			response := zzei2HTTPOutcome(status, out, err)
			immediately := count()
			// Let any still-running statement settle.
			var settled string
			last := ""
			stable := 0
			for i := 0; i < 60 && stable < 5; i++ {
				time.Sleep(200 * time.Millisecond)
				settled = count()
				if settled == last {
					stable++
				} else {
					stable = 0
				}
				last = settled
			}
			deleted := settled != fmt.Sprintf("[[%d]]", c.nodes)
			success := err == nil && status == 200 && len(out.Errors) == 0
			t.Logf("ZZEI2 L527 %s: status=%d after %s response=%s; count right after=%s; settled count=%s; deletions committed=%t",
				c.name, status, elapsed.Round(time.Millisecond), response, immediately, settled, deleted)
			if deleted && !success {
				t.Errorf("the response reported failure (%s) but the deletions committed", response)
			}
			if !deleted && success {
				t.Errorf("the response reported success but nothing was deleted")
			}
		})
	}
}
