package server

// Batch-3b probes against the lab's leased Neo4j reference (research only,
// untracked; run with tnrun --ref neo-ref, which sets
// NORNICDB_NEO4J_REFERENCE_URI / _HTTP_URI).
//   TestZZ3BNeo4jTimeoutDelete  packet K row 72: the same HTTP auto-commit
//       DELETE under db.transaction.timeout set to the values the NornicDB
//       runs use; answer, its time, node count after.
//   TestZZ3BNeo4jNOR27          packet J row 10: the three statements.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func zz3bNeo(t *testing.T) (neo4j.DriverWithContext, string) {
	boltURI, httpURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"), os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if boltURI == "" || httpURI == "" {
		t.Skip("no leased Neo4j reference (run with tnrun --ref neo-ref)")
	}
	driver, err := neo4j.NewDriverWithContext(boltURI, neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return driver, httpURI
}

func zz3bNeoExec(t *testing.T, d neo4j.DriverWithContext, q string, p map[string]any) ([]*neo4j.Record, error) {
	res, err := neo4j.ExecuteQuery(context.Background(), d, q, p, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		return nil, err
	}
	return res.Records, nil
}

func zz3bNeoRows(recs []*neo4j.Record, err error) string {
	if err != nil {
		return "ERROR " + err.Error()
	}
	rows := [][]any{}
	for _, r := range recs {
		rows = append(rows, r.Values)
	}
	b, _ := json.Marshal(rows)
	return string(b)
}

func zz3bNeoPost(httpURI, q string, limit time.Duration) (int, string, time.Duration) {
	body, _ := json.Marshal(map[string]any{"statements": []map[string]any{{"statement": q}}})
	req, _ := http.NewRequest(http.MethodPost, strings.TrimRight(httpURI, "/")+"/db/neo4j/tx/commit", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if limit > 0 {
		// Neo4j's per-transaction timeout for the HTTP API, in milliseconds.
		req.Header.Set("max-execution-time", fmt.Sprint(limit.Milliseconds()))
	}
	start := time.Now()
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "TRANSPORT " + err.Error(), time.Since(start)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, strings.TrimSpace(string(raw)), time.Since(start)
}

type zz3bNeoPlan struct {
	nodes  int
	limits []string // durations, or "f0.30" for a fraction of the longest unconstrained run
}

var zz3bNeoPlanDefault = []zz3bNeoPlan{
	{2000, []string{"100ms", "250ms", "500ms", "1s"}},
	{60000, []string{"1s", "2s", "5s"}},
}

func TestZZ3BNeo4jTimeoutDelete(t *testing.T) { zz3bNeoTimeout(t, zz3bNeoPlanDefault) }

// Limits that fall inside the statement on Neo4j, whose 60,000-node delete
// takes 0.3-0.5 s.
func TestZZ3BNeo4jTimeoutInside(t *testing.T) {
	zz3bNeoTimeout(t, []zz3bNeoPlan{
		{60000, []string{"50ms", "100ms", "200ms"}},
		{600000, []string{"f0.30", "f0.60", "f0.90"}},
	})
}

func zz3bNeoTimeout(t *testing.T, plan []zz3bNeoPlan) {
	d, httpURI := zz3bNeo(t)
	t.Logf("ZZ3B neo components: %s", zz3bNeoRows(zz3bNeoExec(t, d, "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition", nil)))
	// The reference is Neo4j Community: dbms.setConfigValue does not exist, so
	// db.transaction.timeout cannot be changed at run time. Neo4j's
	// per-transaction timeout is used instead: the HTTP header
	// max-execution-time and, over Bolt, the transaction timeout.
	t.Logf("ZZ3B neo db.transaction.timeout: %s", zz3bNeoRows(zz3bNeoExec(t, d, "SHOW SETTINGS YIELD name, value WHERE name = 'db.transaction.timeout' RETURN value", nil)))
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	clear := func() {
		for {
			recs, err := zz3bNeoExec(t, d, "MATCH (n:ZZDel) WITH n LIMIT 100000 DETACH DELETE n RETURN count(*) AS c", nil)
			require.NoError(t, err)
			if recs[0].Values[0].(int64) == 0 {
				return
			}
		}
	}
	seed := func(nodes int) {
		clear()
		for lo := 1; lo <= nodes; lo += 50000 {
			_, err := zz3bNeoExec(t, d, "UNWIND range($lo, $hi) AS i CREATE (:ZZDel {i: i})", map[string]any{"lo": lo, "hi": min(lo+49999, nodes)})
			require.NoError(t, err)
		}
	}
	count := func() string { return zz3bNeoRows(zz3bNeoExec(t, d, "MATCH (n:ZZDel) RETURN count(n) AS c", nil)) }
	boltDelete := func(limit time.Duration) (string, time.Duration) {
		ctx := context.Background()
		session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
		defer session.Close(ctx)
		start := time.Now()
		res, err := session.Run(ctx, q, nil, neo4j.WithTxTimeout(limit))
		if err == nil {
			var recs []*neo4j.Record
			recs, err = res.Collect(ctx)
			if err == nil {
				return zz3bNeoRows(recs, nil), time.Since(start)
			}
		}
		return "ERROR " + err.Error(), time.Since(start)
	}
	for _, p := range plan {
		var longest time.Duration
		for i := 0; i < 3; i++ {
			seed(p.nodes)
			status, body, took := zz3bNeoPost(httpURI, q, 0)
			longest = max(longest, took)
			t.Logf("ZZ3B K72neo nodes=%d unconstrained: status=%d after %v body=%.200s count=%s", p.nodes, status, took.Round(time.Millisecond), body, count())
		}
		for _, limit := range p.limits {
			for rep := 1; rep <= 3; rep++ {
				seed(p.nodes)
				var lim time.Duration
				if strings.HasPrefix(limit, "f") {
					var f float64
					fmt.Sscanf(limit[1:], "%g", &f)
					lim = time.Duration(float64(longest) * f)
				} else {
					lim, _ = time.ParseDuration(limit)
				}
				status, body, took := zz3bNeoPost(httpURI, q, lim)
				right := count()
				time.Sleep(300 * time.Millisecond)
				settled := count()
				late := ""
				if took > lim {
					late = fmt.Sprintf(" LATE by %v (%.2fx limit)", (took - lim).Round(time.Millisecond), float64(took)/float64(lim))
				}
				t.Logf("ZZ3B K72neo nodes=%d timeout=%s(%v) rep=%d: status=%d answered after %v%s body=%.300s; count right after=%s settled=%s",
					p.nodes, limit, lim, rep, status, took.Round(time.Millisecond), late, body, right, settled)
				seed(p.nodes)
				bres, btook := boltDelete(lim)
				bright := count()
				time.Sleep(300 * time.Millisecond)
				blate := ""
				if btook > lim {
					blate = fmt.Sprintf(" LATE by %v (%.2fx limit)", (btook - lim).Round(time.Millisecond), float64(btook)/float64(lim))
				}
				t.Logf("ZZ3B K72neo-bolt nodes=%d txTimeout=%s(%v) rep=%d: answered after %v%s result=%.300s; count right after=%s settled=%s",
					p.nodes, limit, lim, rep, btook.Round(time.Millisecond), blate, bres, bright, count())
			}
		}
	}
	clear()
}

func TestZZ3BNeo4jNOR27(t *testing.T) {
	d, _ := zz3bNeo(t)
	ctx := context.Background()
	for _, q := range []string{
		"WITH [{element:'r',ends:['a','b']}] AS page RETURN [i IN range(0,size(page)-1) | {id:page[i].element, from:page[i].ends[0]}] AS seeds",
		"UNWIND ['a','a','b'] AS x RETURN collect(DISTINCT x)[0] AS owner",
		"UNWIND ['agent','agent','b'] AS x RETURN collect(DISTINCT x)[0..2] AS owners",
	} {
		t.Logf("ZZ3B J10neo %q -> %s", q, zz3bNeoRows(zz3bNeoExec(t, d, q, nil)))
	}
	const del = "MATCH (a:Alias {thing_id: $id}) DETACH DELETE a WITH a FOREACH (x IN [] | SET a.survivor_id = 'after') RETURN 1 AS seen"
	params := map[string]any{"id": "probe"}
	reset := func() {
		_, err := zz3bNeoExec(t, d, "MATCH (a:Alias) DETACH DELETE a", nil)
		require.NoError(t, err)
		_, err = zz3bNeoExec(t, d, "CREATE (:Alias {thing_id:'probe',survivor_id:'before'})", nil)
		require.NoError(t, err)
	}
	remaining := func() string { return zz3bNeoRows(zz3bNeoExec(t, d, "MATCH (a:Alias) RETURN count(a) AS c", nil)) }
	reset()
	t.Logf("ZZ3B J10neo auto-commit %q -> %s; remaining :Alias %s", del, zz3bNeoRows(zz3bNeoExec(t, d, del, params)), remaining())
	reset()
	session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	tx, err := session.BeginTransaction(ctx)
	require.NoError(t, err)
	var stmt string
	if res, err := tx.Run(ctx, del, params); err != nil {
		stmt = "ERROR " + err.Error()
	} else if recs, err := res.Collect(ctx); err != nil {
		stmt = "ERROR " + err.Error()
	} else {
		stmt = zz3bNeoRows(recs, nil)
	}
	commit := "committed"
	if err := tx.Commit(ctx); err != nil {
		commit = "COMMIT ERROR " + err.Error()
	}
	_ = session.Close(ctx)
	t.Logf("ZZ3B J10neo explicit BEGIN/%q/COMMIT -> %s; %s; remaining :Alias %s", del, stmt, commit, remaining())
	_, _ = zz3bNeoExec(t, d, "MATCH (a:Alias) DETACH DELETE a", nil)
}

// Positive control: the same per-transaction timeouts on a long read-only
// statement, to show that Neo4j applies them (the request should end near
// the timeout with a timeout error).
func TestZZ3BNeo4jTimeoutControl(t *testing.T) {
	d, httpURI := zz3bNeo(t)
	const q = "UNWIND range(1, 300000000) AS x WITH x WHERE x < 0 RETURN count(*) AS c"
	ctx := context.Background()
	for _, lim := range []time.Duration{100 * time.Millisecond, time.Second} {
		for rep := 1; rep <= 2; rep++ {
			status, body, took := zz3bNeoPost(httpURI, q, lim)
			t.Logf("ZZ3B K72neo-control HTTP max-execution-time=%v rep=%d: status=%d after %v body=%.300s", lim, rep, status, took.Round(time.Millisecond), body)
			session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
			start := time.Now()
			res, err := session.Run(ctx, q, nil, neo4j.WithTxTimeout(lim))
			out := ""
			if err == nil {
				var recs []*neo4j.Record
				recs, err = res.Collect(ctx)
				out = zz3bNeoRows(recs, err)
			} else {
				out = "ERROR " + err.Error()
			}
			_ = session.Close(ctx)
			t.Logf("ZZ3B K72neo-control Bolt txTimeout=%v rep=%d: after %v result=%.300s", lim, rep, time.Since(start).Round(time.Millisecond), out)
		}
	}
	status, body, took := zz3bNeoPost(httpURI, q, 0)
	t.Logf("ZZ3B K72neo-control no timeout: status=%d after %v body=%.200s", status, took.Round(time.Millisecond), body)
}
