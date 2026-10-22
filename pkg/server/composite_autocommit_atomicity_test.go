package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// compositeAtomicityBody is the writing body the dynamic procedures run.
const compositeAtomicityBody = "CREATE (:AcAtom {run: $run}) RETURN 1 AS w"

// A composite statement sent over HTTP, on the autocommit endpoint or in an
// explicit transaction, commits its local writes, from a write clause or a
// procedure, only when the whole statement succeeds, as Neo4j 5.26.30
// commits a statement (compared on neo-ref). The failure is a runtime error
// on the local constituent or on a Neo4j constituent reached over Bolt or
// HTTP, after the local write. The explicit transaction is checked with the
// local failure. A procedure's write, also one found only when its dynamic
// body runs, makes accmp.one the statement's one write database.
func TestHTTPCompositeAutocommitStatementWritesAreAtomic(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	httpURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if uri == "" || httpURI == "" {
		t.Skip("requires --ref neo-ref")
	}
	ctx := context.Background()
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	require.NoError(t, server.dbManager.CreateDatabase("acone"))
	require.NoError(t, server.dbManager.CreateDatabase("actwo"))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("accmp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "acone", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "actwo", Type: "local", AccessMode: "read_write"},
		{Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri},
		{Alias: "http", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: httpURI},
	}))
	reference, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reference.Close(context.Background()) })

	post := func(path, statement string, params map[string]any) TransactionResponse {
		rec := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}}, token)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		return resp
	}
	require.Empty(t, post("/db/acone/tx/commit", "CREATE OR REPLACE PROCEDURE acatom.write($run) MODE WRITE AS CREATE (:AcAtom {run: $run}) RETURN 1 AS w", nil).Errors)
	t.Cleanup(func() { post("/db/acone/tx/commit", "DROP PROCEDURE acatom.write", nil) })
	writes := func(constituent, run string) float64 {
		resp := post("/db/accmp/tx/commit", "USE "+constituent+" MATCH (n:AcAtom {run: $run}) RETURN count(n) AS c", map[string]any{"run": run})
		require.Empty(t, resp.Errors)
		return resp.Results[0].Data[0].Row[0].(float64)
	}
	referenceWrites := func(statement string, params map[string]any) (bool, int64) {
		session := reference.NewSession(ctx, neo4j.SessionConfig{})
		defer func() { _ = session.Close(ctx) }()
		result, err := session.Run(ctx, statement, params)
		if err == nil {
			_, err = result.Consume(ctx)
		}
		count, countErr := neo4j.ExecuteQuery(ctx, reference, "MATCH (n:AcAtom {run: $run}) WITH count(n) AS c, collect(n) AS ns FOREACH (n IN ns | DELETE n) RETURN c", map[string]any{"run": params["run"]}, neo4j.EagerResultTransformer)
		require.NoError(t, countErr)
		return err != nil, count.Records[0].Values[0].(int64)
	}

	const created = "CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { RETURN 1 / $d AS z } RETURN w, z"
	for _, shape := range []struct {
		name, query, reference string
		writes                 int64
	}{
		{"call_then_error", "CALL { USE accmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z", "", 1},
		{"union_branch_error", "CALL { USE accmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS z UNION ALL USE {fail} RETURN 1 / $d AS z } RETURN z", "", 1},
		{"nested_call_error", "CALL { USE accmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { CALL { USE {fail} RETURN 1 / $d AS z } RETURN z } RETURN w, z", "", 1},
		{"second_write_error", "CALL { USE accmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE accmp.one CREATE (:AcAtom {run: $run, v: w / $d}) RETURN 1 AS z } RETURN z", "", 2},
		{"procedure_write_then_error", "CALL { USE accmp.one CALL acatom.write($run) YIELD w RETURN w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z", created, 1},
		{"dynamic_write_then_error", "CALL { USE accmp.one CALL apoc.cypher.doIt('CREATE (:AcAtom {run: $run}) RETURN 1 AS w', {run: $run}) YIELD value RETURN value.w AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z", created, 1},
		{"read_call_write_then_error", "CALL { USE accmp.one CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z", created, 1},
		{"doitall_write_then_error", "CALL { USE accmp.one CALL apoc.cypher.doitall($body, {run: $run}) YIELD value RETURN value.w AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z", created, 1},
	} {
		for _, fail := range []string{"accmp.one", "accmp.remote", "accmp.http"} {
			for _, explicit := range []bool{false, true} {
				if explicit && fail != "accmp.one" {
					continue
				}
				for _, d := range []int64{0, 1} {
					t.Run(fmt.Sprintf("%s/%s/explicit=%v/d=%d", shape.name, fail, explicit, d), func(t *testing.T) {
						run := fmt.Sprintf("http-%s-%s-%v-%d-%d", shape.name, fail, explicit, d, time.Now().UnixNano())
						params := map[string]any{"run": run, "d": d, "body": compositeAtomicityBody}
						statement := "USE accmp " + strings.ReplaceAll(shape.query, "{fail}", fail)
						var resp TransactionResponse
						if explicit {
							resp = post("/db/accmp/tx", statement, params)
							if resp.Commit != "" {
								commit, err := url.Parse(resp.Commit)
								require.NoError(t, err)
								if len(resp.Errors) == 0 {
									ended := post(commit.Path, "RETURN 1", nil)
									require.Empty(t, ended.Errors)
								} else {
									makeRequest(t, server, http.MethodDelete, strings.TrimSuffix(commit.Path, "/commit"), nil, token)
								}
							}
						} else {
							resp = post("/db/accmp/tx/commit", statement, params)
						}
						want := float64(shape.writes)
						if d == 0 {
							require.NotEmpty(t, resp.Errors)
							want = 0
						} else {
							require.Empty(t, resp.Errors)
						}
						require.Equal(t, want, writes("accmp.one", run), "AcAtom nodes left on accmp.one")

						referenceStatement := shape.reference
						if referenceStatement == "" {
							referenceStatement = strings.NewReplacer("USE accmp.one ", "", "USE {fail} ", "").Replace(shape.query)
						}
						failed, referenceCount := referenceWrites(referenceStatement, params)
						require.Equal(t, d == 0, failed, "Neo4j reference outcome")
						require.Equal(t, int64(want), referenceCount, "Neo4j reference writes")
					})
				}
			}
		}
	}

	// A write to accmp.one through a procedure (a WRITE procedure, or a
	// reading procedure whose dynamic body writes) and a write to accmp.two,
	// in both orders: a statement writes to one database, so each is refused
	// and leaves nothing on either.
	orders := []struct{ name, query string }{
		{"procedure_first", "USE accmp CALL { USE accmp.one CALL acatom.write($run) YIELD w RETURN w } CALL { WITH w USE accmp.two CREATE (:AcAtom {run: $run}) RETURN w AS z } RETURN z"},
		{"procedure_second", "USE accmp CALL { USE accmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE accmp.one CALL acatom.write($run) YIELD w AS z RETURN z } RETURN w, z"},
	}
	for _, procedure := range []string{"apoc.cypher.run", "apoc.cypher.doitall"} {
		call := "CALL " + procedure + "($body, {run: $run}) YIELD value"
		orders = append(orders,
			struct{ name, query string }{procedure + "_first", "USE accmp CALL { USE accmp.one " + call + " RETURN value.w AS w } CALL { WITH w USE accmp.two CREATE (:AcAtom {run: $run}) RETURN w AS z } RETURN z"},
			struct{ name, query string }{procedure + "_second", "USE accmp CALL { USE accmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE accmp.one " + call + " RETURN value.w AS z } RETURN w, z"},
		)
	}
	for _, order := range orders {
		t.Run("second_write_database/"+order.name, func(t *testing.T) {
			run := fmt.Sprintf("http-shard-%s-%d", order.name, time.Now().UnixNano())
			resp := post("/db/accmp/tx/commit", order.query, map[string]any{"run": run, "body": compositeAtomicityBody})
			require.NotEmpty(t, resp.Errors)
			require.Contains(t, resp.Errors[0].Message, "Writing to more than one database per transaction is not allowed")
			require.Equal(t, float64(0), writes("accmp.one", run))
			require.Equal(t, float64(0), writes("accmp.two", run))
		})
	}
}
