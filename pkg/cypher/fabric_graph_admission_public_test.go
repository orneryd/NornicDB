package cypher

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// A cached plan is syntax, not a cached transaction verdict. Every invocation
// must admit its selected graphs before rows, effects or projection shortcuts.
func TestPreparedGraphAdmissionTransactionPublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	body := "FOREACH (x IN [1,2] | CREATE (:GraphAdmission {id:$id,v:abs(x)})) RETURN 1 AS v"
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, composite := range []bool{false, true} {
			for _, warm := range []bool{false, true} {
				for _, form := range []string{"static", "call", "dynamic", "shortcut", "profile"} {
					t.Run(fmt.Sprintf("%s/composite_%v/warm_%v/norm_%v", form, composite, warm, normalize), func(t *testing.T) {
						exec := newFabricCallRowsExecutor(t)
						if composite {
							exec = fabricCallRowsCompositeExecutor(t, exec)
						}
						ctx := context.Background()
						query := "USE comp.one " + body
						switch form {
						case "call":
							query = "CALL { USE comp.one " + body + " } RETURN v"
						case "dynamic":
							query = "USE graph.byName($target) " + body
						case "shortcut":
							query = "USE comp.one RETURN 1 AS v"
						case "profile":
							query = "PROFILE USE comp.one " + body
						}
						if warm && (composite || form != "dynamic") {
							result, err := exec.Execute(ctx, query, map[string]interface{}{"id": "warm", "target": "comp.one"})
							require.NoError(t, err, query)
							require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
							require.NotNil(t, exec.fabricPlanCache)
							_, planQuery := parseExecutionMode(query)
							_, cached := exec.fabricPlanCache.Get(semanticCacheKey(planQuery, false), exec.currentDatabaseName())
							require.True(t, cached, "the public warm-up must retain the plan")
						}
						for attempt := 0; attempt < 2; attempt++ {
							func() {
								id := fmt.Sprintf("probe-%d", attempt)
								params := map[string]interface{}{"id": id, "target": "comp.one"}
								_, err := exec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer func() { _, err := exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
								result, err := exec.Execute(ctx, query, params)
								if !composite {
									require.Error(t, err, query)
									code, _ := nornicerrors.Neo4jStatus(err)
									expected := "Neo.ClientError.Statement.AccessMode"
									if form == "dynamic" {
										expected = "Neo.ClientError.Statement.SyntaxError"
									}
									require.Equal(t, expected, code, query)
									return
								}
								require.NoError(t, err, query)
								require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
								result, err = exec.Execute(ctx, "USE comp.one MATCH (n:GraphAdmission {id:$id}) RETURN sum(n.v) AS v", params)
								require.NoError(t, err)
								expected := int64(3)
								if form == "shortcut" {
									expected = 0
								}
								require.Equal(t, [][]interface{}{{expected}}, result.Rows)
							}()
							// Check both the selected constituent and the original database after
							// the failed/rolled-back transaction. A wrong-namespace write is a failure.
							for _, prefix := range []string{"", "USE comp.one "} {
								if composite && prefix == "" {
									continue
								}
								result, err := exec.Execute(ctx, prefix+"MATCH (n:GraphAdmission {id:$id}) RETURN count(n) AS v", map[string]interface{}{"id": fmt.Sprintf("probe-%d", attempt)})
								require.NoError(t, err)
								require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
							}
						}
					})
				}
			}
		}
	}
}

func TestPreparedGraphAdmissionRemoteRejectsBeforeHTTP(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"errors":[]}`))
	}))
	defer server.Close()
	exec := newFabricCallRowsExecutor(t)
	manager := exec.dbManager.(*testDatabaseManagerAdapter).manager
	require.NoError(t, manager.AddConstituent("comp", multidb.ConstituentRef{Alias: "remote", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: server.URL}))
	ctx := context.Background()
	for attempt := 0; attempt < 2; attempt++ {
		func() {
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer exec.Execute(ctx, "ROLLBACK", nil)
			before := requests.Load()
			_, err = exec.Execute(ctx, "USE comp.remote CREATE (:GraphAdmission) RETURN 1 AS v", nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.AccessMode", code)
			require.Equal(t, before, requests.Load(), "rejection precedes opening a remote participant")
		}()
	}
}

// Neo4j Community has no second user database. Before the first write,
// system rejects FOREACH as unsupported system syntax. After a first-graph
// write, its transaction policy rejects the second write with AccessMode,
// before system syntax validation. Only that policy case is a value/code
// oracle for a NornicDB user constituent; retain the other reference outcomes
// explicitly so the fixture cannot conflate these admission boundaries.
func TestPreparedGraphAdmissionNeo4jPolicy(t *testing.T) {
	ctx := context.Background()
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("Neo4j 5.26 reference is required")
	}
	cases := []struct {
		name, first, code, detail string
	}{
		{"before_first_statement", "", "Neo.ClientError.Statement.SemanticError", "unsupported clauses were used: FOREACH"},
		{"after_read", "RETURN 1 AS pinned", "Neo.ClientError.Statement.SemanticError", "unsupported clauses were used: FOREACH"},
		{"after_write", "CREATE (:GraphAdmissionReference {id:'first'}) RETURN 1 AS pinned", "Neo.ClientError.Statement.AccessMode", "Writing to more than one database per transaction is not allowed"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j", AccessMode: neo4j.AccessModeWrite})
			defer session.Close(ctx)
			tx, err := session.BeginTransaction(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)
			if test.first != "" {
				result, err := tx.Run(ctx, test.first, nil)
				require.NoError(t, err)
				_, err = result.Consume(ctx)
				require.NoError(t, err)
			}
			body := "FOREACH (x IN [1,2] | CREATE (:GraphAdmissionReference {v:abs(x)})) RETURN 1 AS v"
			result, err := tx.Run(ctx, "USE system "+body, nil)
			if err == nil {
				_, err = result.Consume(ctx)
			}
			require.Error(t, err)
			var reference *neo4j.Neo4jError
			require.ErrorAs(t, err, &reference)
			t.Logf("Neo4j ordinary-session second-graph write: code=%s message=%s", reference.Code, reference.Msg)
			require.Equal(t, test.code, reference.Code)
			require.Contains(t, reference.Msg, test.detail)
			require.NoError(t, tx.Rollback(ctx))
			rows, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (n:GraphAdmissionReference) RETURN count(n) AS remaining", nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("neo4j"))
			require.NoError(t, err)
			require.Equal(t, int64(0), rows.Records[0].Values[0])
			if test.name != "after_write" {
				return
			}

			// Compare the canonical policy at the same transaction state, using
			// a real user constituent rather than the restricted system graph.
			exec := newFabricCallRowsExecutor(t)
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer exec.Execute(ctx, "ROLLBACK", nil)
			_, err = exec.Execute(ctx, test.first, nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "USE comp.one "+body, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, reference.Code, code)
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
			for _, prefix := range []string{"", "USE comp.one "} {
				remaining, err := exec.Execute(ctx, prefix+"MATCH (n:GraphAdmissionReference) RETURN count(n) AS remaining", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0)}}, remaining.Rows)
			}
		})
	}
}
