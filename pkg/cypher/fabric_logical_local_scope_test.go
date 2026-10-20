package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFabricLogicalLocalScopes(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	driver := fabricCallRowsReference(t)
	for _, sample := range []struct{ name, query, reference, want string }{
		{"predicate_comment", "WITH 99 AS x CALL { WITH x RETURN any /* local */ (x IN [1,2] WHERE CASE WHEN x=2 THEN true ELSE false END) AS v } RETURN v", "", "[[true]]"},
		{"reduce_comment", "WITH 99 AS x CALL { WITH x RETURN reduce /* local */ (s=0, x IN [1,2] | s + CASE WHEN x>0 THEN x ELSE 0 END) AS v } RETURN v", "", "[[3]]"},
		{"literal_names", "WITH 99 AS x CALL { WITH x RETURN [x IN [1,2] | CASE WHEN 'x IN [99] /* local */' <> '' THEN x ELSE 0 END] AS v } RETURN v", "", "[[[1,2]]]"},
		{"nested_shadow", "WITH 99 AS x CALL { WITH x RETURN [x IN [1,2] | [x IN [x+1] | CASE WHEN x=2 THEN x ELSE -x END]] AS v } RETURN v", "", "[[[[2],[-3]]]]"},
		{"case_subquery", "WITH 99 AS x CALL { WITH x RETURN [x IN [1,2] | COUNT { WITH x AS y WHERE CASE WHEN y=1 THEN true ELSE false END RETURN y }] AS v } RETURN v", "", "[[[1,0]]]"},
		// FILTER is an existing NornicDB extension on the 5.26 reference;
		// its list-comprehension equivalent supplies the value oracle.
		{"filter", "RETURN filter /* local */ (x IN [1,2] WHERE CASE WHEN x=2 THEN true ELSE false END) AS v", "RETURN [x IN [1,2] WHERE x=2] AS v", "[[[2]]]"},
		{"filter_clause", "UNWIND [1,2] AS x FILTER x>1 RETURN x AS v", "UNWIND [1,2] AS x WITH x WHERE x>1 RETURN x AS v", "[[2]]"},
		{"map_extension", "RETURN {k:v IN {a:1,b:2} WHERE v>1 | k:v+1} AS v", "RETURN {b:3} AS v", "[[{\"b\":3}]]"},
		{"map_scoped_extension", "WITH {a:1} AS m CALL (m) { RETURN {k:v IN m | k:v+1} AS v } RETURN v", "RETURN {a:2} AS v", "[[{\"a\":2}]]"},
	} {
		if driver != nil {
			query := sample.query
			if sample.reference != "" {
				query = sample.reference
			}
			session := driver.NewSession(context.Background(), neo4j.SessionConfig{})
			ref, err := session.BeginTransaction(context.Background())
			require.NoError(t, err)
			fabricCallRowsAssertReference(t, ref, query, sample.want)
			require.NoError(t, ref.Rollback(context.Background()))
			require.NoError(t, session.Close(context.Background()))
		}
		for _, normalize := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for _, route := range []string{"native", "constituent", "nested"} {
					t.Run(fmt.Sprintf("%s/normalize_%v/explicit_%v/%s", sample.name, normalize, explicit, route), func(t *testing.T) {
						config.SetCypherQueryNormalizationEnabled(normalize)
						exec := newFabricCallRowsExecutor(t)
						query := sample.query
						if route != "native" {
							exec = fabricCallRowsCompositeExecutor(t, exec)
							query = "USE comp.one " + query
							if route == "nested" {
								query = "CALL { " + query + " } RETURN v"
							}
						}
						ctx := context.Background()
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer exec.Execute(ctx, "ROLLBACK", nil)
							if route != "native" {
								_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
								require.True(t, ok)
							}
						}
						for repeat := 0; repeat < 2; repeat++ {
							result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
							require.NoError(t, err, query)
							fabricDispatchRows(t, result.Rows, sample.want)
						}
					})
				}
			}
		}
	}
}

// An actual HTTP participant executes the received public text. It rejects
// internal functions through Execute, rather than returning canned rows.
func TestFabricLogicalRemoteComprehensions(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	driver := fabricCallRowsReference(t)
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, sample := range []struct{ name, query, reference, want string }{
				{"profile_conditional", "PROFILE WITH 'outer' AS value CALL (value) { WHEN true THEN RETURN value AS tag ELSE RETURN value AS tag } RETURN tag AS v", "RETURN 'outer' AS v", "[[\"outer\"]]"},
				{"profile_next", "PROFILE RETURN 'outer' AS value NEXT WHEN true THEN RETURN value AS v ELSE RETURN value AS v", "RETURN 'outer' AS v", "[[\"outer\"]]"},
				{"bound_pattern", "MATCH (p:FabricRemoteRead) MATCH (p {a:1}) RETURN p.b AS v", "", "[[42]]"},
				{"map", "RETURN {k:v IN {a:1,b:2} WHERE v>1 | k:v+1} AS v", "RETURN {b:3} AS v", "[[{\"b\":3}]]"},
				{"map_inline", "OPTIONAL MATCH(n:FabricRemoteScopeProbe WHERE true) RETURN {k:v IN {a:1,b:2} WHERE v>1 | k:v+1} AS v", "RETURN {b:3} AS v", "[[{\"b\":3}]]"},
				{"list_inline", "OPTIONAL MATCH(n:FabricRemoteScopeProbe WHERE true) RETURN [x IN [1,2] WHERE x>1 | x+1] AS v", "RETURN [3] AS v", "[[[3]]]"},
				{"filter_inline", "OPTIONAL MATCH(n:FabricRemoteScopeProbe WHERE true) UNWIND [1,2] AS x FILTER x>1 RETURN x AS v", "RETURN 2 AS v", "[[2]]"},
				{"list", "RETURN [x IN [1,2] WHERE x>1 | CASE WHEN x=2 THEN x+1 ELSE 0 END] AS v", "", "[[[3]]]"},
				{"filter", "UNWIND [1,2] AS x FILTER x>1 RETURN x AS v", "UNWIND [1,2] AS x WITH x WHERE x>1 RETURN x AS v", "[[2]]"},
				{"pattern", "RETURN [(a:FabricRemoteScopeProbe)-->(b) | b] AS v", "", "[[[]]]"},
				{"nested_map", "WITH {a:1} AS m CALL (m) { RETURN {k:v IN m | k:v+1} AS v } RETURN v", "RETURN {a:2} AS v", "[[{\"a\":2}]]"},
			} {
				t.Run(fmt.Sprintf("%s/normalize_%v/explicit_%v", sample.name, normalize, explicit), func(t *testing.T) {
					config.SetCypherQueryNormalizationEnabled(normalize)
					// The 5.26 reference supplies equivalent-expression values for
					// map comprehensions; this is not raw Cypher 25 syntax parity.
					if driver != nil {
						query := sample.query
						if sample.reference != "" {
							query = sample.reference
						}
						session := driver.NewSession(context.Background(), neo4j.SessionConfig{})
						ref, err := session.BeginTransaction(context.Background())
						require.NoError(t, err)
						fabricCallRowsAssertReference(t, ref, "CREATE (:FabricRemoteRead {a:1,b:42}), (:FabricRemoteRead {a:2,b:99}) RETURN 1", "[[1]]")
						fabricCallRowsAssertReference(t, ref, query, sample.want)
						require.NoError(t, ref.Rollback(context.Background()))
						require.NoError(t, session.Close(context.Background()))
					}
					participant := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "native"))
					_, seedErr := participant.Execute(context.Background(), "CREATE (:FabricRemoteRead {a:1,b:42}), (:FabricRemoteRead {a:2,b:99})", nil)
					require.NoError(t, seedErr)
					var mutex sync.Mutex
					var received, userReceived []string
					var server *httptest.Server
					server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						mutex.Lock()
						defer mutex.Unlock()
						var request struct {
							Statements []struct {
								Statement  string
								Parameters map[string]interface{}
							}
						}
						if r.Method == http.MethodPost {
							_ = json.NewDecoder(r.Body).Decode(&request)
						}
						results := make([]interface{}, 0, len(request.Statements))
						for _, statement := range request.Statements {
							result, err := participant.Execute(r.Context(), statement.Statement, statement.Parameters)
							if err != nil {
								t.Errorf("remote public statement %s: %v", statement.Statement, err)
								w.WriteHeader(500)
								return
							}
							// Capture every executed participant statement. A continuation
							// may project its final column locally after a remote import.
							received = append(received, statement.Statement)
							if statement.Parameters["admission_passthrough_probe"] == true {
								userReceived = append(userReceived, statement.Statement)
							}
							data := make([]interface{}, 0, len(result.Rows))
							for _, row := range result.Rows {
								data = append(data, map[string]interface{}{"row": row})
							}
							results = append(results, map[string]interface{}{"columns": result.Columns, "data": data})
						}
						w.Header().Set("Content-Type", "application/json")
						_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": results, "errors": []interface{}{}, "commit": server.URL + "/db/native/tx/1/commit"})
					}))
					t.Cleanup(server.Close)
					base := newFabricCallRowsExecutor(t)
					manager := base.dbManager.(*testDatabaseManagerAdapter).manager
					require.NoError(t, manager.AddConstituent("comp", multidb.ConstituentRef{Alias: "remote", DatabaseName: "native", Type: "remote", AccessMode: "read_write", URI: server.URL}))
					exec := fabricCallRowsCompositeExecutor(t, base)
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					mode, body := parseExecutionMode(sample.query)
					prefix := ""
					if mode != ModeNormal {
						prefix = string(mode) + " "
					}
					result, err := exec.Execute(ctx, "CYPHER 25 "+prefix+"USE comp.remote "+body, map[string]interface{}{"admission_passthrough_probe": true})
					require.NoError(t, err)
					fabricDispatchRows(t, result.Rows, sample.want)
					mutex.Lock()
					defer mutex.Unlock()
					require.NotEmpty(t, received)
					for _, query := range received {
						require.NoError(t, internalFunctionCallError(query), query)
					}
					// Match request provenance, never query text or result columns:
					// startup catalog reads are not public Cypher25 invocations.
					require.NotEmpty(t, userReceived)
					for _, query := range userReceived {
						require.True(t, strings.HasPrefix(query, "CYPHER 25 "), query)
					}
				})
			}
		}
	}
}
