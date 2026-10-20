package cypher

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// FILTER has raw Cypher25 parity on the approved 2025.12.1 reference.
// Map comprehensions are a bounded NornicDB extension: this reference rejects
// their syntax. Their values and NornicDB remote passthrough have separate tests.
func TestFabricLogicalNeo25Comprehensions(t *testing.T) {
	if os.Getenv("NORNICDB_NEO25_PARITY") != "1" {
		t.Skip("requires --ref neo25 and NORNICDB_NEO25_PARITY=1")
	}
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	require.NotEmpty(t, uri)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	version, err := neo4j.ExecuteQuery(ctx, driver, "CALL dbms.components() YIELD versions RETURN versions[0] AS v", nil, neo4j.EagerResultTransformer)
	require.NoError(t, err)
	require.Equal(t, "2025.12.1", version.Records[0].Values[0])
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, sample := range []struct {
		name, query, want string
		extension         bool
	}{
		{"map", "RETURN {k:v IN {a:1,b:2} | k:v+1} AS v", "[[{\"a\":2,\"b\":3}]]", true},
		{"map_predicate", "RETURN {k:v IN {a:1,b:2} WHERE CASE WHEN v>1 THEN true ELSE false END | k:v+1} AS v", "[[{\"b\":3}]]", true},
		{"map_scoped", "WITH {a:1} AS m CALL (m) { RETURN {k:v IN m | k:v+1} AS v } RETURN v", "[[{\"a\":2}]]", true},
		{"map_nested", "RETURN {k:v IN {a:{b:1}} | k:{kk:vv IN v | kk:vv+1}} AS v", "[[{\"a\":{\"b\":2}}]]", true},
		{"filter", "UNWIND [1,2] AS x FILTER x>1 RETURN x AS v", "[[2]]", false},
		{"filter_scoped", "WITH [1,2] AS xs CALL (xs) { UNWIND xs AS x FILTER CASE WHEN x>1 THEN true ELSE false END RETURN x AS v } RETURN v", "[[2]]", false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			session := driver.NewSession(ctx, neo4j.SessionConfig{})
			reference, err := session.BeginTransaction(ctx)
			require.NoError(t, err)
			if sample.extension {
				_, err := reference.Run(ctx, "CYPHER 25 "+sample.query, nil)
				require.Error(t, err)
				require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError")
				require.NoError(t, reference.Rollback(ctx))
				require.NoError(t, session.Close(ctx))
				t.Log("NornicDB map-comprehension extension: Neo4j rejects its public syntax; no raw parity or Neo4j remote support is claimed")
				return
			}
			fabricCallRowsAssertReference(t, reference, "CYPHER 25 "+sample.query, sample.want)
			require.NoError(t, reference.Rollback(ctx))
			require.NoError(t, session.Close(ctx))
			for _, normalize := range []bool{false, true} {
				for _, explicit := range []bool{false, true} {
					for _, route := range []string{"native", "constituent", "nested", "remote"} {
						t.Run(fmt.Sprintf("normalize_%v/explicit_%v/%s", normalize, explicit, route), func(t *testing.T) {
							config.SetCypherQueryNormalizationEnabled(normalize)
							exec := newFabricCallRowsExecutor(t)
							query := sample.query
							if route != "native" {
								manager := exec.dbManager.(*testDatabaseManagerAdapter).manager
								target := "one"
								if route == "remote" {
									target = "remote"
									require.NoError(t, manager.AddConstituent("comp", multidb.ConstituentRef{Alias: target, DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri}))
								}
								exec = fabricCallRowsCompositeExecutor(t, exec)
								query = "USE comp." + target + " " + query
								if route == "nested" {
									query = "CALL { " + query + " } RETURN v"
								}
							}
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
		})
	}
}
