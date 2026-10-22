package cypher

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// The generated differential runs every shape. This independent semantic
// sample prevents agreement between two wrong product access paths from passing.
func TestNodePatternIndexDifferentialNeo4j(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires the leased Neo4j reference")
	}
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(ctx)
	setup := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer setup.Close(ctx)
	exec := func(query string) {
		result, err := setup.Run(ctx, query, nil)
		require.NoError(t, err)
		_, err = result.Consume(ctx)
		require.NoError(t, err)
	}
	for _, property := range []string{"id", "bucket"} {
		exec("DROP INDEX r73_diff_" + property + " IF EXISTS")
	}
	exec("MATCH (n) DETACH DELETE n")
	exec(nodePatternDifferentialFixture)
	var sample []nodePatternDifferentialCase
	all := nodePatternDifferentialCases()
	for index, shape := range all {
		if nodePatternOracleSample(shape, index, len(all)) {
			sample = append(sample, shape)
		}
	}
	params := map[string]interface{}{"skip": int64(2), "limit": int64(3), "value": int64(3)}
	for _, indexed := range []bool{false, true} {
		store := nodePatternDifferentialStore(t, indexed)
		if indexed {
			for _, property := range []string{"id", "bucket"} {
				exec("CREATE INDEX r73_diff_" + property + " FOR (n:R73Label) ON (n." + property + ")")
			}
			exec("CALL db.awaitIndexes()")
		}
		for _, explicit := range []bool{false, true} {
			for _, profile := range []bool{false, true} {
				for _, shape := range sample {
					t.Run(fmt.Sprintf("index=%t/tx=%t/profile=%t/%s", indexed, explicit, profile, shape.name), func(t *testing.T) {
						session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
						defer session.Close(ctx)
						run := func(ctx context.Context, query string, params map[string]interface{}) (neo4j.ResultWithContext, error) {
							return session.Run(ctx, query, params)
						}
						if explicit {
							tx, err := session.BeginTransaction(ctx)
							require.NoError(t, err)
							defer tx.Rollback(ctx)
							run = tx.Run
						}
						query := shape.query
						if profile {
							query = "PROFILE " + query
						}
						result, err := run(ctx, query, params)
						require.NoError(t, err, query)
						records, err := result.Collect(ctx)
						require.NoError(t, err)
						rows := [][]interface{}{}
						for _, record := range records {
							rows = append(rows, record.Values)
						}
						got := executeNodePatternDifferential(t, store, shape.query, explicit, profile)
						keys, err := result.Keys()
						require.NoError(t, err)
						require.Equal(t, keys, got.Columns, query)
						require.Equal(t, rows, got.Rows, query)
					})
				}
			}
		}
	}
}
