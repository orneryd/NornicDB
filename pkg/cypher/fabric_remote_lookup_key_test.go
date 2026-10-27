package cypher

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// A correlated lookup on a remote participant matches outer keys by Cypher
// equality, whatever number type each side carries: 42 = 42.0, 0 = -0.0,
// 9007199254740993 <> 9007199254740992.0, and NaN matches nothing. Each
// batched lookup shape (count, collect, rows) runs through Bolt and HTTP, in
// autocommit and in an explicit transaction, and returns what Neo4j returns
// for the same statement, compared by kind and exact value.
func TestFabricRemoteLookupKeyEquality(t *testing.T) {
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("requires --ref neo-ref")
	}
	httpURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	require.NotEmpty(t, httpURI, "requires the reference HTTP endpoint")
	ctx := context.Background()
	label := fmt.Sprintf("FabricLookupKey%d", time.Now().UnixNano())
	_, err := neo4j.ExecuteQuery(ctx, driver, "UNWIND [{k: 42, v: 'hit'}, {k: 0, v: 'zero'}, {k: 9007199254740993, v: 'big'}, {k: 2.5, v: 'half'}] AS p CREATE (n:"+label+") SET n = p", nil, neo4j.EagerResultTransformer)
	require.NoError(t, err)
	defer func() {
		_, _ = neo4j.ExecuteQuery(context.Background(), driver, "MATCH (n:"+label+") DETACH DELETE n", nil, neo4j.EagerResultTransformer)
	}()
	exec := fabricDispatchRemoteExecutor(t)
	mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
	require.NoError(t, mgr.AddConstituent(t.Context(), "comp", multidb.ConstituentRef{Alias: "http", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: httpURI}))
	shapes := []struct{ name, tail string }{
		{"count", "RETURN count(n) AS c } RETURN key, c"},
		{"collect", "RETURN collect(n.v) AS vs } RETURN key, vs"},
		{"rows", "RETURN n.v AS v } RETURN key, v"},
	}
	keySets := []struct {
		name, keys string
		boltOnly   bool
	}{
		{"float_keys", "[42.0, 43.0]", false},
		{"integer_keys", "[42, 43]", false},
		{"fraction", "[2.5, 2.0, 3]", false},
		{"signed_zero", "[-0.0, 0.0, 0]", false},
		{"beyond_2p53", "[9007199254740992.0, 9007199254740993, 9007199254740994.0]", false},
		// JSON has no NaN, so a NaN parameter can't reach an HTTP participant.
		{"nan", "[0.0 / 0.0, 42]", true},
	}
	for _, keys := range keySets {
		for _, shape := range shapes {
			body := "UNWIND " + keys.keys + " AS key CALL { WITH key %sMATCH (n:" + label + ") WHERE n.k = key " + shape.tail
			ref, err := neo4j.ExecuteQuery(ctx, driver, fmt.Sprintf(body, ""), nil, neo4j.EagerResultTransformer)
			require.NoError(t, err)
			wantRows := make([][]interface{}, 0, len(ref.Records))
			for _, record := range ref.Records {
				wantRows = append(wantRows, record.Values)
			}
			want := fabricSortedTypedRows(wantRows)
			for _, destination := range []string{"remote", "http"} {
				if keys.boltOnly && destination == "http" {
					continue
				}
				query := "USE comp " + fmt.Sprintf(body, "USE comp."+destination+" ")
				for _, explicit := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/explicit=%v", keys.name, shape.name, destination, explicit), func(t *testing.T) {
						ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						defer cancel()
						runner := exec
						if explicit {
							runner = fabricCallRowsCompositeExecutor(t, exec)
							_, err := runner.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer func() { _, _ = runner.Execute(context.Background(), "ROLLBACK", nil) }()
						}
						result, err := runner.Execute(ctx, query, nil)
						require.NoError(t, err, "Fabric query: %s", query)
						require.Equal(t, want, fabricSortedTypedRows(result.Rows), "Fabric query: %s", query)
					})
				}
			}
		}
	}
}

// fabricSortedTypedRows is fabricTypedRows in a fixed order, for statements
// whose row order is not specified.
func fabricSortedTypedRows(rows [][]interface{}) []interface{} {
	typed := fabricTypedRows(rows)
	sort.Slice(typed, func(i, j int) bool { return fmt.Sprint(typed[i]) < fmt.Sprint(typed[j]) })
	return typed
}
