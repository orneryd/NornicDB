package cypher

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newFabricCallRowsExecutor(t testing.TB) *StorageExecutor {
	t.Helper()
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	for _, db := range []string{"one", "two"} {
		require.NoError(t, mgr.CreateDatabase(db))
		store, err := mgr.GetStorage(db)
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: storage.NodeID(db), Labels: []string{"P"}, Properties: map[string]interface{}{"db": db}})
		require.NoError(t, err)
	}
	require.NoError(t, mgr.CreateCompositeDatabase("comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "two", Type: "local", AccessMode: "read_write"},
	}))
	store, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	return exec
}

// These data-independent queries exercise the public composite path. With --ref
// neo-ref they also run against Neo4j 5.26.30, omitting only the USE selectors:
// the reference is a single database and the branch bodies do not read storage.
func TestFabricCallRowStream(t *testing.T) {
	ctx := context.Background()
	exec := newFabricCallRowsExecutor(t)
	var driver neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		driver, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
		version, err := neo4j.ExecuteQuery(ctx, driver, "CALL dbms.components() YIELD versions RETURN versions[0] AS version", nil, neo4j.EagerResultTransformer)
		require.NoError(t, err)
		require.Equal(t, "5.26.30", version.Records[0].Values[0])
	}
	call := "USE comp CALL { USE comp.one UNWIND [2, 1, 2] AS k RETURN k } "
	tests := []struct {
		name, query, want string
	}{
		{"plain_collect", call + "RETURN collect(k) AS ks", "[[[2,1,2]]]"},
		{"plain_count", call + "RETURN count(*) AS c", "[[3]]"},
		{"with_collect", call + "WITH k RETURN collect(k) AS ks", "[[[2,1,2]]]"},
		{"with_order_collect", call + "WITH k ORDER BY k RETURN collect(k) AS ks", "[[[1,2,2]]]"},
		{"with_star_order_collect", call + "WITH * ORDER BY k RETURN collect(k) AS ks", "[[[1,2,2]]]"},
		{"with_distinct_collect", call + "WITH DISTINCT k ORDER BY k RETURN collect(k) AS ks", "[[[1,2]]]"},
		{"with_order_slice", call + "WITH k ORDER BY k SKIP 1 LIMIT 1 RETURN collect(k) AS ks", "[[[2]]]"},
		{"return_order_slice", call + "RETURN k ORDER BY k SKIP 1 LIMIT 1", "[[2]]"},
		{"return_distinct", call + "RETURN DISTINCT k ORDER BY k", "[[1],[2]]"},
		{"with_filter", call + "WITH k WHERE k = 2 RETURN count(*) AS c", "[[2]]"},
		{"with_group", call + "WITH k, count(*) AS c ORDER BY k RETURN collect([k,c]) AS ks", "[[[[1,1],[2,2]]]]"},
		{"multiple_horizons", call + "WITH k ORDER BY k WITH k LIMIT 2 RETURN collect(k) AS ks", "[[[1,2]]]"},
		{"unwind_continuation", call + "UNWIND [k,k] AS j RETURN count(*) AS c", "[[6]]"},
		{"with_before_second_call", call + "WITH k ORDER BY k LIMIT 2 CALL { USE comp.two RETURN 9 AS j } RETURN collect(k) AS ks", "[[[1,2]]]"},
		{"distinct_before_second_call", call + "WITH DISTINCT k CALL { USE comp.two RETURN 9 AS j } RETURN count(*) AS c", "[[2]]"},
		{"unwind_before_second_call", call + "UNWIND [k,k] AS j CALL { USE comp.two RETURN 9 AS z } RETURN count(*) AS c", "[[6]]"},
		{"correlated_call_control", call + "CALL { USE comp.two WITH k RETURN k + 10 AS j } RETURN j ORDER BY j", "[[11],[12],[12]]"},
		{"local_call_continuation", call + "CALL { WITH k RETURN k + 10 AS j } RETURN collect(j) AS js", "[[[12,11,12]]]"},
		{"nested_horizon", "USE comp CALL { CALL { USE comp.one UNWIND [2,1] AS k RETURN k } WITH k ORDER BY k RETURN collect(k) AS ks } RETURN ks", "[[[1,2]]]"},
		{"empty_aggregate", "USE comp CALL { USE comp.one UNWIND [] AS k RETURN k } WITH k ORDER BY k RETURN count(*) AS c", "[[0]]"},
		{"empty_nonaggregate", "USE comp CALL { USE comp.one UNWIND [] AS k RETURN k } WITH k RETURN k", "[]"},
		{"empty_group", "USE comp CALL { USE comp.one UNWIND [] AS k RETURN k } WITH k RETURN k, count(*) AS c", "[]"},
		{"union_all_body", "USE comp CALL { USE comp.one RETURN 1 AS k UNION ALL USE comp.two RETURN 2 AS k } RETURN k ORDER BY k", "[[1],[2]]"},
		{"union_distinct_body", "USE comp CALL { USE comp.one RETURN 1 AS k UNION USE comp.two RETURN 1 AS k } RETURN count(*) AS c", "[[1]]"},
		{"union_all_duplicates", "USE comp CALL { USE comp.one RETURN 1 AS k UNION ALL USE comp.two RETURN 1 AS k } RETURN count(*) AS c", "[[2]]"},
		{"union_three_branches", "USE comp CALL { USE comp.one RETURN 3 AS k UNION ALL USE comp.two RETURN 1 AS k UNION ALL USE comp.one RETURN 2 AS k } WITH k ORDER BY k RETURN collect(k) AS ks", "[[[1,2,3]]]"},
		{"union_branch_limits", "USE comp CALL { USE comp.one UNWIND [3,1] AS k RETURN k ORDER BY k LIMIT 1 UNION ALL USE comp.two UNWIND [4,2] AS k RETURN k ORDER BY k LIMIT 1 } WITH k ORDER BY k RETURN collect(k) AS ks", "[[[1,2]]]"},
		{"union_nested", "USE comp CALL { CALL { USE comp.one RETURN 1 AS k UNION ALL USE comp.two RETURN 2 AS k } RETURN k } RETURN count(*) AS c", "[[2]]"},
		{"union_nested_branch", "USE comp CALL { CALL { USE comp.one RETURN 1 AS k } RETURN k UNION ALL USE comp.two RETURN 2 AS k } RETURN k ORDER BY k", "[[1],[2]]"},
		{"union_empty_branch", "USE comp CALL { USE comp.one UNWIND [] AS k RETURN k UNION ALL USE comp.two RETURN 2 AS k } RETURN collect(k) AS ks", "[[[2]]]"},
		{"union_correlated", "USE comp UNWIND [10,20] AS x CALL { USE comp.one WITH x RETURN x + 1 AS k UNION ALL USE comp.two WITH x RETURN x + 2 AS k } RETURN x,k ORDER BY x,k", "[[10,11],[10,12],[20,21],[20,22]]"},
		{"union_inherited_use", "USE comp CALL { USE comp.one RETURN 1 AS k UNION ALL RETURN 2 AS k } RETURN k ORDER BY k", "[[1],[2]]"},
		{"union_second_use", "USE comp CALL { RETURN 1 AS k UNION ALL USE comp.two RETURN 2 AS k } RETURN k ORDER BY k", "[[1],[2]]"},
		{"union_top_level_control", "USE comp.one RETURN 1 AS k UNION ALL USE comp.two RETURN 2 AS k", "[[1],[2]]"},
		{"dynamic_inner", "USE comp CALL { USE graph.byName('comp.one') UNWIND [2,1] AS k RETURN k } WITH k ORDER BY k RETURN collect(k) AS ks", "[[[1,2]]]"},
		{"dynamic_union", "USE comp CALL { USE graph.byName('comp.one') RETURN 1 AS k UNION ALL USE graph.byName('comp.two') RETURN 2 AS k } RETURN k ORDER BY k", "[[1],[2]]"},
		{"quoted_union_text", "USE comp CALL { USE comp.one RETURN 'UNION ALL' AS k UNION ALL USE comp.two RETURN 'two' AS k } RETURN k ORDER BY k", "[[\"UNION ALL\"],[\"two\"]]"},
		{"quoted_union_column", "USE comp CALL { USE comp.one RETURN 1 AS `UNION key` UNION ALL USE comp.two RETURN 2 AS `UNION key` } RETURN `UNION key` ORDER BY `UNION key`", "[[1],[2]]"},
		{"comment_union", "USE comp CALL { USE comp.one RETURN 1 AS k /* UNION ignored */ UNION ALL USE comp.two RETURN 2 AS k } RETURN k ORDER BY k", "[[1],[2]]"},
		{"dynamic_outer", "USE graph.byName('comp.one') CALL { USE comp.two UNWIND [2,1] AS k RETURN k } WITH k ORDER BY k RETURN collect(k) AS ks", "[[[1,2]]]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if driver != nil {
				query := strings.NewReplacer("USE graph.byName('comp.one') ", "", "USE graph.byName('comp.two') ", "", "USE comp.one ", "", "USE comp.two ", "", "USE comp ", "").Replace(tt.query)
				ref, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer)
				require.NoError(t, err, "Neo4j query: %s", query)
				rows := make([][]interface{}, 0, len(ref.Records))
				for _, record := range ref.Records {
					rows = append(rows, record.Values)
				}
				encoded, err := json.Marshal(rows)
				require.NoError(t, err)
				require.JSONEq(t, tt.want, string(encoded), "Neo4j reference")
			}
			for _, mode := range []string{"autocommit", "transaction"} {
				t.Run(mode, func(t *testing.T) {
					if mode == "transaction" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() {
							_, err := exec.Execute(ctx, "ROLLBACK", nil)
							require.NoError(t, err)
						}()
					}
					result, err := exec.Execute(ctx, tt.query, nil)
					require.NoError(t, err)
					rows := append(make([][]interface{}, 0, len(result.Rows)), result.Rows...)
					encoded, err := json.Marshal(rows)
					require.NoError(t, err)
					require.JSONEq(t, tt.want, string(encoded))
				})
			}
		})
	}
}

// The benchmark also measures the formerly incorrect horizons on the base:
// those timings are the cost of the old result, not equivalent correct work.
func BenchmarkFabricCallRows(b *testing.B) {
	for _, tt := range []struct{ name, query string }{
		{"projection", "USE comp CALL { USE comp.one UNWIND range(1,100) AS k RETURN k } RETURN k"},
		{"ordered_return", "USE comp CALL { USE comp.one UNWIND range(1,100) AS k RETURN k } RETURN k ORDER BY k DESC"},
		{"ordered_aggregate", "USE comp CALL { USE comp.one UNWIND range(1,100) AS k RETURN k } WITH k ORDER BY k DESC RETURN collect(k) AS ks"},
		{"union_body", "USE comp CALL { USE comp.one RETURN 1 AS k UNION ALL USE comp.two RETURN 2 AS k } RETURN count(*) AS c"},
		{"union_top_level", "USE comp.one RETURN 1 AS k UNION ALL USE comp.two RETURN 2 AS k"},
	} {
		b.Run(tt.name, func(b *testing.B) {
			exec := newFabricCallRowsExecutor(b)
			exec.cache.SetEnabled(false)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, tt.query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestFabricCallRowsConstituentData(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	ctx := context.Background()
	for _, explicit := range []bool{false, true} {
		if explicit {
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer func() {
				_, err := exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}()
		}
		result, err := exec.Execute(ctx, "USE comp CALL { USE comp.one MATCH (n:P) RETURN n UNION ALL USE comp.two MATCH (n:P) RETURN n } WITH n ORDER BY n.db RETURN collect(n.db) AS dbs", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{"one", "two"}}}, result.Rows)
	}
}

func TestFabricCallRowsRemote(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires --ref neo-ref")
	}
	ctx := context.Background()
	exec := newFabricCallRowsExecutor(t)
	mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
	require.NoError(t, mgr.AddConstituent("comp", multidb.ConstituentRef{Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri}))
	for _, query := range []string{
		"USE comp CALL { USE comp.remote UNWIND [2,1] AS k RETURN k } WITH k ORDER BY k RETURN collect(k) AS ks",
		"USE comp CALL { USE comp.one RETURN 2 AS k UNION ALL USE comp.remote RETURN 1 AS k } WITH k ORDER BY k RETURN collect(k) AS ks",
		"USE comp CALL { USE comp.remote RETURN 2 AS k UNION ALL USE comp.two RETURN 1 AS k } WITH k ORDER BY k RETURN collect(k) AS ks",
	} {
		t.Run(query, func(t *testing.T) {
			for _, explicit := range []bool{false, true} {
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() {
						_, err := exec.Execute(ctx, "ROLLBACK", nil)
						require.NoError(t, err)
					}()
				}
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				encoded, err := json.Marshal(result.Rows)
				require.NoError(t, err)
				require.JSONEq(t, "[[[1,2]]]", string(encoded))
			}
		})
	}
}
