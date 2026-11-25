package cypher

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type pdW161Fixture struct {
	t    *testing.T
	exec *StorageExecutor
}

func newPDW161Fixture(t *testing.T) pdW161Fixture {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	f := pdW161Fixture{t: t, exec: NewStorageExecutor(storage.NewNamespacedEngine(engine, "pd_w161"))}
	f.run("CREATE CONSTRAINT pd_graph_node_id FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE", nil)
	return f
}

func (f pdW161Fixture) run(query string, params map[string]any) *ExecuteResult {
	f.t.Helper()
	res, err := f.exec.Execute(f.t.Context(), query, params)
	require.NoError(f.t, err, "%s", query)
	return res
}

func (f pdW161Fixture) fill(size int, pattern string, params map[string]any) {
	f.t.Helper()
	if params == nil {
		params = make(map[string]any)
	}
	for from := 0; from < size; from += 500 {
		params["from"] = int64(from)
		params["to"] = int64(min(from+500, size) - 1)
		f.run("UNWIND range($from, $to) AS i "+pattern, params)
	}
}

func (f pdW161Fixture) timed(item, size int, variant string, query string, params map[string]any) *ExecuteResult {
	f.t.Helper()
	f.run("BEGIN", nil)
	start := time.Now()
	result := f.run(query, params)
	statement := time.Since(start)
	start = time.Now()
	f.run("COMMIT", nil)
	commit := time.Since(start)
	f.t.Logf("PDW161 item=%d size=%d variant=%s statement_ms=%.3f commit_ms=%.3f relationships_created=%d rows=%d",
		item, size, variant, float64(statement)/float64(time.Millisecond), float64(commit)/float64(time.Millisecond),
		result.Stats.RelationshipsCreated, len(result.Rows))
	return result
}

// TestPDW161Relationships covers the public S46 MERGE and S47 COMMIT workloads.
func TestPDW161Relationships(t *testing.T) {
	for _, size := range []int{1000, 4000, 9600} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			f := newPDW161Fixture(t)
			f.run("CREATE CONSTRAINT pd_relation_id FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE", nil)
			f.run("CREATE (:PDGraphNode {id: 'hub'})", nil)
			f.fill(size, "CREATE (:PDGraphNode {id: 'n' + toString(i)})", nil)
			rows := make([]any, size)
			for i := range rows {
				rows[i] = strconv.Itoa(i)
			}
			params := map[string]any{"rows": rows}
			for _, kind := range []string{"PROBE", "PD_RELATION"} {
				create := fmt.Sprintf("MATCH (h:PDGraphNode {id: 'hub'}) UNWIND $rows AS x MATCH (n:PDGraphNode {id: 'n' + x}) CREATE (h)-[:%s {id: $prefix + x, kind: 'k'}]->(n)", kind)
				merge := strings.Replace(create, "CREATE (h)", "MERGE (h)", 1)
				change := fmt.Sprintf("MATCH (:PDGraphNode {id: 'hub'})-[r:%s]->() WHERE r.id STARTS WITH 'a' SET r.eff_state = 'usable'", kind)
				params["prefix"] = "a"
				require.Equal(t, size, f.timed(2, size, kind+"_create", create, params).Stats.RelationshipsCreated)
				params["prefix"] = "b"
				require.Equal(t, size, f.timed(2, size, kind+"_merge_new", merge, params).Stats.RelationshipsCreated)
				require.Zero(t, f.timed(2, size, kind+"_merge_existing", merge, params).Stats.RelationshipsCreated)
				f.timed(3, size, kind+"_set_other_property", change, nil)
				check := fmt.Sprintf("MATCH (:PDGraphNode {id: 'hub'})-[r:%s]->() RETURN count(r), count(r.eff_state)", kind)
				require.Equal(t, [][]any{{int64(size * 2), int64(size)}}, f.run(check, nil).Rows)
			}
			f.run("BEGIN", nil)
			_, err := f.exec.Execute(t.Context(), "MATCH (a:PDGraphNode {id: 'n1'}) MATCH (b:PDGraphNode {id: 'n5'}) CREATE (a)-[:PD_RELATION {id: 'a1'}]->(b)", nil)
			if err == nil {
				_, err = f.exec.Execute(t.Context(), "COMMIT", nil)
			}
			require.Error(t, err)
			t.Logf("PDW161 item=3 size=%d duplicate_refused=%v", size, err)
			_, _ = f.exec.Execute(t.Context(), "ROLLBACK", nil)
		})
	}
}
