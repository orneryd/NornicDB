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

func TestPendingBetweenPublicMerge(t *testing.T) {
	f := newPDW161Fixture(t)
	f.run("CREATE (:PDGraphNode {id:'left'}) CREATE (:PDGraphNode {id:'right'})", nil)
	forward := "MATCH (a:PDGraphNode {id:'left'}) MATCH (b:PDGraphNode {id:'right'}) MERGE (a)-[:STAGED {id:'one'}]->(b)"
	reverse := "MATCH (a:PDGraphNode {id:'right'}) MATCH (b:PDGraphNode {id:'left'}) MERGE (a)-[:STAGED {id:'one'}]->(b)"
	f.run("BEGIN", nil)
	require.Equal(t, 1, f.run(forward, nil).Stats.RelationshipsCreated)
	require.Zero(t, f.run(forward, nil).Stats.RelationshipsCreated, "the second MERGE must see the staged first edge")
	require.Equal(t, 1, f.run(reverse, nil).Stats.RelationshipsCreated, "direction is part of the pair key")
	f.run("COMMIT", nil)
	require.Equal(t, [][]any{{int64(2)}}, f.run("MATCH ()-[r:STAGED]->() RETURN count(r)", nil).Rows)

	f.run("BEGIN", nil)
	f.run("MATCH (a:PDGraphNode {id:'left'})-[r:STAGED {id:'one'}]->(b:PDGraphNode {id:'right'}) DELETE r", nil)
	require.Equal(t, 1, f.run(forward, nil).Stats.RelationshipsCreated, "delete must remove the staged pair before MERGE")
	f.run("COMMIT", nil)
	require.Equal(t, [][]any{{int64(2)}}, f.run("MATCH ()-[r:STAGED]->() RETURN count(r)", nil).Rows)
}

// C70: the indexed label may appear second in a multi-label MERGE.
// This is the recorded production shape and its MATCH/label-order controls.
func TestPDW161LabelOrder(t *testing.T) {
	for _, fixture := range [][2]int{{10000, 300}, {150000, 3000}} {
		size, parties := fixture[0], fixture[1]
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			f := newPDW161Fixture(t)
			f.fill(size, "CREATE (:PDGraphNode:PDEntity {id: 'bg' + toString(i), kind: 'probe'})", nil)
			f.fill(parties, "CREATE (:PDGraphNode:PDIdentity {id: 'p' + toString(i), kind: 'person', status: 'confirmed', collection_id: 'c', name_key: 'k' + toString(i % 7), survivor: 'p' + toString(i - i % 3)})", nil)
			ids := make([]any, 22)
			expected := make([][]any, len(ids))
			for i := range ids {
				ids[i] = "p" + strconv.Itoa(i*3)
				expected[i] = []any{ids[i]}
			}
			params := map[string]any{"ids": ids}
			for _, verb := range []string{"MATCH", "MERGE"} {
				for _, labels := range []string{"PDGraphNode:PDIdentity", "PDIdentity:PDGraphNode"} {
					query := "UNWIND $ids AS id " + verb + " (p:" + labels + " {id: id}) RETURN p.id AS id"
					res := f.timed(6, size, verb+"_"+labels, query, params)
					require.ElementsMatch(t, expected, res.Rows)
					require.Zero(t, res.Stats.NodesCreated)
				}
			}
		})
	}
}

func TestC70MergeSecondLabelUsesIndex(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		f := newPDW161Fixture(t)
		f.fill(1_000, "CREATE (:PDGraphNode:PDEntity {id: 'bg' + toString(i)})", nil)
		f.fill(30, "CREATE (:PDGraphNode:PDIdentity {id: 'p' + toString(i)})", nil)
		ids := []any{"p0", "p3", "p6", "p9"}
		want := [][]any{{"p0"}, {"p3"}, {"p6"}, {"p9"}}
		for _, labels := range []string{"PDGraphNode:PDIdentity", "PDIdentity:PDGraphNode"} {
			for _, shape := range []struct {
				name, query string
				params      map[string]any
				want        [][]any
			}{
				{"unwind", "UNWIND $ids AS id MERGE (p:" + labels + " {id: id}) RETURN p.id AS id", map[string]any{"ids": ids}, want},
				{"standalone", "MERGE (p:" + labels + " {id: 'p0'}) RETURN p.id AS id", nil, [][]any{{"p0"}}},
			} {
				t.Run(shape.name+"/"+labels, func(t *testing.T) {
					exec := NewStorageExecutor(f.exec.storage)
					result, err := exec.Execute(t.Context(), shape.query, shape.params)
					require.NoError(t, err)
					require.Equal(t, shape.want, result.Rows)
					require.Zero(t, result.Stats.NodesCreated)
					trace := exec.LastHotPathTrace()
					require.True(t, trace.MergeSchemaLookupUsed, "indexed label %s must be considered: %+v", labels, trace)
					require.False(t, trace.MergeScanFallbackUsed, "indexed label %s must avoid a full label scan: %+v", labels, trace)
				})
			}
		}
	})
}

func BenchmarkC70MergeIndexedSecondLabel(b *testing.B) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "c70_bench")
	setup := NewStorageExecutor(store)
	ctx := b.Context()
	for _, statement := range []string{
		"CREATE CONSTRAINT c70_identity_id FOR (n:C70Identity) REQUIRE n.id IS UNIQUE",
		"UNWIND range(0, 9999) AS i CREATE (:C70Base:C70Other {id: 'bg' + toString(i)})",
		"UNWIND range(0, 299) AS i CREATE (:C70Base:C70Identity {id: 'p' + toString(i)})",
	} {
		if _, err := setup.Execute(ctx, statement, nil); err != nil {
			b.Fatal(err)
		}
	}
	ids := make([]any, 22)
	for i := range ids {
		ids[i] = "p" + strconv.Itoa(i*3)
	}
	query := "UNWIND $ids AS id MERGE (n:C70Base:C70Identity {id: id}) RETURN n.id"
	params := map[string]any{"ids": ids}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// A fresh executor prevents its per-statement node cache from masking
		// the schema lookup or fallback scan under measurement.
		exec := NewStorageExecutor(store)
		result, err := exec.Execute(ctx, query, params)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(ids) || result.Stats.NodesCreated != 0 {
			b.Fatalf("unexpected MERGE result: rows=%d created=%d", len(result.Rows), result.Stats.NodesCreated)
		}
	}
}
