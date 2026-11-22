package cypher

import (
	"bytes"
	"fmt"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
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
	before := pdW161Counters()
	var cpu bytes.Buffer
	profileAction := item == 5 && size == 8000
	if profileAction {
		require.NoError(f.t, pprof.StartCPUProfile(&cpu))
		defer pprof.StopCPUProfile()
	}
	start := time.Now()
	result := f.run(query, params)
	statement := time.Since(start)
	afterStatement := pdW161Counters()
	if profileAction {
		pprof.StopCPUProfile()
		pdW161Profile(f.t, variant, &cpu)
	}
	start = time.Now()
	f.run("COMMIT", nil)
	commit := time.Since(start)
	afterCommit := pdW161Counters()
	f.t.Logf("PDW161CAUSE item=%d size=%d variant=%s statement=%v commit=%v fields=[pair_calls pair_pending_visits unique_calls unique_pending_slots merge_schema_probes first_identity unique_hits merge_equality_probes merge_scan_candidates]",
		item, size, variant, pdW161CounterDelta(afterStatement, before), pdW161CounterDelta(afterCommit, afterStatement))
	f.t.Logf("PDW161 item=%d size=%d variant=%s statement_ms=%.3f commit_ms=%.3f relationships_created=%d rows=%d",
		item, size, variant, float64(statement)/float64(time.Millisecond), float64(commit)/float64(time.Millisecond),
		result.Stats.RelationshipsCreated, len(result.Rows))
	return result
}

func pdW161Counters() [9]int64 {
	return [9]int64{
		storage.PDW161PairCalls.Load(), storage.PDW161PairPendingVisits.Load(),
		storage.PDW161UniqueCalls.Load(), storage.PDW161UniquePendingVisits.Load(),
		pdW161MergeSeekCalls.Load(), pdW161FirstIdentity.Load(), pdW161IndexedHits.Load(),
		pdW161EqualityCalls.Load(), pdW161MergeScanVisits.Load(),
	}
}

func pdW161CounterDelta(after, before [9]int64) [9]int64 {
	for i := range after {
		after[i] -= before[i]
	}
	return after
}

func pdW161Profile(t *testing.T, variant string, cpu *bytes.Buffer) {
	t.Helper()
	p, err := profile.ParseData(cpu.Bytes())
	require.NoError(t, err)
	byLeaf, byCaller := make(map[string]int64), make(map[string]int64)
	var total int64
	for _, sample := range p.Sample {
		weight := sample.Value[len(sample.Value)-1]
		total += weight
		seen := make(map[string]bool)
		leaf := true
		for _, location := range sample.Location {
			for _, line := range location.Line {
				name := strings.TrimPrefix(line.Function.Name, "github.com/orneryd/nornicdb/")
				where := fmt.Sprintf("%s @ %s:%d", name, line.Function.Filename, line.Line)
				if leaf {
					byLeaf[where] += weight
					leaf = false
				}
				if !seen[where] {
					byCaller[where] += weight
					seen[where] = true
				}
			}
		}
	}
	t.Logf("PDW161PROFILE variant=%s sampled_cpu=%s", variant, time.Duration(total))
	for _, table := range []struct {
		kind string
		data map[string]int64
	}{{"leaf", byLeaf}, {"stack", byCaller}} {
		names := make([]string, 0, len(table.data))
		for key := range table.data {
			names = append(names, key)
		}
		sort.Slice(names, func(i, j int) bool {
			if table.data[names[i]] == table.data[names[j]] {
				return names[i] < names[j]
			}
			return table.data[names[i]] > table.data[names[j]]
		})
		for _, name := range names[:min(24, len(names))] {
			t.Logf("PDW161PROFILE variant=%s %s cpu=%s %s", variant, table.kind, time.Duration(table.data[name]), name)
		}
	}
}

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

func TestPDW161Computed(t *testing.T) {
	for _, size := range []int{2000, 8000, 16000} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			f := newPDW161Fixture(t)
			for _, variant := range []struct {
				name       string
				clause     string
				expression string
				existing   bool
			}{
				{"on_match_plain", "MERGE (n:PDGraphNode {id: row.id}) ON MATCH SET", "row.kept", true},
				{"on_match_case", "MERGE (n:PDGraphNode {id: row.id}) ON MATCH SET", "CASE WHEN row.carried THEN row.value ELSE n.value END", true},
				{"on_create_plain", "MERGE (n:PDGraphNode {id: row.id}) ON CREATE SET", "row.kept", false},
				{"on_create_case", "MERGE (n:PDGraphNode {id: row.id}) ON CREATE SET", "CASE WHEN row.carried THEN row.value ELSE 'old' END", false},
				{"plain_set_case", "MERGE (n:PDGraphNode {id: row.id}) SET", "CASE WHEN row.carried THEN row.value ELSE n.value END", true},
			} {
				prefix := variant.name + "-"
				if variant.existing {
					f.fill(size, "CREATE (:PDGraphNode:PDEntity {id: $prefix + toString(i), value: 'old'})", map[string]any{"prefix": prefix})
				}
				rows := make([]any, size)
				for i := range rows {
					value := "v" + strconv.Itoa(i)
					var kept string
					if i%2 == 0 {
						kept = value
					} else {
						kept = "old"
					}
					rows[i] = map[string]any{"id": prefix + strconv.Itoa(i), "value": value, "carried": i%2 == 0, "kept": kept}
				}
				params := map[string]any{"rows": rows}
				statement := "UNWIND $rows AS row " + variant.clause + " n.value = " + variant.expression + " RETURN count(n) AS n"
				require.Equal(t, [][]any{{int64(size)}}, f.timed(5, size, variant.name, statement, params).Rows)
				require.Equal(t, [][]any{{int64(size)}}, f.run("UNWIND $rows AS row MATCH (n:PDGraphNode {id: row.id}) WHERE n.value = row.kept RETURN count(n) AS n", params).Rows)
			}
		})
	}
}

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
					if verb == "MERGE" && labels == "PDIdentity:PDGraphNode" {
						pdW161UseIndexedSecond.Store(true)
						t.Cleanup(func() { pdW161UseIndexedSecond.Store(false) })
						res = f.timed(6, size, "MERGE_unindexed_first_diagnostic_second_label_probe", query, params)
						pdW161UseIndexedSecond.Store(false)
						require.ElementsMatch(t, expected, res.Rows)
						require.Zero(t, res.Stats.NodesCreated)
					}
				}
			}
		})
	}
}
