package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestGDSNodeIdentityThroughEveryOutput: a node leaves every GDS and
// similarity output with the id it was given, however the row shows it:
// scalar and grouped node1 (one list shared by every row), node2, shared
// nodeIds, local and remote nodes together, tieBreakProperty, rows built in
// Cypher and called once per UNWIND row, the result cache, every projection
// stream and an aggregation projection. The ids include ones that start
// with NUL or look like an internal key.
func TestGDSNodeIdentityThroughEveryOutput(t *testing.T) {
	ctx := context.Background()
	check := func(name string, got, want interface{}) {
		t.Helper()
		if g, w := fmt.Sprintf("%q", got), fmt.Sprintf("%q", want); g != w {
			t.Errorf("%s: got %s, want %s", name, g, w)
		}
	}
	weird := []string{"\x00Rx", "\x00Lx", "\x00L\x00Rx", "\x00remote\x00x", "\x00", "p"}
	remote := storage.RemoteEntityValue{"elementId": "x", "labels": []interface{}{"Doc"}, "properties": map[string]interface{}{}}
	for _, id := range weird {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r73gds")
		exec := NewStorageExecutor(store)
		for _, n := range []string{id, "b", "c", "d", "s0", "q", "r"} {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(n), Labels: []string{"P"}, Properties: map[string]interface{}{"name": n}})
			require.NoError(t, err)
		}
		for _, n := range []string{"t", "u"} {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(n), Labels: []string{"T"}})
			require.NoError(t, err)
		}
		local, err := store.GetNode(storage.NodeID(id))
		require.NoError(t, err)
		sim := func(rows []interface{}, source interface{}, extra string) [][]interface{} {
			res, err := exec.Execute(ctx, "CALL nornicdb.similarity.stream($rows, {sourceNode: $source"+extra+"}) YIELD node1, node2, shared RETURN node1, node2, [s IN shared | s.nodeId] AS sharedIds ORDER BY node2", map[string]interface{}{"rows": rows, "source": source})
			if err != nil {
				return [][]interface{}{{"ERROR " + err.Error()}}
			}
			return res.Rows
		}
		row := func(s, th interface{}) interface{} { return map[string]interface{}{"source": s, "thing": th} }
		q := fmt.Sprintf("%q", id)
		// B1 single source scalar, several targets.
		base := []interface{}{row(local, "t"), row("b", "t"), row("c", "t"), row("d", "t")}
		for i, r := range sim(base, local, "") {
			check(fmt.Sprintf("B1 scalar node1 %s row%d", q, i), r[0], id)
		}
		// B2 grouped list source, several targets (one node1 list for all rows).
		rows := sim(base, []interface{}{local}, "")
		check("B2 grouped rows "+q, len(rows), 3)
		for i, r := range rows {
			check(fmt.Sprintf("B2 grouped node1 %s row%d", q, i), r[0], []interface{}{id})
		}
		// B3 grouped two sources.
		want := []string{id, "b"}
		sort.Strings(want)
		for i, r := range sim(base, []interface{}{local, "b"}, "") {
			check(fmt.Sprintf("B3 grouped2 node1 %s row%d", q, i), r[0], []interface{}{want[0], want[1]})
		}
		// B4 as a thing, several rows (shared list nodeId).
		for i, r := range sim([]interface{}{row("s0", local), row("q", local), row("r", local)}, "s0", "") {
			check(fmt.Sprintf("B4 shared nodeId %s row%d", q, i), r[2], []interface{}{id})
		}
		// B5 as candidate node2.
		rows = sim([]interface{}{row("s0", "t"), row(local, "t")}, "s0", "")
		check("B5 node2 "+q, len(rows) == 1 && rows[0][1] == id, true)
		// B6 local and remote x grouped, several targets.
		mixed := []interface{}{row(local, "t"), row(remote, "u"), row("b", "t"), row("c", "u")}
		shown := []string{id, "x"}
		sort.Strings(shown)
		rows = sim(mixed, []interface{}{local, remote}, "")
		check("B6 mixed rows "+q, len(rows), 2)
		for i, r := range rows {
			check(fmt.Sprintf("B6 mixed node1 %s row%d", q, i), r[0], []interface{}{shown[0], shown[1]})
		}
		// B7 the same statement twice (result cache).
		first := sim(base, []interface{}{local}, "")
		second := sim(base, []interface{}{local}, "")
		check("B7 repeat "+q, fmt.Sprint(second), fmt.Sprint(first))
		// B8 tie break property, grouped.
		for i, r := range sim(base, []interface{}{local}, ", tieBreakProperty: 'name'") {
			check(fmt.Sprintf("B8 tiebreak node1 %s row%d", q, i), r[0], []interface{}{id})
		}
		// B9 rows built in Cypher, grouped source, called once per UNWIND row.
		_, err = exec.Execute(ctx, "MATCH (p:P), (t:T) WHERE id(t) = 't' AND id(p) IN [$id, 'b', 'c'] CREATE (p)-[:H]->(t)", map[string]interface{}{"id": id})
		require.NoError(t, err)
		res, err := exec.Execute(ctx, "UNWIND [1, 2] AS i MATCH (s:P)-[:H]->(th) WITH i, collect({source: s, thing: th}) AS rows MATCH (src:P) WHERE id(src) = $id CALL nornicdb.similarity.stream(rows, {sourceNode: [src]}) YIELD node1, node2 RETURN i, node1, node2 ORDER BY i, node2", map[string]interface{}{"id": id})
		if err != nil {
			check("B9 cypher rows "+q, err.Error(), "")
		} else {
			check("B9 cypher rows count "+q, len(res.Rows), 4)
			for i, r := range res.Rows {
				check(fmt.Sprintf("B9 cypher node1 %s row%d", q, i), r[1], []interface{}{id})
			}
		}
		// B10 projection streams over stored ids (NUL-prefixed too).
		_, err = exec.Execute(ctx, "MATCH (p:P) WHERE id(p) IN [$id, 'b', 'c', 'd'] MATCH (t:T) CREATE (p)-[:R]->(t)", map[string]interface{}{"id": id})
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "CALL gds.graph.project('g', ['P', 'T'], {R: {orientation: 'UNDIRECTED'}})", nil)
		require.NoError(t, err)
		ids := func(query string, params map[string]interface{}, cols ...int) []string {
			res, err := exec.Execute(ctx, query, params)
			if err != nil {
				return []string{"ERROR " + err.Error()}
			}
			seen := map[string]bool{}
			add := func(v interface{}) {
				if s, ok := v.(string); ok && (s == id) {
					seen[s] = true
				} else if ok && strings.Contains(s, "x") {
					seen["WRONG:"+s] = true
				}
			}
			for _, r := range res.Rows {
				for _, c := range cols {
					if list, ok := r[c].([]interface{}); ok {
						for _, x := range list {
							add(x)
						}
					} else {
						add(r[c])
					}
				}
			}
			var out []string
			for k := range seen {
				out = append(out, k)
			}
			sort.Strings(out)
			return out
		}
		check("B10 fastRP nodeId "+q, ids("CALL gds.fastRP.stream('g', {embeddingDimension: 4, randomSeed: 1}) YIELD nodeId RETURN nodeId", nil, 0), []string{id})
		check("B10 louvain nodeId "+q, ids("CALL gds.louvain.stream('g') YIELD nodeId RETURN nodeId", nil, 0), []string{id})
		check("B10 nodeSimilarity node1/node2 "+q, ids("CALL gds.nodeSimilarity.stream('g', {topK: 50}) YIELD node1, node2 RETURN node1, node2", nil, 0, 1), []string{id})
		check("B10 nodeSimilarity filtered "+q, ids("CALL gds.nodeSimilarity.filtered.stream('g', {sourceNodeFilter: [$id], topK: 50}) YIELD node1 RETURN node1", map[string]interface{}{"id": id}, 0), []string{id})
		for _, algo := range []string{"commonNeighbors", "adamicAdar", "resourceAllocation", "preferentialAttachment", "jaccard", "predict"} {
			check("B10 linkPrediction "+algo+" "+q, ids("CALL gds.linkPrediction."+algo+".stream('g', {sourceNode: 'b', topK: 50}) YIELD node1, node2 RETURN node1, node2", nil, 0, 1), []string{id})
		}
		// B11 aggregation projection over the stored nodes.
		_, err = exec.Execute(ctx, "MATCH (p:P)-[:R]->(t:T) WITH gds.graph.project('agg', p, t) AS g RETURN g.nodeCount", nil)
		if err == nil {
			check("B11 agg nodeSimilarity "+q, ids("CALL gds.nodeSimilarity.stream('agg', {topK: 50}) YIELD node1, node2 RETURN node1, node2", nil, 0, 1), []string{id})
		} else {
			check("B11 agg project "+q, err.Error(), "")
		}
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('g')", nil)
		_, _ = exec.Execute(ctx, "CALL gds.graph.drop('agg')", nil)
	}
}
