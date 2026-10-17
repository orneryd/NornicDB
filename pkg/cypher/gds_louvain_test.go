package cypher

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/louvain"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newLouvainKarateGraph is Zachary's karate club, projected undirected with
// a weight, as GDS's probe.
func newLouvainKarateGraph(t *testing.T) func(string) [][]interface{} {
	return newLouvainKarateGraphOf(t, gdsKarateIDOrder, func(member int, properties map[string]interface{}) {
		if member <= 2 {
			properties["seed"] = int64(100)
		}
	}, "{K: {properties: {seed: {property: 'seed', defaultValue: null}}}}")
}

// newLouvainKarateGraphOf is the karate club with its members in order,
// each one's extra properties, projected with the node projection.
func newLouvainKarateGraphOf(t *testing.T, order []int, extra func(int, map[string]interface{}), nodeProjection string) func(string) [][]interface{} {
	t.Helper()
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "lv")
	exec := NewStorageExecutor(engine)
	run := func(query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	var edges []interface{}
	for _, edge := range strings.Split(louvainKarateClub, ",") {
		ends := strings.Split(edge, "-")
		edges = append(edges, []interface{}{ends[0], ends[1]})
	}
	// Node ids that sort in order.
	for position, i := range order {
		properties := map[string]interface{}{"id": strconv.Itoa(i), "seq": int64(i)}
		extra(i, properties)
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("k%02d", position)), Labels: []string{"K"}, Properties: properties})
		require.NoError(t, err)
	}
	var err error
	_, err = exec.Execute(context.Background(), "UNWIND $edges AS e MATCH (a:K {id: e[0]}), (b:K {id: e[1]}) CREATE (a)-[:R {w: 1.0}]->(b)", map[string]interface{}{"edges": edges})
	require.NoError(t, err)
	run("CALL gds.graph.project('k', " + nodeProjection + ", {R: {orientation: 'UNDIRECTED', properties: 'w'}})")
	return run
}

// louvainGroups is the communities of a gds.louvain.stream call, as sorted
// lists of the members' ids.
func louvainGroups(t *testing.T, run func(string) [][]interface{}, config string) []string {
	t.Helper()
	rows := run("CALL gds.louvain.stream('k', {" + config + "}) YIELD nodeId, communityId MATCH (n:K) WHERE elementId(n) ENDS WITH nodeId OR id(n) = nodeId RETURN n.seq, communityId")
	members := map[interface{}][]int{}
	for _, row := range rows {
		members[row[1]] = append(members[row[1]], int(row[0].(int64)))
	}
	var groups []string
	for _, ids := range members {
		sort.Ints(ids)
		parts := make([]string, len(ids))
		for i, id := range ids {
			parts[i] = strconv.Itoa(id)
		}
		groups = append(groups, strings.Join(parts, " "))
	}
	sort.Strings(groups)
	return groups
}

// TestGDSLouvainStreamAsGDS: gds.louvain.stream reads the projection and
// gives GDS 2.13.12's communities and level counts on the karate club
// (#1198).
func TestGDSLouvainStreamAsGDS(t *testing.T) {
	run := newLouvainKarateGraph(t)
	count := func(config string) int {
		t.Helper()
		return len(louvainGroups(t, run, config))
	}
	// GDS: 4 communities; maxLevels 1: 6; maxIterations 1: 4; tolerance
	// 0.5: 8; weighted: 4.
	require.Equal(t, 4, count(""))
	require.Equal(t, 6, count("maxLevels: 1"))
	require.Equal(t, 4, count("maxIterations: 1"))
	require.Equal(t, 8, count("tolerance: 0.5"))
	require.Equal(t, 4, count("relationshipWeightProperty: 'w'"))
	require.Equal(t, 4, count("consecutiveIds: true, concurrency: 1, jobId: 'j', logProgress: false, sudo: false, nodeLabels: ['K'], relationshipTypes: ['R']"))

	// consecutiveIds numbers the communities by first node: the first's is 0.
	first := run("CALL gds.louvain.stream('k', {consecutiveIds: true}) YIELD nodeId, communityId, intermediateCommunityIds RETURN communityId, intermediateCommunityIds LIMIT 1")
	require.Equal(t, int64(0), first[0][0])
	require.Nil(t, first[0][1])
	// includeIntermediateCommunities: one entry per level, the last the
	// community (GDS: two levels).
	levels := run("CALL gds.louvain.stream('k', {includeIntermediateCommunities: true}) YIELD communityId, intermediateCommunityIds RETURN communityId, intermediateCommunityIds")
	for _, row := range levels {
		list := row[1].([]interface{})
		require.Len(t, list, 2)
		require.Equal(t, row[0], list[len(list)-1])
	}
	// minCommunitySize drops the small communities' rows.
	require.Less(t, len(run("CALL gds.louvain.stream('k', {minCommunitySize: 10})")), 34)
	require.Len(t, run("CALL gds.louvain.stream('k', {minCommunitySize: 1})"), 34)

	for config, message := range map[string]string{
		"includeIntermediateCommunities: true, consecutiveIds: true": "`includeIntermediateResults` and the `consecutiveIds` option cannot be used at the same time.",
		"seedProperty: 'nope'": "Seed property `nope` not found in graph with node properties: [seed]",
		"maxLevels: 0":         "Value for `maxLevels` was `0`, but must be within the range [1, 2147483647].",
		"maxIterations: 0":     "Value for `maxIterations` was `0`, but must be within the range [1, 2147483647].",
		"tolerance: -1":        "Value for `tolerance` was `-1.0`, but must be within the range [0.00, 179769313486231570000",
		"bogus: 1":             "Unexpected configuration key: bogus",
		"minCommunitySize: 0":  "Value for `minCommunitySize` was `0`, but must be within the range [1, 9223372036854775807].",
		"seedProperty: 'seed', consecutiveIds: true": "Seeding and the `consecutiveIds` option cannot be used at the same time.",
		"relationshipWeightProperty: 'nope'":         "Relationship weight property `nope` not found in relationship types ['R']. Properties existing on all relationship types: ['w']",
	} {
		_, err := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "lv")).Execute(context.Background(), "CALL gds.louvain.stream('k', {"+config+"})", nil)
		require.ErrorContains(t, err, message, config)
	}
}

// gdsKarateIDOrder is the karate club's members in GDS's internal id order
// in the probe (Neo4j reused deleted nodes' ids): GDS visits nodes in this
// order.
var gdsKarateIDOrder = []int{10, 11, 12, 13, 6, 7, 8, 9, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 1, 2, 3, 4, 5}

// TestLouvainLevelsAsGDS: the levels and their modularity are GDS 2.13.12's
// on the karate club, for both id orders GDS was probed with: its
// modularity optimization colours the nodes and moves each colour class at
// once (#1198).
func TestLouvainLevelsAsGDS(t *testing.T) {
	build := func(order []int) *louvain.Graph {
		position := map[int]int{}
		for i, member := range order {
			position[member] = i
		}
		g := louvain.NewGraph(34)
		for _, edge := range strings.Split(louvainKarateClub, ",") {
			ends := strings.Split(edge, "-")
			a, _ := strconv.Atoi(ends[0])
			b, _ := strconv.Atoi(ends[1])
			g.Link(position[a], position[b], 1)
		}
		return g
	}
	creation := make([]int, 34)
	for i := range creation {
		creation[i] = i + 1
	}
	for _, tc := range []struct {
		order        []int
		options      louvain.Options
		modularities []float64
		communities  int
	}{
		{gdsKarateIDOrder, louvain.Options{MaxLevels: 10, MaxIterations: 10, Tolerance: 0.0001}, []float64{0.3613576594345825, 0.41880341880341876}, 4},
		{gdsKarateIDOrder, louvain.Options{MaxLevels: 10, MaxIterations: 10, Tolerance: 0.5}, []float64{0.3251150558842867}, 8},
		{gdsKarateIDOrder, louvain.Options{MaxLevels: 10, MaxIterations: 1, Tolerance: 0.0001}, []float64{0.2979125575279421, 0.41584483892176194, 0.4197896120973044}, 4},
		{creation, louvain.Options{MaxLevels: 10, MaxIterations: 10, Tolerance: 0.0001}, []float64{0.3465647600262985, 0.41880341880341876}, 4},
		{creation, louvain.Options{MaxLevels: 1, MaxIterations: 10, Tolerance: 0.0001}, []float64{0.3465647600262985}, 7},
	} {
		run := louvain.Run(build(tc.order), tc.options)
		require.Len(t, run.Modularities, len(tc.modularities), "%+v", tc.options)
		for i, want := range tc.modularities {
			require.InDelta(t, want, run.Modularities[i], 1e-12, "%+v level %d", tc.options, i)
		}
		final := louvain.Final(run)
		seen := map[int]bool{}
		for _, community := range final {
			seen[community] = true
		}
		require.Len(t, seen, tc.communities, "%+v", tc.options)
	}
}

// TestGDSLouvainStreamArgumentBranches covers the remaining refusals.
func TestGDSLouvainStreamArgumentBranches(t *testing.T) {
	newLouvainKarateGraph(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "lv"))
	_, err := exec.callGdsLouvainStream(nil)
	require.ErrorContains(t, err, "graphName")
	_, err = exec.callGdsLouvainStream([]interface{}{"k", int64(5)})
	require.ErrorContains(t, err, "configuration")
	_, err = exec.callGdsLouvainStream([]interface{}{"missing"})
	require.ErrorContains(t, err, "Graph with name `missing` does not exist")
	for config, message := range map[string]string{
		"maxIterations: 'x'":                  "The value of `maxIterations` must be of type `Integer` but was `String`.",
		"tolerance: 'x'":                      "The value of `tolerance` must be of type `Double` but was `String`.",
		"includeIntermediateCommunities: 'x'": "The value of `includeIntermediateCommunities` must be of type `Boolean` but was `String`.",
		"consecutiveIds: 'x'":                 "The value of `consecutiveIds` must be of type `Boolean` but was `String`.",
		"minCommunitySize: 'x'":               "The value of `minCommunitySize` must be of type `Integer` but was `String`.",
		"relationshipWeightProperty: 5":       "The value of `relationshipWeightProperty` must be of type `String` but was `Long`.",
		"nodeLabels: 5":                       "The value of `nodeLabels` must be of type `List` but was `Long`.",
		"relationshipTypes: 5":                "The value of `relationshipTypes` must be of type `List` but was `Long`.",
		"concurrency: 0":                      "Value for `concurrency` was `0`, but must be within the range [1, 2147483647].",
		"jobId: 7":                            "Expected JobId or String. Got Long.",
		"logProgress: 'x'":                    "The value of `logProgress` must be of type `Boolean` but was `String`.",
		"seedProperty: 5":                     "The value of `seedProperty` must be of type `String` but was `Long`.",
		"nodeLabels: ['Nope']":                "Could not find the specified `nodeLabels` of ['Nope']. Available labels are ['K'].",
	} {
		_, err := exec.Execute(context.Background(), "CALL gds.louvain.stream('k', {"+config+"})", nil)
		require.ErrorContains(t, err, message, config)
	}
}

// TestGDSLouvainCommunityIDsAsGDS: the community ids are GDS 2.13.12's on
// the karate club created in order (#1198): a community is named by the
// node it grew from, at every level; with seeds, by that node's seed, or
// for an unseeded node, the largest seed + 1 + its position where GDS adds
// the largest seed to its Neo4j id (29 + position in the probe).
func TestGDSLouvainCommunityIDsAsGDS(t *testing.T) {
	creation := make([]int, 34)
	for i := range creation {
		creation[i] = i + 1
	}
	run := newLouvainKarateGraphOf(t, creation, func(member int, properties map[string]interface{}) {
		switch {
		case member >= 33:
			properties["s2"] = int64(7)
		case member == 1:
			properties["s2"] = int64(3)
		}
		if member <= 2 {
			properties["neg"] = int64(-3)
			properties["fl"] = 1.5
		}
	}, "{K: {properties: {s2: {defaultValue: null}, neg: {defaultValue: null}, fl: {defaultValue: null}}}}")
	ids := func(config string, fromGDS func(int64) int64) string {
		t.Helper()
		var parts []string
		for _, row := range run("CALL gds.louvain.stream('k', {" + config + "}) YIELD nodeId, communityId, intermediateCommunityIds MATCH (n:K) WHERE elementId(n) ENDS WITH nodeId OR id(n) = nodeId RETURN n.seq, communityId, intermediateCommunityIds ORDER BY n.seq") {
			levels := make([]string, 0)
			for _, level := range row[2].([]interface{}) {
				levels = append(levels, strconv.FormatInt(level.(int64), 10))
			}
			parts = append(parts, fmt.Sprintf("%d, %d, [%s]", row[0], row[1], strings.Join(levels, ", ")))
		}
		return strings.Join(parts, " ")
	}
	// GDS's rows, its ids translated by fromGDS.
	gds := func(rows string, fromGDS func(int64) int64) string {
		var parts []string
		for _, row := range strings.Split(strings.TrimSuffix(rows, "]"), "] ") {
			seq, rest, _ := strings.Cut(row, ", ")
			community, levels, _ := strings.Cut(rest, ", [")
			translate := func(text string) string {
				value, err := strconv.ParseInt(text, 10, 64)
				require.NoError(t, err)
				return strconv.FormatInt(fromGDS(value), 10)
			}
			var translated []string
			for _, level := range strings.Split(levels, ", ") {
				translated = append(translated, translate(level))
			}
			parts = append(parts, seq+", "+translate(community)+", ["+strings.Join(translated, ", ")+"]")
		}
		return strings.Join(parts, " ")
	}
	same := func(id int64) int64 { return id }
	require.Equal(t, gds("1, 3, [17, 3] 2, 3, [17, 3] 3, 3, [3, 3] 4, 3, [3, 3] 5, 5, [10, 5] 6, 5, [5, 5] 7, 5, [5, 5] 8, 3, [3, 3] 9, 32, [32, 32] 10, 3, [3, 3] 11, 5, [10, 5] 12, 3, [17, 3] 13, 3, [3, 3] 14, 3, [3, 3] 15, 32, [32, 32] 16, 32, [32, 32] 17, 5, [5, 5] 18, 3, [17, 3] 19, 32, [32, 32] 20, 3, [17, 3] 21, 32, [32, 32] 22, 3, [17, 3] 23, 32, [32, 32] 24, 25, [25, 25] 25, 25, [25, 25] 26, 25, [25, 25] 27, 32, [29, 32] 28, 25, [25, 25] 29, 25, [25, 25] 30, 32, [29, 32] 31, 32, [32, 32] 32, 25, [25, 25] 33, 32, [32, 32] 34, 32, [32, 32]", same),
		ids("includeIntermediateCommunities: true", same))
	// Seeded: seed 7 names the community it grew from; GDS's unseeded
	// names are 29 + position + 7, NornicDB's 7 + 1 + position.
	seeded := func(id int64) int64 {
		if id == 7 {
			return id
		}
		return id - 28
	}
	require.Equal(t, gds("1, 39, [53, 39] 2, 39, [53, 39] 3, 39, [39, 39] 4, 39, [39, 39] 5, 41, [46, 41] 6, 41, [41, 41] 7, 41, [41, 41] 8, 39, [39, 39] 9, 7, [7, 7] 10, 39, [39, 39] 11, 41, [46, 41] 12, 39, [53, 39] 13, 39, [39, 39] 14, 39, [39, 39] 15, 7, [7, 7] 16, 7, [7, 7] 17, 41, [41, 41] 18, 39, [53, 39] 19, 7, [7, 7] 20, 39, [53, 39] 21, 7, [7, 7] 22, 39, [53, 39] 23, 7, [7, 7] 24, 61, [61, 61] 25, 61, [61, 61] 26, 61, [61, 61] 27, 7, [7, 7] 28, 61, [61, 61] 29, 61, [61, 61] 30, 7, [7, 7] 31, 7, [7, 7] 32, 61, [61, 61] 33, 7, [7, 7] 34, 7, [7, 7]", seeded),
		ids("seedProperty: 's2', includeIntermediateCommunities: true", same))
	// consecutiveIds: 0, 1, … by first node.
	var got []string
	for _, row := range run("CALL gds.louvain.stream('k', {consecutiveIds: true}) YIELD nodeId, communityId MATCH (n:K) WHERE elementId(n) ENDS WITH nodeId OR id(n) = nodeId RETURN n.seq, communityId ORDER BY n.seq") {
		got = append(got, fmt.Sprint(row[1]))
	}
	require.Equal(t, "0 0 0 0 1 1 1 0 2 0 1 0 0 0 2 2 1 0 2 0 2 0 2 3 3 3 2 3 3 2 2 3 2 2", strings.Join(got, " "))
	// minCommunitySize keeps the seeded run's communities of 9 or more.
	require.Len(t, run("CALL gds.louvain.stream('k', {seedProperty: 's2', minCommunitySize: 9})"), 23)

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "lv"))
	for config, message := range map[string]string{
		"seedProperty: 'neg'": "Seeded values should be non-negative",
		"seedProperty: 'fl'":  "Provided seeding property `fl` does not comprise exclusively of long values",
	} {
		_, err := exec.Execute(context.Background(), "CALL gds.louvain.stream('k', {"+config+"})", nil)
		require.ErrorContains(t, err, message, config)
	}
	for _, tolerance := range []float64{math.Inf(1), math.NaN()} {
		_, err := exec.callGdsLouvainStream([]interface{}{"k", map[string]interface{}{"tolerance": tolerance}})
		require.ErrorContains(t, err, "Value for `tolerance` was `"+javaDoubleText(tolerance)+"`, but must be within the range [0.00, 1797", tolerance)
	}
	require.Equal(t, "Infinity", javaDoubleText(math.Inf(1)))
	require.Equal(t, "-Infinity", javaDoubleText(math.Inf(-1)))
}

// TestGDSLouvainIsolatedNodes: with no relationships GDS runs one level,
// each node its own community.
func TestGDSLouvainIsolatedNodes(t *testing.T) {
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "lv"))
	for _, query := range []string{"UNWIND range(1, 3) AS i CREATE (:Z {seq: i})", "CALL gds.graph.project('iso', 'Z', '*')"} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	result, err := exec.Execute(context.Background(), "CALL gds.louvain.stream('iso', {includeIntermediateCommunities: true}) YIELD communityId, intermediateCommunityIds RETURN communityId, intermediateCommunityIds", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0), []interface{}{int64(0)}}, {int64(1), []interface{}{int64(1)}}, {int64(2), []interface{}{int64(2)}}}, result.Rows)
	require.Len(t, louvain.Run(louvain.NewGraph(0), louvain.Options{MaxLevels: 1, MaxIterations: 1}).Levels, 1)
}

// TestGDSLouvainReadsTheProjectionAsGDS: GDS 2.13.12 on the same graphs
// (#1198 review): a directed projection is read in its direction, a
// missing weight is NaN (no node moves), GDS's gain arithmetic decides a
// tie, and large seeds keep every digit.
func TestGDSLouvainReadsTheProjectionAsGDS(t *testing.T) {
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	communities := func(run func(string) (*ExecuteResult, error), query string) []interface{} {
		t.Helper()
		result, err := run(query)
		require.NoError(t, err, query)
		ids := make([]interface{}, len(result.Rows))
		for i, row := range result.Rows {
			ids[i] = row[1]
		}
		return ids
	}
	setup := func(run func(string) (*ExecuteResult, error), queries ...string) {
		t.Helper()
		for _, query := range queries {
			_, err := run(query)
			require.NoError(t, err, query)
		}
	}

	// A star 0→1, 0→2, 0→3: GDS gives [0, 0, 0, 1] (default ids [2, 2, 2,
	// 3]); reversed, [0, 0, 0, 0].
	_, run := newGDSEngineGraph(t, "star", gdsNode("n0", "S", nil), gdsNode("n1", "S", nil), gdsNode("n2", "S", nil), gdsNode("n3", "S", nil))
	setup(run, "MATCH (a), (b) WHERE id(a) = 'n0' AND id(b) IN ['n1', 'n2', 'n3'] CREATE (a)-[:R]->(b)",
		"CALL gds.graph.project('star', 'S', 'R')", "CALL gds.graph.project('rats', 'S', {R: {orientation: 'REVERSE'}})")
	require.Equal(t, []interface{}{int64(0), int64(0), int64(0), int64(1)}, communities(run, "CALL gds.louvain.stream('star', {consecutiveIds: true})"))
	require.Equal(t, []interface{}{int64(2), int64(2), int64(2), int64(3)}, communities(run, "CALL gds.louvain.stream('star')"))
	require.Equal(t, []interface{}{int64(0), int64(0), int64(0), int64(0)}, communities(run, "CALL gds.louvain.stream('rats', {consecutiveIds: true})"))

	// An undirected path 0—1—2 weighted 1.0 then nothing: GDS weighs the
	// second NaN and leaves every node alone.
	_, run = newGDSEngineGraph(t, "nan", gdsNode("n0", "P", nil), gdsNode("n1", "P", nil), gdsNode("n2", "P", nil))
	setup(run, "MATCH (a), (b) WHERE id(a) = 'n0' AND id(b) = 'n1' CREATE (a)-[:R {w: 1.0}]->(b)",
		"MATCH (a), (b) WHERE id(a) = 'n1' AND id(b) = 'n2' CREATE (a)-[:R]->(b)",
		"CALL gds.graph.project('nan', 'P', {R: {orientation: 'UNDIRECTED', properties: 'w'}})")
	require.Equal(t, []interface{}{int64(0), int64(1), int64(2)}, communities(run, "CALL gds.louvain.stream('nan', {relationshipWeightProperty: 'w'})"))

	// Undirected 0—1 (2), 0—2 (1) and self-loops on 1 (9), 2 (1.5), 3
	// (10.5): both of node 0's gains are 0.03125 up to rounding, and GDS's
	// arithmetic puts it with node 2: [2, 1, 2, 3].
	_, run = newGDSEngineGraph(t, "tie", gdsNode("n0", "T", nil), gdsNode("n1", "T", nil), gdsNode("n2", "T", nil), gdsNode("n3", "T", nil))
	setup(run, "MATCH (a), (b) WHERE id(a) = 'n0' AND id(b) = 'n1' CREATE (a)-[:R {w: 2.0}]->(b)",
		"MATCH (a), (b) WHERE id(a) = 'n0' AND id(b) = 'n2' CREATE (a)-[:R {w: 1.0}]->(b)",
		"MATCH (a) WHERE id(a) = 'n1' CREATE (a)-[:R {w: 9.0}]->(a)",
		"MATCH (a) WHERE id(a) = 'n2' CREATE (a)-[:R {w: 1.5}]->(a)",
		"MATCH (a) WHERE id(a) = 'n3' CREATE (a)-[:R {w: 10.5}]->(a)",
		"CALL gds.graph.project('tie', 'T', {R: {orientation: 'UNDIRECTED', properties: 'w'}})")
	require.Equal(t, []interface{}{int64(2), int64(1), int64(2), int64(3)}, communities(run, "CALL gds.louvain.stream('tie', {relationshipWeightProperty: 'w'})"))

	// Seeds past 2^53 stay distinct.
	_, run = newGDSEngineGraph(t, "big",
		gdsNode("n0", "B", map[string]interface{}{"seed": int64(9007199254740992)}),
		gdsNode("n1", "B", map[string]interface{}{"seed": int64(9007199254740993)}),
		gdsNode("n2", "B", map[string]interface{}{"seed": int64(9007199254740994)}))
	setup(run, "CALL gds.graph.project('big', {B: {properties: 'seed'}}, '*')")
	require.Equal(t, []interface{}{int64(9007199254740992), int64(9007199254740993), int64(9007199254740994)}, communities(run, "CALL gds.louvain.stream('big', {seedProperty: 'seed'})"))
}
