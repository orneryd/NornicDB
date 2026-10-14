package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newLouvainKarateGraph is Zachary's karate club, projected undirected with
// a weight, as GDS's probe.
func newLouvainKarateGraph(t *testing.T) func(string) [][]interface{} {
	t.Helper()
	graphProjections = make(map[string]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
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
	// Node ids that sort in GDS's internal id order of its probe.
	for position, i := range gdsKarateIDOrder {
		properties := map[string]interface{}{"id": strconv.Itoa(i), "seq": int64(i)}
		if i <= 2 {
			properties["seed"] = int64(100)
		}
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("k%02d", position)), Labels: []string{"K"}, Properties: properties})
		require.NoError(t, err)
	}
	var err error
	_, err = exec.Execute(context.Background(), "UNWIND $edges AS e MATCH (a:K {id: e[0]}), (b:K {id: e[1]}) CREATE (a)-[:R {w: 1.0}]->(b)", map[string]interface{}{"edges": edges})
	require.NoError(t, err)
	run("CALL gds.graph.project('k', {K: {properties: {seed: {property: 'seed', defaultValue: null}}}}, {R: {orientation: 'UNDIRECTED', properties: 'w'}})")
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

	// Communities numbered by first node: node 1's is 0.
	first := run("CALL gds.louvain.stream('k') YIELD nodeId, communityId, intermediateCommunityIds RETURN communityId, intermediateCommunityIds LIMIT 1")
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
	// seedProperty: nodes 1 and 2 seeded 100 keep the seed as their id.
	seeded := run("CALL gds.louvain.stream('k', {seedProperty: 'seed'}) YIELD nodeId, communityId MATCH (n:K) WHERE elementId(n) ENDS WITH nodeId OR id(n) = nodeId RETURN n.seq, communityId ORDER BY n.seq LIMIT 2")
	require.Equal(t, [][]interface{}{{int64(1), int64(100)}, {int64(2), int64(100)}}, seeded)

	for config, message := range map[string]string{
		"includeIntermediateCommunities: true, consecutiveIds: true": "`includeIntermediateResults` and the `consecutiveIds` option cannot be used at the same time.",
		"seedProperty: 'nope'":               "Seed property `nope` not found in graph with node properties: [seed]",
		"maxLevels: 0":                       "Value for `maxLevels` was `0`, but must be within the range [1, 2147483647].",
		"maxIterations: 0":                   "Value for `maxIterations` was `0`, but must be within the range [1, 2147483647].",
		"tolerance: -1":                      "Value for `tolerance` was `-1.0`, but must be within the range [0.00, 179769313486231570000",
		"bogus: 1":                           "Unexpected configuration key: bogus",
		"minCommunitySize: -1":               "Value for `minCommunitySize` was `-1`, but must be within the range [0, 2147483647].",
		"relationshipWeightProperty: 'nope'": "Relationship weight property `nope` not found in relationship types ['R']. Properties existing on all relationship types: ['w']",
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
	build := func(order []int) *louvainGraph {
		position := map[int]int{}
		for i, member := range order {
			position[member] = i
		}
		g := newLouvainGraph(34)
		for _, edge := range strings.Split(louvainKarateClub, ",") {
			ends := strings.Split(edge, "-")
			a, _ := strconv.Atoi(ends[0])
			b, _ := strconv.Atoi(ends[1])
			g.link(position[a], position[b], 1)
		}
		return g
	}
	creation := make([]int, 34)
	for i := range creation {
		creation[i] = i + 1
	}
	for _, tc := range []struct {
		order        []int
		options      louvainOptions
		modularities []float64
		communities  int
	}{
		{gdsKarateIDOrder, louvainOptions{maxLevels: 10, maxIterations: 10, tolerance: 0.0001}, []float64{0.3613576594345825, 0.41880341880341876}, 4},
		{gdsKarateIDOrder, louvainOptions{maxLevels: 10, maxIterations: 10, tolerance: 0.5}, []float64{0.3251150558842867}, 8},
		{gdsKarateIDOrder, louvainOptions{maxLevels: 10, maxIterations: 1, tolerance: 0.0001}, []float64{0.2979125575279421, 0.41584483892176194, 0.4197896120973044}, 4},
		{creation, louvainOptions{maxLevels: 10, maxIterations: 10, tolerance: 0.0001}, []float64{0.3465647600262985, 0.41880341880341876}, 4},
		{creation, louvainOptions{maxLevels: 1, maxIterations: 10, tolerance: 0.0001}, []float64{0.3465647600262985}, 7},
	} {
		run := runLouvain(build(tc.order), tc.options)
		require.Len(t, run.modularities, len(tc.modularities), "%+v", tc.options)
		for i, want := range tc.modularities {
			require.InDelta(t, want, run.modularities[i], 1e-12, "%+v level %d", tc.options, i)
		}
		final := louvainFinal(run, 34)
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
