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

// newWCCExecutor is an executor over a fresh store, with an empty catalog.
func newWCCExecutor(t testing.TB) (*StorageExecutor, storage.Engine) {
	graphProjections = make(map[string]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "wcc")
	return NewStorageExecutor(engine), engine
}

func wccRows(t *testing.T, exec *StorageExecutor, query string) [][]interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	return result.Rows
}

// wccComponents is a gds.wcc.stream call's componentId of each node, by its k.
func wccComponents(t *testing.T, exec *StorageExecutor, graph, config string) map[int64]int64 {
	t.Helper()
	rows := wccRows(t, exec, "CALL gds.wcc.stream('"+graph+"', {"+config+"}) YIELD nodeId, componentId RETURN gds.util.asNode(nodeId).k AS k, componentId")
	components := make(map[int64]int64, len(rows))
	for _, row := range rows {
		components[row[0].(int64)] = row[1].(int64)
	}
	return components
}

// newWCCGraph creates nodes (k, label) under stored IDs that sort in the
// given order, then the relationships, between nodes named by k.
func newWCCGraph(t *testing.T, nodes [][2]interface{}, relationships string) *StorageExecutor {
	t.Helper()
	exec, engine := newWCCExecutor(t)
	for position, node := range nodes {
		_, err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("n%02d", position)),
			Labels: []string{node[1].(string)}, Properties: map[string]interface{}{"k": int64(node[0].(int))},
		})
		require.NoError(t, err)
	}
	for _, relationship := range strings.Split(relationships, ";") {
		wccRows(t, exec, relationship)
	}
	return exec
}

// newWCCReferenceGraph is the graph of the GDS 2.13.12 probe (Neo4j
// 5.26.30), its nodes stored in GDS's node order there (k4, k6, k7, k8, k9,
// k1, k2, k3, k5), so its componentIds compare directly.
func newWCCReferenceGraph(t *testing.T) *StorageExecutor {
	exec := newWCCGraph(t, [][2]interface{}{
		{4, "Doc"}, {6, "Other"}, {7, "Doc"}, {8, "Doc"}, {9, "Version"}, {1, "Doc"}, {2, "Doc"}, {3, "Version"}, {5, "Doc"},
	}, "MATCH (a {k: 1}), (c {k: 3}) CREATE (a)-[:HAS_VERSION {w: 1.0}]->(c);"+
		"MATCH (b {k: 2}), (c {k: 3}) CREATE (b)-[:HAS_VERSION {w: 3.0}]->(c);"+
		"MATCH (d {k: 4}), (e {k: 5}) CREATE (d)-[:LINK {w: 1.0}]->(e);"+
		"MATCH (e {k: 5}), (g {k: 7}) CREATE (e)-[:SAME_DOCUMENT {w: 1.0}]->(g);"+
		"MATCH (f {k: 6}), (g {k: 7}) CREATE (f)-[:HAS_ORIGINAL {w: 1.0}]->(g);"+
		"MATCH (i {k: 9}), (h {k: 8}) CREATE (i)-[:HAS_ORIGINAL {w: 5.0}]->(h)")
	wccRows(t, exec, "CALL gds.graph.project('g', ['Doc', 'Version', 'Other'], {HAS_VERSION: {properties: 'w'}, HAS_ORIGINAL: {properties: 'w'}, SAME_DOCUMENT: {properties: 'w'}, LINK: {properties: 'w'}})")
	return exec
}

// TestGDSWCCStreamAsGDS: componentIds are GDS 2.13.12's on its probe graph:
// a component is named by its lowest node position (in the selected
// subgraph when nodeLabels selects), relationships join either way, and
// consecutiveIds, minComponentSize and a weight threshold apply as in GDS.
func TestGDSWCCStreamAsGDS(t *testing.T) {
	exec := newWCCReferenceGraph(t)
	for config, want := range map[string]map[int64]int64{
		"": {1: 5, 2: 5, 3: 5, 4: 0, 5: 0, 6: 0, 7: 0, 8: 3, 9: 3},
		"concurrency: 1, jobId: 'x', logProgress: false, sudo: true": {1: 5, 2: 5, 3: 5, 4: 0, 5: 0, 6: 0, 7: 0, 8: 3, 9: 3},
		"relationshipTypes: ['HAS_VERSION', 'SAME_DOCUMENT']":        {1: 5, 2: 5, 3: 5, 4: 0, 5: 2, 6: 1, 7: 2, 8: 3, 9: 4},
		"nodeLabels: ['Doc', 'Other']":                               {1: 4, 2: 5, 4: 0, 5: 0, 6: 0, 7: 0, 8: 3},
		"consecutiveIds: true":                                       {1: 2, 2: 2, 3: 2, 4: 0, 5: 0, 6: 0, 7: 0, 8: 1, 9: 1},
		"relationshipWeightProperty: 'w', threshold: 2.0":            {1: 5, 2: 6, 3: 6, 4: 0, 5: 8, 6: 1, 7: 2, 8: 3, 9: 3},
		"minComponentSize: 3":                                        {1: 5, 2: 5, 3: 5, 4: 0, 5: 0, 6: 0, 7: 0},
		"minComponentSize: -1":                                       {1: 5, 2: 5, 3: 5, 4: 0, 5: 0, 6: 0, 7: 0, 8: 3, 9: 3},
		// Components a size filter drops take no consecutive number.
		"minComponentSize: 3, consecutiveIds: true": {1: 1, 2: 1, 3: 1, 4: 0, 5: 0, 6: 0, 7: 0},
	} {
		require.Equal(t, want, wccComponents(t, exec, "g", config), config)
	}
	// Rows come in the projection's node order, the same on every call.
	rows := wccRows(t, exec, "CALL gds.wcc.stream('g') YIELD nodeId, componentId RETURN nodeId, componentId")
	require.Equal(t, []interface{}{"n00", int64(0)}, rows[0])
	require.Equal(t, rows, wccRows(t, exec, "CALL gds.wcc.stream('g') YIELD nodeId, componentId RETURN nodeId, componentId"))
}

// TestGDSWCCStreamThresholdAndOrientation: GDS's probe of weight
// thresholds (strictly above; a projected default weight counts; without a
// threshold the weights don't filter), projected orientations (every
// relationship joins both ways) and a self-loop.
func TestGDSWCCStreamThresholdAndOrientation(t *testing.T) {
	nodes := [][2]interface{}{{1, "Doc"}, {2, "Doc"}, {3, "Version"}, {4, "Doc"}, {5, "Doc"}, {6, "Other"}, {7, "Doc"}, {8, "Doc"}, {9, "Version"}, {10, "Doc"}}
	exec := newWCCGraph(t, nodes, "MATCH (a {k: 1}), (c {k: 3}) CREATE (a)-[:HAS_VERSION {w: 1.0}]->(c);"+
		"MATCH (b {k: 2}), (c {k: 3}) CREATE (b)-[:HAS_VERSION {w: 3.0}]->(c);"+
		"MATCH (d {k: 4}), (e {k: 5}) CREATE (d)-[:LINK {w: 0.0}]->(e);"+
		"MATCH (e {k: 5}), (g {k: 7}) CREATE (e)-[:SAME_DOCUMENT {w: -1.0}]->(g);"+
		"MATCH (f {k: 6}), (g {k: 7}) CREATE (f)-[:HAS_ORIGINAL {w: 1.0}]->(g);"+
		"MATCH (i {k: 9}), (h {k: 8}) CREATE (i)-[:HAS_ORIGINAL {w: 5.0}]->(h);"+
		"MATCH (j {k: 10}) CREATE (j)-[:LINK {w: 1.0}]->(j);"+
		"MATCH (h {k: 8}), (i {k: 9}) CREATE (h)-[:HAS_ORIGINAL]->(i)")
	wccRows(t, exec, "CALL gds.graph.project('g', ['Doc', 'Version', 'Other'], {HAS_VERSION: {properties: 'w'}, HAS_ORIGINAL: {properties: {w: {property: 'w', defaultValue: 7.5}}}, SAME_DOCUMENT: {properties: 'w'}, LINK: {properties: 'w'}})")
	wccRows(t, exec, "CALL gds.graph.project('r', ['Doc', 'Version', 'Other'], {HAS_VERSION: {orientation: 'REVERSE'}, HAS_ORIGINAL: {orientation: 'UNDIRECTED'}, SAME_DOCUMENT: {orientation: 'NATURAL'}, LINK: {orientation: 'REVERSE'}})")
	all := map[int64]int64{1: 0, 2: 0, 3: 0, 4: 3, 5: 3, 6: 3, 7: 3, 8: 7, 9: 7, 10: 9}
	require.Equal(t, all, wccComponents(t, exec, "g", ""))
	require.Equal(t, all, wccComponents(t, exec, "r", ""))
	require.Equal(t, all, wccComponents(t, exec, "g", "relationshipWeightProperty: 'w'"))
	require.Equal(t, all, wccComponents(t, exec, "g", "relationshipWeightProperty: 'w', threshold: -5"))
	require.Equal(t, map[int64]int64{1: 0, 2: 1, 3: 1, 4: 3, 5: 4, 6: 5, 7: 6, 8: 7, 9: 7, 10: 9},
		wccComponents(t, exec, "g", "relationshipWeightProperty: 'w', threshold: 1.0"))
	require.Equal(t, map[int64]int64{1: 0, 2: 0, 3: 0, 4: 3, 5: 4, 6: 5, 7: 5, 8: 7, 9: 7, 10: 9},
		wccComponents(t, exec, "g", "relationshipWeightProperty: 'w', threshold: 0.5"))
	// A threshold of zero or less filters nothing, and needs no weight.
	require.Equal(t, all, wccComponents(t, exec, "g", "relationshipWeightProperty: 'w', threshold: 0.0"))
	require.Equal(t, all, wccComponents(t, exec, "g", "threshold: 0.0"))
	require.Equal(t, all, wccComponents(t, exec, "g", "threshold: -1.0"))
	_, err := exec.Execute(context.Background(), "CALL gds.wcc.stream('g', {threshold: 2.0})", nil)
	require.ErrorContains(t, err, "Specifying a threshold requires `relationshipWeightProperty` to be set.")

	// A relationship without the weight (NaN) passes no positive threshold,
	// and joins when none applies.
	wccRows(t, exec, "CREATE (x:N {k: 100}), (y:N {k: 101}), (z:N {k: 102}), (x)-[:R]->(y), (x)-[:R {w: 2.0}]->(z)")
	wccRows(t, exec, "CALL gds.graph.project('nan', 'N', {R: {properties: 'w'}})")
	// These nodes have generated IDs, so compare the grouping, not the names.
	components := wccComponents(t, exec, "nan", "relationshipWeightProperty: 'w', threshold: 0.9999999")
	require.Equal(t, components[100], components[102])
	require.NotEqual(t, components[100], components[101])
	components = wccComponents(t, exec, "nan", "relationshipWeightProperty: 'w', threshold: -0.5")
	require.Equal(t, components[100], components[101])
	require.Equal(t, components[100], components[102])
}

// TestGDSWCCSeedsAsGDS: with seedProperty a component is named by its
// least seed, as GDS 2.13.12 names it; components that share a seed share
// the name and count as one for minComponentSize and the statistics. A
// component without seeds is named above every seed: GDS numbers those in
// its traversal order (its probe gave 22, 24 or 25 here, depending on
// concurrency); NornicDB by node order. Negative seeds count as none.
func TestGDSWCCSeedsAsGDS(t *testing.T) {
	exec, engine := newWCCExecutor(t)
	for position, node := range []struct {
		k     int64
		label string
		seed  interface{}
	}{{1, "Doc", 10}, {2, "Doc", 20}, {3, "Version", nil}, {4, "Doc", 20}, {5, "Doc", nil}, {6, "Other", 5}, {7, "Doc", nil}, {8, "Doc", nil}, {9, "Version", nil}, {10, "Doc", 3}} {
		properties := map[string]interface{}{"k": node.k}
		if node.seed != nil {
			properties["s"] = int64(node.seed.(int))
		}
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%02d", position)), Labels: []string{node.label}, Properties: properties})
		require.NoError(t, err)
	}
	for _, relationship := range []string{
		"MATCH (a {k: 1}), (c {k: 3}) CREATE (a)-[:HAS_VERSION {w: 1.0}]->(c)",
		"MATCH (b {k: 2}), (c {k: 3}) CREATE (b)-[:HAS_VERSION {w: 3.0}]->(c)",
		"MATCH (d {k: 4}), (e {k: 5}) CREATE (d)-[:LINK {w: 0.0}]->(e)",
		"MATCH (e {k: 5}), (g {k: 7}) CREATE (e)-[:SAME_DOCUMENT {w: -1.0}]->(g)",
		"MATCH (f {k: 6}), (g {k: 7}) CREATE (f)-[:HAS_ORIGINAL {w: 1.0}]->(g)",
		"MATCH (i {k: 9}), (h {k: 8}) CREATE (i)-[:HAS_ORIGINAL {w: 5.0}]->(h)",
		"MATCH (j {k: 10}) CREATE (j)-[:LINK {w: 1.0}]->(j)",
		"MATCH (h {k: 8}), (i {k: 9}) CREATE (h)-[:HAS_ORIGINAL]->(i)",
	} {
		wccRows(t, exec, relationship)
	}
	wccRows(t, exec, "CALL gds.graph.project('g', {Doc: {properties: 's'}, Version: {properties: 's'}, Other: {properties: 's'}}, ['HAS_VERSION', 'HAS_ORIGINAL', 'SAME_DOCUMENT', 'LINK'])")

	// GDS: k8 and k9 named 23 or 24; NornicDB: 20 + 1 + rank 3 among the
	// unseeded k3, k5, k7, k8, k9.
	require.Equal(t, map[int64]int64{1: 10, 2: 10, 3: 10, 4: 5, 5: 5, 6: 5, 7: 5, 8: 24, 9: 24, 10: 3}, wccComponents(t, exec, "g", "seedProperty: 's'"))
	// GDS: k3 24 or 25, k5 24 or 25, k8 and k9 22 (NornicDB 21, 22, 24).
	require.Equal(t, map[int64]int64{1: 10, 2: 20, 3: 21, 4: 20, 5: 22, 6: 5, 7: 5, 8: 24, 9: 24, 10: 3},
		wccComponents(t, exec, "g", "seedProperty: 's', relationshipTypes: ['HAS_ORIGINAL']"))
	// k2 and k4 share seed 20: one component of two for the size filter.
	require.Equal(t, map[int64]int64{2: 20, 4: 20, 6: 5, 7: 5, 8: 24, 9: 24},
		wccComponents(t, exec, "g", "seedProperty: 's', relationshipTypes: ['HAS_ORIGINAL'], minComponentSize: 2"))
	require.Equal(t, map[int64]int64{1: 10, 2: 20, 4: 20, 5: 20, 7: 20, 8: 23, 10: 3},
		wccComponents(t, exec, "g", "seedProperty: 's', nodeLabels: ['Doc']"))
	rows := wccRows(t, exec, "CALL gds.wcc.stats('g', {seedProperty: 's', relationshipTypes: ['HAS_ORIGINAL']}) YIELD componentCount, componentDistribution, configuration")
	require.Equal(t, int64(7), rows[0][0])
	require.Equal(t, map[string]interface{}{
		"p1": int64(1), "p5": int64(1), "p10": int64(1), "p25": int64(1), "p50": int64(1), "p75": int64(2), "p90": int64(2),
		"p95": int64(2), "p99": int64(2), "p999": int64(2), "min": int64(1), "max": int64(2), "mean": 10.0 / 7.0,
	}, rows[0][1])
	require.Equal(t, "s", rows[0][2].(map[string]interface{})["seedProperty"])

	for config, message := range map[string]string{
		"seedProperty: 'nope'":                    "Seed property `nope` not found in graph with node properties: [s]",
		"seedProperty: 's', consecutiveIds: true": "Seeding and the `consecutiveIds` option cannot be used at the same time.",
	} {
		_, err := exec.Execute(context.Background(), "CALL gds.wcc.stream('g', {"+config+"})", nil)
		require.ErrorContains(t, err, message, config)
	}

	// A negative seed counts as none: largest seed + 1 + its rank.
	wccRows(t, exec, "CREATE (:A {k: 200, s: -2}), (:A {k: 201, s: 4})")
	wccRows(t, exec, "CALL gds.graph.project('neg', {A: {properties: 's'}}, '*')")
	require.Equal(t, map[int64]int64{200: 5, 201: 4}, wccComponents(t, exec, "neg", "seedProperty: 's'"))
	// A Float property is refused as a seed.
	wccRows(t, exec, "CREATE (:F {k: 300, s: 2.5}), (:F {k: 301, s: 1.5})")
	wccRows(t, exec, "CALL gds.graph.project('fl', {F: {properties: 's'}}, '*')")
	_, err := exec.Execute(context.Background(), "CALL gds.wcc.stream('fl', {seedProperty: 's'})", nil)
	require.ErrorContains(t, err, "Provided seeding property `s` does not comprise exclusively of long values")
}

// wccFamilyGraph is a generic families-of-copies store: items (Doc), their
// versions and originals, joined by HAS_VERSION, HAS_ORIGINAL and
// SAME_DOCUMENT. One large family is a chain of items joined both ways by
// SAME_DOCUMENT, with a cycle, each item with a version and every third
// version with an original; small families of two to five members include a
// SAME_DOCUMENT triangle, parallel relationships and a self-loop; some items
// and versions are isolated. CITES relationships and an unprojected Note
// node connect different families and must not join them.
func newWCCFamilyGraph(t *testing.T) *StorageExecutor {
	t.Helper()
	exec, engine := newWCCExecutor(t)
	key := 0
	node := func(label string) storage.NodeID {
		key++
		id := storage.NodeID(fmt.Sprintf("x%04d", (key*7919)%10007)) // stored order differs from key order
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{label}, Properties: map[string]interface{}{"key": int64(key)}})
		require.NoError(t, err)
		return id
	}
	edges := 0
	link := func(from, to storage.NodeID, kind string) {
		edges++
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%04d", edges)), StartNode: from, EndNode: to, Type: kind}))
	}
	var chain []storage.NodeID
	for i := 0; i < 40; i++ {
		item := node("Doc")
		if i > 0 {
			if i%2 == 0 {
				link(chain[i-1], item, "SAME_DOCUMENT")
			} else {
				link(item, chain[i-1], "SAME_DOCUMENT")
			}
		}
		chain = append(chain, item)
		version := node("Version")
		link(item, version, "HAS_VERSION")
		if i%3 == 0 {
			link(version, node("Original"), "HAS_ORIGINAL")
		}
	}
	link(chain[30], chain[27], "SAME_DOCUMENT")
	var small [][]storage.NodeID
	for size := 0; size < 15; size++ {
		item, version := node("Doc"), node("Version")
		link(item, version, "HAS_VERSION")
		family := []storage.NodeID{item, version}
		switch size % 5 {
		case 1:
			original := node("Original")
			link(version, original, "HAS_ORIGINAL")
			family = append(family, original)
		case 2:
			other := node("Doc")
			link(other, item, "SAME_DOCUMENT")
			link(other, item, "SAME_DOCUMENT")
			family = append(family, other)
		case 3:
			b, c := node("Doc"), node("Doc")
			link(item, b, "SAME_DOCUMENT")
			link(b, c, "SAME_DOCUMENT")
			link(c, item, "SAME_DOCUMENT")
			family = append(family, b, c)
		case 4:
			link(item, item, "SAME_DOCUMENT")
			second := node("Version")
			link(second, item, "HAS_VERSION")
			original := node("Original")
			link(second, original, "HAS_ORIGINAL")
			family = append(family, second, original)
		}
		small = append(small, family)
	}
	var isolated []storage.NodeID
	for i := 0; i < 10; i++ {
		isolated = append(isolated, node("Doc"))
	}
	isolated = append(isolated, node("Version"), node("Original"))
	link(chain[5], small[0][0], "CITES")
	link(small[1][1], small[2][0], "CITES")
	link(small[3][0], isolated[0], "CITES")
	note := node("Note")
	link(small[4][0], note, "HAS_VERSION")
	link(note, small[5][1], "HAS_VERSION")
	link(note, isolated[1], "HAS_ORIGINAL")
	return exec
}

// wccPartition is the components of rows of (key, componentId), each as the
// sorted keys of its members, sorted.
func wccPartition(rows [][]interface{}) []string {
	members := map[interface{}][]int64{}
	for _, row := range rows {
		members[row[1]] = append(members[row[1]], row[0].(int64))
	}
	groups := make([]string, 0, len(members))
	for _, keys := range members {
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		groups = append(groups, fmt.Sprint(keys))
	}
	sort.Strings(groups)
	return groups
}

// wccFamilyLabels selects the family members.
const wccFamilyLabels = "(x:Doc OR x:Version OR x:Original)"

// TestGDSWCCStreamFamiliesMatchReachability: over a projection of exactly
// the family relationship types, gds.wcc.stream gives the components a
// plain Cypher reachability query gives (undirected, through family members
// only), including isolated members; the least Doc key per family, derived
// by the caller from the stream, matches the oracle's. A projection of the
// whole store filtered by nodeLabels and relationshipTypes, and a filtered
// Cypher aggregation projection give the same families.
func TestGDSWCCStreamFamiliesMatchReachability(t *testing.T) {
	exec := newWCCFamilyGraph(t)
	oracleRows := wccRows(t, exec, `MATCH (n) WHERE `+strings.ReplaceAll(wccFamilyLabels, "x", "n")+`
		OPTIONAL MATCH p = (n)-[:HAS_VERSION|HAS_ORIGINAL|SAME_DOCUMENT*1..]-(m)
		WHERE all(x IN nodes(p) WHERE `+wccFamilyLabels+`)
		WITH n, collect(DISTINCT m) + [n] AS family
		RETURN n.key AS key, [x IN family | x.key] AS members,
			reduce(least = null, x IN family | CASE WHEN x:Doc AND (least IS NULL OR x.key < least) THEN x.key ELSE least END) AS familyKey`)
	oracleGroups := map[string]bool{}
	oracleKey := map[int64]interface{}{}
	for _, row := range oracleRows {
		// A cycle back to the start repeats it; a member counts once.
		distinct := map[int64]bool{}
		var keys []int64
		for _, member := range row[1].([]interface{}) {
			if key := member.(int64); !distinct[key] {
				distinct[key] = true
				keys = append(keys, key)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		oracleGroups[fmt.Sprint(keys)] = true
		oracleKey[row[0].(int64)] = row[2]
	}
	want := make([]string, 0, len(oracleGroups))
	for group := range oracleGroups {
		want = append(want, group)
	}
	sort.Strings(want)
	// The large family: 40 items, 40 versions, 14 originals. 15 small
	// families of 2, 3, 3, 4 and 4 members (three of each). 12 isolated
	// members, each its own family.
	require.Len(t, want, 1+15+12)
	require.Len(t, oracleRows, 94+3*(2+3+3+4+4)+12)

	wccRows(t, exec, "CALL gds.graph.project('families', ['Doc', 'Version', 'Original'], ['HAS_VERSION', 'HAS_ORIGINAL', 'SAME_DOCUMENT'])")
	// The whole store, by its labels and types: a '*' projection holds only
	// the label __ALL__, which nodeLabels cannot select from, as in GDS.
	wccRows(t, exec, "CALL gds.graph.project('store', ['Doc', 'Version', 'Original', 'Note'], ['HAS_VERSION', 'HAS_ORIGINAL', 'SAME_DOCUMENT', 'CITES'])")
	wccRows(t, exec, `MATCH (s) WHERE `+strings.ReplaceAll(wccFamilyLabels, "x", "s")+`
		OPTIONAL MATCH (s)-[:HAS_VERSION|HAS_ORIGINAL|SAME_DOCUMENT]->(t) WHERE `+strings.ReplaceAll(wccFamilyLabels, "x", "t")+`
		RETURN gds.graph.project('families-cypher', s, t)`)
	for _, call := range []string{
		"CALL gds.wcc.stream('families')",
		"CALL gds.wcc.stream('families', {concurrency: 4})",
		"CALL gds.wcc.stream('store', {nodeLabels: ['Doc', 'Version', 'Original'], relationshipTypes: ['HAS_VERSION', 'HAS_ORIGINAL', 'SAME_DOCUMENT']})",
		"CALL gds.wcc.stream('families-cypher')",
	} {
		rows := wccRows(t, exec, call+` YIELD nodeId, componentId
			WITH gds.util.asNode(nodeId) AS n, componentId
			WITH componentId, collect(n) AS family
			WITH componentId, family, reduce(least = null, x IN family | CASE WHEN x:Doc AND (least IS NULL OR x.key < least) THEN x.key ELSE least END) AS familyKey
			UNWIND family AS n
			RETURN n.key AS key, componentId, familyKey`)
		got := wccPartition(rows)
		require.Equal(t, want, got, call)
		for _, row := range rows {
			require.Equal(t, oracleKey[row[0].(int64)], row[2], "%s: family key of %v", call, row[0])
		}
	}

	// Unfiltered, CITES and the Note join families: the filter is what
	// separates them.
	unfiltered := wccPartition(wccRows(t, exec, "CALL gds.wcc.stream('store') YIELD nodeId, componentId WITH gds.util.asNode(nodeId) AS n, componentId WHERE NOT n:Note RETURN n.key, componentId"))
	require.Less(t, len(unfiltered), len(want))

	// Each componentId is its component's lowest position in the
	// projection, whose nodes are in stored ID order.
	rows := wccRows(t, exec, "CALL gds.wcc.stream('families') YIELD nodeId, componentId RETURN nodeId, componentId")
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row[0].(string)
	}
	require.True(t, sort.StringsAreSorted(ids))
	lowest := map[interface{}]int64{}
	for i, row := range rows {
		if _, seen := lowest[row[1]]; !seen {
			lowest[row[1]] = int64(i)
		}
		require.Equal(t, lowest[row[1]], row[1], row[0])
	}

	// apoc.algo.wcc over the same members and relationships, through the
	// same union-find, gives the same families.
	wccRows(t, exec, "MATCH ()-[r:CITES]->() DELETE r")
	wccRows(t, exec, "MATCH (n:Note) DETACH DELETE n")
	apocRows := wccRows(t, exec, "MATCH (n) WHERE "+strings.ReplaceAll(wccFamilyLabels, "x", "n")+" WITH collect(n) AS nodes CALL apoc.algo.wcc(nodes) YIELD node, componentId RETURN node.key, componentId")
	got := wccPartition(apocRows)
	require.Equal(t, want, got)
}

// TestGDSWCCStatsAsGDS: gds.wcc.stats reports the number of components and
// their size distribution as GDS 2.13.12 does, its percentiles rounded as
// GDS's histogram rounds them, and the configuration GDS reports.
func TestGDSWCCStatsAsGDS(t *testing.T) {
	exec := newWCCReferenceGraph(t)
	result, err := exec.Execute(context.Background(), "CALL gds.wcc.stats('g', {jobId: 'j'})", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"componentCount", "componentDistribution", "postProcessingMillis", "preProcessingMillis", "computeMillis", "configuration"}, result.Columns)
	row := result.Rows[0]
	require.Equal(t, int64(3), row[0])
	require.Equal(t, map[string]interface{}{
		"p1": int64(2), "p5": int64(2), "p10": int64(2), "p25": int64(2), "p50": int64(3), "p75": int64(4), "p90": int64(4),
		"p95": int64(4), "p99": int64(4), "p999": int64(4), "min": int64(2), "max": int64(4), "mean": 3.0,
	}, row[1])
	require.Equal(t, map[string]interface{}{
		"jobId": "j", "seedProperty": nil, "consecutiveIds": false, "threshold": 0.0, "logProgress": true,
		"nodeLabels": []interface{}{"*"}, "sudo": false, "relationshipTypes": []interface{}{"*"}, "concurrency": int64(4),
	}, row[5])

	result, err = exec.Execute(context.Background(), "CALL gds.wcc.stats('g', {relationshipWeightProperty: 'w', threshold: 1, consecutiveIds: true, nodeLabels: ['Doc'], relationshipTypes: ['LINK'], concurrency: 2, jobId: 'j', logProgress: false, sudo: true})", nil)
	require.NoError(t, err)
	require.Equal(t, int64(6), result.Rows[0][0]) // 6 Docs: the LINK weighs 1, not above 1
	require.Equal(t, map[string]interface{}{
		"jobId": "j", "seedProperty": nil, "consecutiveIds": true, "threshold": 1.0, "logProgress": false, "relationshipWeightProperty": "w",
		"nodeLabels": []interface{}{"Doc"}, "sudo": true, "relationshipTypes": []interface{}{"LINK"}, "concurrency": int64(2),
	}, result.Rows[0][5])

	_, err = exec.Execute(context.Background(), "CALL gds.wcc.stats('g', {minComponentSize: 2})", nil)
	require.ErrorContains(t, err, "Unexpected configuration key: minComponentSize")
}

// TestGDSWCCStatsDistributionRounding: 30 components of 1 … 30 nodes, as
// GDS's probe: p10 is 3 (HdrHistogram rounds the count just below the
// percentile up), where rounding 0.1 × 30 up would give 4.
func TestGDSWCCStatsDistributionRounding(t *testing.T) {
	exec, _ := newWCCExecutor(t)
	wccRows(t, exec, "UNWIND range(1, 30) AS size UNWIND range(1, size) AS i CREATE (:C {size: size, i: i})")
	wccRows(t, exec, "MATCH (a:C), (b:C) WHERE a.size = b.size AND b.i = a.i + 1 CREATE (a)-[:NEXT]->(b)")
	wccRows(t, exec, "CALL gds.graph.project('h', 'C', 'NEXT')")
	rows := wccRows(t, exec, "CALL gds.wcc.stats('h') YIELD componentCount, componentDistribution")
	require.Equal(t, int64(30), rows[0][0])
	require.Equal(t, map[string]interface{}{
		"p1": int64(1), "p5": int64(2), "p10": int64(3), "p25": int64(8), "p50": int64(15), "p75": int64(23), "p90": int64(27),
		"p95": int64(29), "p99": int64(30), "p999": int64(30), "min": int64(1), "max": int64(30), "mean": 15.5,
	}, rows[0][1])
	rows = wccRows(t, exec, "CALL gds.graph.list('h') YIELD degreeDistribution")
	require.Equal(t, map[string]interface{}{
		"p50": int64(1), "p75": int64(1), "p90": int64(1), "p95": int64(1), "p99": int64(1), "p999": int64(1),
		"min": int64(0), "max": int64(1), "mean": 435.0 / 465.0,
	}, rows[0][0])
}

// TestGDSWCCEmptyGraph: a graph without nodes has no components, and GDS
// reports an empty distribution.
func TestGDSWCCEmptyGraph(t *testing.T) {
	exec, _ := newWCCExecutor(t)
	projection := &GraphProjection{Name: "empty", Database: exec.databaseName(), nodeIndex: map[string]int{}, NodeLabels: map[string][]string{}}
	graphProjections[gdsCatalogKey(projection.Database, projection.Name)] = projection
	require.Empty(t, wccRows(t, exec, "CALL gds.wcc.stream('empty')"))
	rows := wccRows(t, exec, "CALL gds.wcc.stats('empty') YIELD componentCount, componentDistribution")
	require.Equal(t, []interface{}{int64(0), map[string]interface{}{}}, rows[0])
}

// TestGDSWCCRefusesAsGDS: what GDS 2.13.12 refuses, with its messages.
func TestGDSWCCRefusesAsGDS(t *testing.T) {
	exec := newWCCReferenceGraph(t)
	for config, message := range map[string]string{
		"bogus: 1":                                        "Unexpected configuration key: bogus",
		"concurrency: 0":                                  "Valid values for Concurrency are int[1..], Value provided was `0`.",
		"concurrency: 1.5":                                "Unsupported Concurrency input of type Double",
		"nodeLabels: ['Nope']":                            "Could not find the specified `nodeLabels` of ['Nope']. Available labels are ['Doc', 'Other', 'Version'].",
		"relationshipTypes: ['NOPE']":                     "Could not find the specified `relationshipTypes` of ['NOPE']. Available relationship types are ['HAS_ORIGINAL', 'HAS_VERSION', 'LINK', 'SAME_DOCUMENT'].",
		"relationshipWeightProperty: 'nope'":              "Relationship weight property `nope` not found in relationship types ['HAS_ORIGINAL', 'HAS_VERSION', 'LINK', 'SAME_DOCUMENT']. Properties existing on all relationship types: ['w']",
		"threshold: 'x', relationshipWeightProperty: 'w'": "The value of `threshold` must be of type `Double` but was `String`.",
		"threshold: 2.0":                                  "Specifying a threshold requires `relationshipWeightProperty` to be set.",
		"consecutiveIds: 1":                               "The value of `consecutiveIds` must be of type `Boolean` but was `Long`.",
		"minComponentSize: 'x'":                           "The value of `minComponentSize` must be of type `Integer` but was `String`.",
		"jobId: 7":                                        "Expected JobId or String. Got Long.",
		"logProgress: 'x'":                                "The value of `logProgress` must be of type `Boolean` but was `String`.",
		"sudo: 'x'":                                       "The value of `sudo` must be of type `Boolean` but was `String`.",
	} {
		_, err := exec.Execute(context.Background(), "CALL gds.wcc.stream('g', {"+config+"})", nil)
		require.ErrorContains(t, err, message, config)
	}
	_, err := exec.Execute(context.Background(), "CALL gds.wcc.stream('missing')", nil)
	require.ErrorContains(t, err, "Graph with name `missing` does not exist on database")
	_, err = exec.Execute(context.Background(), "CALL gds.wcc.stats('missing')", nil)
	require.ErrorContains(t, err, "Graph with name `missing` does not exist on database")
}

// TestGDSWCCStreamIntegerNodeIDs: over an aggregation projection of INTEGER
// ids, nodeId is the INTEGER the row supplied and components are named by
// GDS's node order, as GDS 2.13.12's probe gave.
func TestGDSWCCStreamIntegerNodeIDs(t *testing.T) {
	exec, _ := newWCCExecutor(t)
	wccRows(t, exec, "UNWIND [[10, 20], [20, 30], [40, null], [50, 60], [60, 50]] AS e WITH gds.graph.project('ints', e[0], e[1]) AS g RETURN g.nodeCount")
	require.Equal(t, [][]interface{}{
		{int64(10), int64(0)}, {int64(20), int64(0)}, {int64(30), int64(0)}, {int64(40), int64(3)}, {int64(50), int64(4)}, {int64(60), int64(4)},
	}, wccRows(t, exec, "CALL gds.wcc.stream('ints') YIELD nodeId, componentId RETURN nodeId, componentId ORDER BY nodeId"))
	require.Equal(t, [][]interface{}{
		{int64(10), int64(0)}, {int64(20), int64(0)}, {int64(30), int64(0)}, {int64(40), int64(1)}, {int64(50), int64(2)}, {int64(60), int64(2)},
	}, wccRows(t, exec, "CALL gds.wcc.stream('ints', {consecutiveIds: true}) YIELD nodeId, componentId RETURN nodeId, componentId ORDER BY nodeId"))
}

// TestGDSWCCProcedureSignatures: the procedures are listed with GDS's
// parameters and columns; nodeId is ANY, as Louvain's: the INTEGER an
// aggregation row supplied, or the stored ID.
func TestGDSWCCProcedureSignatures(t *testing.T) {
	exec, _ := newWCCExecutor(t)
	rows := wccRows(t, exec, "SHOW PROCEDURES YIELD name, signature WHERE name STARTS WITH 'gds.wcc.' RETURN name, signature ORDER BY name")
	require.Equal(t, [][]interface{}{
		{"gds.wcc.stats", "gds.wcc.stats(graphName :: STRING, configuration = {} :: MAP) :: (componentCount :: INTEGER, componentDistribution :: MAP, postProcessingMillis :: INTEGER, preProcessingMillis :: INTEGER, computeMillis :: INTEGER, configuration :: MAP)"},
		{"gds.wcc.stream", "gds.wcc.stream(graphName :: STRING, configuration = {} :: MAP) :: (nodeId :: ANY, componentId :: INTEGER)"},
	}, rows)
}
