package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// apocPathFixture is the graph the apoc.path rows below were read on, from
// Neo4j 5.26.30 with APOC (#907).
const apocPathFixture = `CREATE (a:N:A {id:'a'}), (b:N:B {id:'b'}), (c:N:C {id:'c'}), (d:N:A {id:'d'}), (e:N:B {id:'e'}), (f:N:X {id:'f'}), (z:N {id:'z'}), (s:N:S {id:'s'})
CREATE (a)-[:R {id:'ab'}]->(b), (b)-[:R {id:'bc'}]->(c), (c)-[:T {id:'cd'}]->(d), (a)-[:T {id:'ad'}]->(d), (d)-[:R {id:'de'}]->(e), (e)-[:R {id:'ea'}]->(a), (b)-[:T {id:'bf'}]->(f), (f)-[:R {id:'fc'}]->(c), (s)-[:R {id:'ss'}]->(s)`

func newApocPathFixture(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, apocPathFixture, nil)
	require.NoError(t, err)
	return exec, ctx
}

// TestApocPathRowsMatchAPOC pins APOC's rows for the rules the expander
// follows: each path is its start node's id and its relationships' ids.
func TestApocPathRowsMatchAPOC(t *testing.T) {
	exec, ctx := newApocPathFixture(t)
	const paths = " YIELD path WITH [nodes(path)[0].id] + [r IN relationships(path) | r.id] AS p ORDER BY p RETURN collect(p) AS paths"
	for _, test := range []struct {
		name, call string
		want       interface{}
	}{
		{"a sequence repeats from the start node's label filter",
			"MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {sequence: 'N,R>,B,R>|T>,N'})" + paths,
			[]interface{}{[]interface{}{"a"}, []interface{}{"a", "ab"}, []interface{}{"a", "ab", "bc"}, []interface{}{"a", "ab", "bf"}, []interface{}{"a", "ab", "bf", "fc"}}},
		{"beginSequenceAtStart false: the first relationship filter is the first step's only",
			"MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {sequence: 'R>,B,T>|R>,N', beginSequenceAtStart: false, maxLevel: 4})" + paths,
			[]interface{}{[]interface{}{"a"}, []interface{}{"a", "ab"}, []interface{}{"a", "ab", "bc"}, []interface{}{"a", "ab", "bf"}}},
		{"below minLevel a termination node doesn't stop the path",
			"MATCH (s0 {id:'b'}) CALL apoc.path.expandConfig(s0, {labelFilter: '/C', minLevel: 2, maxLevel: 4})" + paths,
			[]interface{}{[]interface{}{"b", "ab", "ad", "cd"}, []interface{}{"b", "ab", "ea", "de", "cd"}, []interface{}{"b", "bf", "fc"}}},
		{"a path never turns straight back, even without uniqueness",
			"MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {uniqueness: 'NONE', maxLevel: 2, relationshipFilter: 'R'})" + paths,
			[]interface{}{[]interface{}{"a"}, []interface{}{"a", "ab"}, []interface{}{"a", "ab", "bc"}, []interface{}{"a", "ea"}, []interface{}{"a", "ea", "de"}}},
		{"expand with an end label",
			"MATCH (s0 {id:'a'}) CALL apoc.path.expand(s0, 'R>|T>', '>C', 1, 3)" + paths,
			[]interface{}{[]interface{}{"a", "ab", "bc"}, []interface{}{"a", "ab", "bf", "fc"}}},
		{"each start node is returned, an element id one too",
			"MATCH (s0 {id:'a'}) CALL apoc.path.subgraphNodes([elementId(s0), s0], {maxLevel: 1}) YIELD node WITH node ORDER BY node.id RETURN collect(node.id) AS ids",
			[]interface{}{"a", "a", "b", "d", "e"}},
		{"filterStartNode applies the sequence's first label filter to the start",
			"MATCH (s0 {id:'a'}) CALL apoc.path.subgraphNodes(s0, {sequence: 'B,R>,B', filterStartNode: true}) YIELD node RETURN collect(node.id) AS ids",
			[]interface{}{}},
		{"endNodes", "MATCH (s0 {id:'a'}), (t {id:'c'}) CALL apoc.path.expandConfig(s0, {endNodes: [t], relationshipFilter: 'R>', maxLevel: 4})" + paths,
			[]interface{}{[]interface{}{"a", "ab", "bc"}}},
		{"terminatorNodes", "MATCH (s0 {id:'a'}), (t {id:'b'}) CALL apoc.path.expandConfig(s0, {terminatorNodes: [t], maxLevel: 3})" + paths,
			[]interface{}{[]interface{}{"a", "ab"}, []interface{}{"a", "ad", "cd", "bc"}}},
		{"allowlistNodes", "MATCH (s0 {id:'a'}), (t {id:'b'}), (u {id:'c'}) CALL apoc.path.expandConfig(s0, {allowlistNodes: [t, u], maxLevel: 3})" + paths,
			[]interface{}{[]interface{}{"a"}, []interface{}{"a", "ab"}, []interface{}{"a", "ab", "bc"}}},
		{"denylistNodes", "MATCH (s0 {id:'a'}), (t {id:'b'}) CALL apoc.path.expandConfig(s0, {denylistNodes: [t], relationshipFilter: 'R>', maxLevel: -1})" + paths,
			[]interface{}{[]interface{}{"a"}}},
		{"NODE_PATH uniqueness", "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {uniqueness: 'NODE_PATH', relationshipFilter: 'R>|T>', maxLevel: 3})" + paths,
			[]interface{}{[]interface{}{"a"}, []interface{}{"a", "ab"}, []interface{}{"a", "ab", "bc"}, []interface{}{"a", "ab", "bc", "cd"}, []interface{}{"a", "ab", "bf"}, []interface{}{"a", "ab", "bf", "fc"}, []interface{}{"a", "ad"}, []interface{}{"a", "ad", "de"}}},
		// Which relationship a path claims first follows the store's order;
		// the number of paths doesn't.
		{"RELATIONSHIP_GLOBAL uniqueness", "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {uniqueness: 'RELATIONSHIP_GLOBAL', maxLevel: 3}) YIELD path RETURN count(path) AS paths",
			int64(9)},
		{"optional yields a null for a start with no result", "MATCH (s0 {id:'a'}) CALL apoc.path.subgraphNodes(s0, {relationshipFilter: 'NOPE', minLevel: 1, optional: true}) YIELD node RETURN collect(node IS NULL) AS nulls",
			[]interface{}{true}},
		{"filterStartNode with beginSequenceAtStart false has no label filter for the start",
			"MATCH (s0 {id:'a'}) CALL apoc.path.subgraphNodes(s0, {sequence: 'R>,B', beginSequenceAtStart: false, filterStartNode: true, maxLevel: 1}) YIELD node WITH node ORDER BY node.id RETURN collect(node.id) AS ids",
			[]interface{}{"a", "b"}},
		{"a number setting may be a fraction or a string, truncated",
			"MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {maxLevel: '1.9', minLevel: 0.9, relationshipFilter: 'R>'})" + paths,
			[]interface{}{[]interface{}{"a"}, []interface{}{"a", "ab"}}},
		{"maxLevel below -1 reaches nothing", "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {maxLevel: -2})" + paths,
			[]interface{}{}},
		{"limit 0 returns nothing", "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {limit: 0})" + paths,
			[]interface{}{}},
		{"a boolean setting is true unless false, 0, null or a false word",
			"MATCH (s0 {id:'a'}) CALL apoc.path.subgraphNodes(s0, {sequence: 'B,R>,B', filterStartNode: 'yes'}) YIELD node RETURN collect(node.id) AS ids",
			[]interface{}{}},
		{"only expandConfig reads uniqueness",
			"MATCH (s0 {id:'a'}) CALL apoc.path.subgraphNodes(s0, {uniqueness: 5, relationshipFilter: 'R>'}) YIELD node WITH node ORDER BY node.id RETURN collect(node.id) AS ids",
			[]interface{}{"a", "b", "c"}},
		{"a sequence that needs no second step needs no second relationship filter",
			"MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {sequence: 'R>,B', beginSequenceAtStart: false, maxLevel: 1})" + paths,
			[]interface{}{[]interface{}{"a"}, []interface{}{"a", "ab"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, test.call, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{test.want}}, result.Rows)
		})
	}

	t.Run("subgraphAll returns empty lists when nothing is reached, optional or not", func(t *testing.T) {
		for _, optional := range []string{"false", "true"} {
			result, err := exec.Execute(ctx, "MATCH (s0 {id:'a'}) CALL apoc.path.subgraphAll(s0, {relationshipFilter: 'NOPE', minLevel: 1, optional: "+optional+"}) YIELD nodes, relationships RETURN size(nodes), size(relationships)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0), int64(0)}}, result.Rows)
		}
	})

	t.Run("subgraphAll returns the relationships between the nodes reached", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (s0 {id:'a'}) CALL apoc.path.subgraphAll(s0, {relationshipFilter: 'R>', maxLevel: 2}) YIELD nodes, relationships UNWIND nodes AS n WITH relationships, n ORDER BY n.id WITH relationships, collect(n.id) AS ids UNWIND relationships AS r WITH ids, r ORDER BY r.id RETURN ids, collect(r.id) AS rels", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{"a", "b", "c"}, []interface{}{"ab", "bc"}}}, result.Rows)
	})
}

// TestApocPathFailuresMatchAPOC pins the calls APOC fails with
// ProcedureCallFailed.
func TestApocPathFailuresMatchAPOC(t *testing.T) {
	exec, ctx := newApocPathFixture(t)
	for name, call := range map[string]string{
		"subgraphNodes":                              "MATCH (s0 {id:'a'}) CALL apoc.path.subgraphNodes(s0, {minLevel: 2}) YIELD node RETURN node",
		"subgraphAll":                                "MATCH (s0 {id:'a'}) CALL apoc.path.subgraphAll(s0, {minLevel: -1}) YIELD nodes RETURN nodes",
		"spanningTree":                               "MATCH (s0 {id:'a'}) CALL apoc.path.spanningTree(s0, {minLevel: 3}) YIELD path RETURN path",
		"operator without label":                     "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {labelFilter: 'A|>'}) YIELD path RETURN path",
		"operator in a sequence":                     "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {sequence: 'N,R>,+'}) YIELD path RETURN path",
		"no relationship filter left":                "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {sequence: 'R>,B', beginSequenceAtStart: false}) YIELD path RETURN path",
		"no relationship filter":                     "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {sequence: 'N'}) YIELD path RETURN path",
		"a map start":                                "CALL apoc.path.subgraphNodes({id: 'a'}, {}) YIELD node RETURN node",
		"a null maxLevel":                            "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {maxLevel: null}) YIELD path RETURN path",
		"a maxLevel that isn't a number":             "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {maxLevel: 'x'}) YIELD path RETURN path",
		"a limit below -1":                           "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {limit: -5}) YIELD path RETURN path",
		"a labelFilter that isn't a string":          "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {labelFilter: 5}) YIELD path RETURN path",
		"a minLevel of '1' where 0 or 1 is required": "MATCH (s0 {id:'a'}) CALL apoc.path.subgraphNodes(s0, {minLevel: '1'}) YIELD node RETURN node",
		"expand's null levels":                       "MATCH (s0 {id:'a'}) CALL apoc.path.expand(s0, null, null, null, null) YIELD path RETURN path",
		"a null boolean is false":                    "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {sequence: 'R>,B', beginSequenceAtStart: null}) YIELD path RETURN path",
		"an element id no node has":                  "CALL apoc.path.subgraphNodes('4:nornic:missing', {}) YIELD node RETURN node",
		"a number in endNodes":                       "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {endNodes: [1]}) YIELD path RETURN path",
		"no label filter":                            "MATCH (s0 {id:'a'}) CALL apoc.path.expandConfig(s0, {sequence: 'R>,', beginSequenceAtStart: false, maxLevel: 1}) YIELD path RETURN path",
		"expand's label filter":                      "MATCH (s0 {id:'a'}) CALL apoc.path.expand(s0, 'R>', '>', 1, 2) YIELD path RETURN path",
		"subgraphAll's sequence":                     "MATCH (s0 {id:'a'}) CALL apoc.path.subgraphAll(s0, {sequence: 'N'}) YIELD nodes RETURN nodes",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := exec.Execute(ctx, call, nil)
			require.Error(t, err)
			requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
		})
	}
}

// apocMissingNodeEngine can't load one node, as a store whose relationship
// outlives its end node.
type apocMissingNodeEngine struct {
	storage.Engine
	missing storage.NodeID
}

func (e *apocMissingNodeEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == e.missing {
		return nil, storage.ErrNotFound
	}
	return e.Engine.GetNode(id)
}

// TestApocPathEdgeCases covers inputs the APOC comparison doesn't reach.
func TestApocPathEdgeCases(t *testing.T) {
	t.Run("a relationship whose end node can't be loaded is skipped", func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
		exec := NewStorageExecutor(store)
		ctx := context.Background()
		_, err := exec.Execute(ctx, apocPathFixture, nil)
		require.NoError(t, err)
		ids, err := exec.Execute(ctx, "MATCH (b {id:'b'}) RETURN id(b)", nil)
		require.NoError(t, err)
		exec = NewStorageExecutor(&apocMissingNodeEngine{Engine: store, missing: storage.NodeID(ids.Rows[0][0].(string))})
		result, err := exec.Execute(ctx, "MATCH (s0 {id:'a'}) CALL apoc.path.subgraphNodes(s0, {relationshipFilter: 'R>'}) YIELD node RETURN collect(node.id) AS ids", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{"a"}}}, result.Rows)
	})

	t.Run("a cancelled query stops", func(t *testing.T) {
		exec, _ := newApocPathFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := &storage.Node{ID: "a"}
		_, err := exec.callApocPathSubgraphNodes(ctx, []interface{}{start, map[string]interface{}{}})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("arguments", func(t *testing.T) {
		exec, _ := newApocPathFixture(t)
		nodes, err := exec.apocNodes("the start node", (*storage.Node)(nil))
		require.NoError(t, err)
		require.Empty(t, nodes)
		_, err = exec.apocExpansionFromArguments([]interface{}{nil, int64(5)}, apocPathSubgraphNodesProcedure)
		require.ErrorContains(t, err, "must be a map")
		// APOC's reading of a boolean setting, checked on Neo4j 5.26.30.
		for value, want := range map[interface{}]bool{
			nil: false, true: true, false: false, "true": true, "yes": true, "No": false, "FALSE": false, "": false, "0": false,
			int64(1): true, int64(0): false, 0.5: false, 1.5: true,
		} {
			require.Equal(t, want, apocBoolean(value), "%#v", value)
		}
		require.True(t, apocBoolean(map[string]interface{}{}))
		labels, err := parseApocLabelFilter("+A||B")
		require.NoError(t, err)
		require.Equal(t, map[string]bool{"A": true, "B": true}, labels.allow)
	})
}
