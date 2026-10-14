package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newShortestSelectorExecutor is the graph of the shortest-selector sweep
// family (#1212): a directed triangle 1->2->3->1 with the chord 1->3, a tail
// 3->4, a cycle 4->5->6->4, a :U shortcut 2->5 and the isolated 7; every
// node :S, every other relationship :T with a weight w.
func newShortestSelectorExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "shortest_selector"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (s1:S {id: 1}), (s2:S {id: 2}), (s3:S {id: 3}), (s4:S {id: 4}), (s5:S {id: 5}), "+
		"(s6:S {id: 6}), (s7:S {id: 7}), (s1)-[:T {w: 1}]->(s2), (s2)-[:T {w: 2}]->(s3), (s3)-[:T {w: 3}]->(s1), "+
		"(s1)-[:T {w: 2}]->(s3), (s3)-[:T {w: 1}]->(s4), (s4)-[:T {w: 2}]->(s5), (s5)-[:T {w: 3}]->(s6), "+
		"(s6)-[:T {w: 1}]->(s4), (s2)-[:U {w: 5}]->(s5)", nil)
	require.NoError(t, err)
	return exec, ctx
}

// requireRows runs each statement and compares its rows.
func requireRows(t *testing.T, exec *StorageExecutor, ctx context.Context, params map[string]interface{}, cases map[string][][]interface{}) {
	t.Helper()
	for query, want := range cases {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
}

// SHORTEST over quantified path patterns returns Neo4j 5.26.30's rows: one
// search from the bound end gives each end node its k shortest paths, the
// pattern's predicates select among the paths, the clause's WHERE filters
// the selected ones (#1212).
func TestSelectedPathSearchMatchesNeo4j(t *testing.T) {
	exec, ctx := newShortestSelectorExecutor(t)
	l := func(values ...interface{}) []interface{} { return values }
	i := func(values ...int64) []interface{} {
		out := make([]interface{}, len(values))
		for index, value := range values {
			out[index] = value
		}
		return out
	}
	requireRows(t, exec, ctx, nil, map[string][][]interface{}{
		// Two groups in a row: each takes one repetition at least.
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->())+(()-[:T]->())+(b) RETURN b.id AS e, length(p) AS l ORDER BY e, l": {
			i(1, 2), i(2, 3), i(3, 2), i(4, 2), i(5, 3), i(6, 4)},
		"MATCH p = ALL SHORTEST (a:S {id: 1})(()-[:T]->())+(()-[:T]->())+(b:S {id: 4}) RETURN [n IN nodes(p) | n.id] AS ns ORDER BY ns": {
			l(i(1, 3, 4))},
		// A variable written twice is one node.
		"MATCH p = SHORTEST 2 (a:S {id: 1})-[:T]->(m)(()-[:T]-())+(m) RETURN m.id AS m, length(p) AS l ORDER BY m, l": {i(3, 4), i(3, 4)},
		"MATCH p = ALL SHORTEST (a:S {id: 1})(()-[:T {w: 2}]-())+(b) RETURN [n IN nodes(p) | n.id] AS ns ORDER BY ns": {
			l(i(1, 3)), l(i(1, 3, 2))},
		// A relationship an earlier clause binds.
		"MATCH ()-[x:T {w: 3}]->(:S {id: 1}) MATCH p = SHORTEST 1 (a:S)-[x]->(b) RETURN a.id AS a, b.id AS b": {i(3, 1)},
		// A null endpoint, a null length bound and no path select nothing.
		"OPTIONAL MATCH (x:S {id: 99}) MATCH p = SHORTEST 1 (x)(()-->())+(b) RETURN count(*) AS c":                   {i(0)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]-())+(b) WHERE length(p) <= null RETURN count(*) AS c":            {i(0)},
		"MATCH p = SHORTEST 1 (a:S {id: 4})(()-[:T]-())+(b:S {id: 7}) RETURN count(*) AS c":                          {i(0)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()<-[:T]-())+(b:S {id: 5}) RETURN count(*) AS c":                         {i(0)},
		"MATCH p = SHORTEST 1 (a:S)(()-[:T]->())+(b:S) RETURN count(*) AS c, sum(length(p)) AS l":                    {i(27, 57)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]-())*(b:S {id: 1}) RETURN length(p) AS l":                         {i(0)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->())+(m:S {id: 5})-[:T]->(b) RETURN b.id AS b, length(p) AS l":   {i(6, 4)},
		"MATCH p = SHORTEST 1 (a:S {id: 2})-[:U]->(m)(()-[:T]-())+(b:S {id: 1}) RETURN [n IN nodes(p) | n.id] AS ns": {l(i(2, 5, 4, 3, 1))},
		"MATCH p = SHORTEST 1 (a:S {id: 1})((x)-[r:T]->(y) WHERE r.w > x.id)+(b) RETURN b.id AS e, length(p) AS l":   {i(3, 1)},
		"MATCH (a:S {id: 1}) MATCH p = SHORTEST 1 (a)(()-[:T]->())+(b) WHERE b.id = a.id + 4 RETURN length(p) AS l":  {i(3)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->())+(b) WHERE a.id + 4 = b.id RETURN length(p) AS l":            {i(3)},
		"MATCH p = SHORTEST 3 (a:S {id: 1})(()-[:T]->()){2,3}(b:S {id: 3}) RETURN length(p) AS l ORDER BY l":         {i(2)},
		"MATCH p = ALL SHORTEST (a:S {id: 6})(()<-[:T]-())+(b:S {id: 2}) RETURN [n IN nodes(p) | n.id] AS ns ORDER BY ns": {
			l(i(6, 5, 4, 3, 2))},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[r:T]->() WHERE r.w IN [1, 2])+(b) RETURN b.id AS e, length(p) AS l ORDER BY e, l": {
			i(2, 1), i(3, 1), i(4, 2), i(5, 3)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->(x) WHERE x.id <> 3)+(b) RETURN b.id AS e, length(p) AS l ORDER BY e, l": {i(2, 1)},
		// Two cycles through 3, one each way round.
		"MATCH p = SHORTEST 2 (a:S {id: 3})(()-[:T]-())+(b:S {id: 3}) RETURN [n IN nodes(p) | n.id] AS ns ORDER BY ns": {
			l(i(3, 1, 3)), l(i(3, 1, 3))},
		"MATCH p = ANY SHORTEST (a:S {id: 7})(()-[:T]-())*(b) RETURN b.id AS b, length(p) AS l": {i(7, 0)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->()){,2}(b) RETURN b.id AS e, length(p) AS l ORDER BY e, l": {
			i(1, 0), i(2, 1), i(3, 1), i(4, 2)},
		"MATCH p = SHORTEST 2 GROUPS (a:S)(()-[:T]-())+(b:S {id: 6}) RETURN a.id AS a, length(p) AS l, count(*) AS c ORDER BY a, l": {
			i(1, 3, 2), i(1, 4, 3), i(2, 3, 1), i(2, 4, 3), i(3, 2, 1), i(3, 3, 1), i(4, 1, 1), i(4, 2, 1), i(5, 1, 1), i(5, 2, 1), i(6, 3, 2)},
	})
}

// The statements of #1212 on its graph (newMentionGraphExecutor) return
// Neo4j 5.26.30's answers, the unbounded ones too.
func TestSelectedPathSearchMentionGraph(t *testing.T) {
	exec, ctx := newMentionGraphExecutor(t)
	i := func(values ...int64) []interface{} {
		out := make([]interface{}, len(values))
		for index, value := range values {
			out[index] = value
		}
		return out
	}
	const start = "MATCH (s:Doc {id: 'd10'}) "
	deadline, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	requireRows(t, exec, deadline, map[string]interface{}{"k": int64(3)}, map[string][][]interface{}{
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t) WHERE length(p) <= 1 RETURN count(*)":                                        {i(4)},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-()){1,2}(t) RETURN count(*)":                                                         {i(8)},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-()){1,4}(t) RETURN count(*)":                                                         {i(162)},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t) WHERE length(p) <= $k RETURN count(*)":                                       {i(113)},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t) RETURN count(*), sum(length(p)), max(length(p))":                             {i(272, 1094, 8)},
		start + "MATCH p = SHORTEST 2 (s)(()-[r:REL]-())+(t) RETURN count(*), sum(length(p)), max(length(p))":                             {i(544, 2438, 8)},
		start + "MATCH p = ALL SHORTEST (s)(()-[r:REL]-())+(t) RETURN count(*), sum(length(p)), max(length(p))":                           {i(720, 3828, 8)},
		start + "MATCH p = SHORTEST 2 GROUPS (s)(()-[r:REL]-())+(t) RETURN count(*), sum(length(p)), max(length(p))":                      {i(21080, 195762, 12)},
		start + "MATCH p = SHORTEST 1 (s)-[:REL]-+(t) RETURN count(*), sum(length(p))":                                                    {i(272, 1094)},
		start + "MATCH p = SHORTEST 2 (s)-[:REL]-+(t) RETURN count(*), sum(length(p))":                                                    {i(544, 2438)},
		start + "MATCH p = ALL SHORTEST (s)-[:REL]-+(t) RETURN count(*), sum(length(p))":                                                  {i(720, 3828)},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(s) RETURN length(p)":                                                            {i(8)},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]->())+(t) RETURN count(*), sum(length(p))":                                            {i(8, 12)},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t:Thing) WHERE t.id STARTS WITH 'o' RETURN count(*), sum(length(p))":            {i(20, 112)},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t {id: 'd11'}) WHERE length(p) > 4 RETURN length(p)":                            {},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-(n))+(t {id: 'd11'}) WHERE none(x IN n WHERE x.id IN ['h0', 'h1']) RETURN length(p)": {},
		start + "MATCH p = SHORTEST 1 ((s)(()-[r:REL]-(n))+(t {id: 'd11'}) WHERE none(x IN n WHERE x.id IN ['h0', 'h1'])) RETURN length(p)": {
			i(12)},
		start + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-(n WHERE NOT n.id IN ['h0', 'h1']))+(t {id: 'd11'}) RETURN length(p)": {i(12)},
	})
	// From a document with one mention, no path returns to it: the search
	// ends once every node it reaches has its paths (Neo4j's answers, read
	// with an upper bound longer than every path selected).
	_, err := exec.Execute(ctx, "MATCH (h:Thing {id: 'h0'}) CREATE (:Doc {id: 'leaf'})-[:REL]->(:Mention {id: 'mleaf'})-[:REL]->(h)", nil)
	require.NoError(t, err)
	const leaf = "MATCH (s:Doc {id: 'leaf'}) "
	requireRows(t, exec, deadline, nil, map[string][][]interface{}{
		leaf + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t) RETURN count(*), sum(length(p))":                      {i(273, 1229)},
		leaf + "MATCH p = SHORTEST 2 (s)(()-[r:REL]-())+(t) RETURN count(*), sum(length(p))":                      {i(545, 3265)},
		leaf + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(s) RETURN count(*)":                                      {i(0)},
		leaf + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-())+(t) WHERE t.id = 'mleaf' RETURN count(*)":                 {i(1)},
		leaf + "MATCH p = SHORTEST 1 (s)(()-[r:REL]-()){1,12}(t) RETURN count(*), sum(length(p)), max(length(p))": {i(273, 1229, 6)},
	})
}

// More selected pattern forms, with Neo4j 5.26.30's rows: a bound inner
// node, list variables read back from a search from the end, constant and
// length predicates, exact and bounded lengths, a relationship property
// map, one node at both ends, no walk or no path between bound ends, ends
// that can't take another path, and dynamic types.
func TestSelectedPathSearchForms(t *testing.T) {
	exec, ctx := newShortestSelectorExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (z:Z {id: 0})-[:T]->(a:Z {id: 1})-[:T]->(b:Z {id: 2})-[:T]->(a)", nil)
	require.NoError(t, err)
	l := func(values ...interface{}) []interface{} { return values }
	i := func(values ...int64) []interface{} {
		out := make([]interface{}, len(values))
		for index, value := range values {
			out[index] = value
		}
		return out
	}
	requireRows(t, exec, ctx, map[string]interface{}{"t": "T"}, map[string][][]interface{}{
		"MATCH (m:S {id: 3}) MATCH p = SHORTEST 1 (a:S {id: 1})-[:T]->(m)-[:T]->+(b:S {id: 5}) RETURN length(p) AS l": {i(3)},
		"MATCH (b:S {id: 5}) MATCH p = SHORTEST 1 (a:S)(()-[r:T]->(n))+(b) RETURN a.id AS a, [x IN n | x.id] AS ns, size(r) AS s ORDER BY a": {
			l(int64(1), i(3, 4, 5), int64(3)), l(int64(2), i(3, 4, 5), int64(3)), l(int64(3), i(4, 5), int64(2)),
			l(int64(4), i(5), int64(1)), l(int64(5), i(6, 4, 5), int64(3)), l(int64(6), i(4, 5), int64(2))},
		"MATCH p = SHORTEST 1 ((a:S {id: 1})(()-[:T]->())+(b) WHERE 1 = 2) RETURN count(*) AS c":       {i(0)},
		"MATCH p = SHORTEST 1 ((a:S {id: 1})(()-[:T]->())+(b) WHERE 1 = 1) RETURN count(*) AS c":       {i(6)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->())+(b) WHERE length(p) < 0 RETURN count(*) AS c": {i(0)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->())+(b) WHERE 3 > length(p) RETURN b.id AS e, length(p) AS l ORDER BY e, l": {
			i(1, 2), i(2, 1), i(3, 1), i(4, 2)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->())+(b) WHERE length(p) < 2.5 RETURN b.id AS e, length(p) AS l ORDER BY e, l": {
			i(1, 2), i(2, 1), i(3, 1), i(4, 2)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->())+(b) WHERE length(p) <= 2.5 RETURN b.id AS e, length(p) AS l ORDER BY e, l": {
			i(1, 2), i(2, 1), i(3, 1), i(4, 2)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})-[:T*2]->(b) RETURN b.id AS e, length(p) AS l ORDER BY e, l":                 {i(1, 2), i(3, 2), i(4, 2)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})-[:T {w: 2}]->(m)(()-[:T]->())+(b:S {id: 4}) RETURN length(p) AS l":          {i(2)},
		"MATCH p = SHORTEST 1 (a)(()-[:T]-())+(a:S {id: 3}) RETURN length(p) AS l":                                      {i(2)},
		"MATCH p = ANY SHORTEST (a:S {id: 1})(()-[:T]->(x WHERE x.id <> 3))+(b:S {id: 4}) RETURN count(*) AS c":         {i(0)},
		"MATCH (a:S {id: 4}), (b:S {id: 7}) MATCH p = ANY SHORTEST (a)(()-[:T]-())+(b) RETURN count(*) AS c":            {i(0)},
		"MATCH p = ANY SHORTEST (a:S {id: 2})(()-[:U]-()){3}(b:S {id: 5}) RETURN count(*) AS c":                         {i(0)},
		"MATCH p = ANY SHORTEST (a:S {id: 1})(()-[:T]->(x WHERE x.id <> 4))+(b:S {id: 5}) RETURN count(*) AS c":         {i(0)},
		"MATCH p = SHORTEST 3 (s:Z {id: 0})(()-[:T]->())+(t) RETURN t.id AS t, length(p) AS l ORDER BY t, l":            {i(1, 1), i(1, 3), i(2, 2)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]-())+(b:S WHERE b.id < 7) RETURN count(*) AS c":                      {i(6)},
		"MATCH (x:S {id: 1}) MATCH p = SHORTEST 1 (x:Nope)(()-->())+(b) RETURN count(*) AS c":                           {i(0)},
		"MATCH (a:S {id: 6}), (b:S {id: 2}) MATCH p = SHORTEST 2 (a)(()<-[:T]-())+(b) RETURN length(p) AS l ORDER BY l": {i(4), i(6)},
		"MATCH (a:S {id: 1}), (b:S {id: 6}) MATCH p = SHORTEST 2 GROUPS (a)(()-[:T]-())+(b) RETURN length(p) AS l, count(*) AS c ORDER BY l": {
			i(3, 2), i(4, 3)},
		"WITH $t AS t MATCH p = SHORTEST 1 (a:S {id: 1})-[:$(t)]->+(b) RETURN count(*) AS c":                  {i(6)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[r:T]->() WHERE r.w > 1)+(b) RETURN count(*) AS c":             {i(2)},
		"MATCH p = SHORTEST 1 (a:S {id: 1})-[:T|U]->(:S&!X)(()-[:T]->())+(b:S {id: 6}) RETURN length(p) AS l": {i(4)},
	})
	// MATCH REPEATABLE ELEMENTS (Cypher 25, read in both versions) selects
	// walks: one relationship may repeat.
	requireRows(t, exec, ctx, nil, map[string][][]interface{}{
		"MATCH REPEATABLE ELEMENTS p = ANY SHORTEST (a:S {id: 2})(()-[:U]-()){3}(b:S {id: 5}) RETURN length(p) AS l": {i(3)},
		"MATCH REPEATABLE ELEMENTS p = SHORTEST 5 (a:S {id: 7})(()-[:T]-()){0,2}(b) RETURN count(*) AS c":            {i(1)},
	})
}

// A failed relationship read is the statement's error; a relationship to a
// node that doesn't load is skipped, as the traversal does.
func TestSelectedPathSearchStorage(t *testing.T) {
	ctx := context.Background()
	base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "selected_failing")
	exec := NewStorageExecutor(base)
	_, err := exec.Execute(ctx, "CREATE (:S {id: 1})-[:T]->(:S {id: 2})-[:T]->(:S {id: 3})", nil)
	require.NoError(t, err)
	failing := NewStorageExecutor(&failingEdgeEngine{Engine: base, failOutgoing: true, failIncoming: true})
	for _, query := range []string{
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]-())+(b) RETURN count(*) AS c",
		"MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->())+(b) RETURN count(*) AS c",
		"MATCH (a:S {id: 1}), (b:S {id: 3}) MATCH p = shortestPath((a)-[:T*]-(b)) RETURN count(*) AS c",
		"MATCH (a:S {id: 1}), (b:S {id: 3}) MATCH p = shortestPath((a)-[:T*]->(b)) RETURN count(*) AS c",
	} {
		_, err = failing.Execute(ctx, query, nil)
		require.ErrorIs(t, err, errEdgeRead, query)
	}

	inner := &danglingTraversalEngine{MemoryEngine: newTestMemoryEngine(t), forcedOutgoing: map[storage.NodeID][]*storage.Edge{}}
	exec = NewStorageExecutor(storage.NewNamespacedEngine(inner, "selected_dangling"))
	_, err = exec.Execute(ctx, "CREATE (:S {id: 1})-[:T]->(:S {id: 2})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:S {id: 1}) RETURN a", nil)
	require.NoError(t, err)
	first := "selected_dangling:" + result.Rows[0][0].(*storage.Node).ID
	edges, err := inner.MemoryEngine.GetOutgoingEdges(first)
	require.NoError(t, err)
	inner.forcedOutgoing[first] = append(edges, &storage.Edge{ID: "selected_dangling:dangling", Type: "T", StartNode: first, EndNode: "selected_dangling:missing"})
	result, err = exec.Execute(ctx, "MATCH p = SHORTEST 1 (a:S {id: 1})(()-[:T]->())+(b) RETURN b.id AS b", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}

// The search stops when the statement is cancelled.
func TestSelectedPathSearchCancelled(t *testing.T) {
	exec, ctx := newShortestSelectorExecutor(t)
	plan, _, ok := compileSelectedPathPlan("(a)(()-[:T]-())+(b)", "p", "", nil)
	require.True(t, ok)
	result, err := exec.Execute(ctx, "MATCH (a:S {id: 1}) RETURN a", nil)
	require.NoError(t, err)
	source := result.Rows[0][0].(*storage.Node)
	cancelled, cancel := context.WithCancel(ctx)
	search := exec.newPathSearch(cancelled, plan.forward, pipelineRow{}, -1, source)
	search.begin(source)
	require.True(t, search.nextLevel())
	require.True(t, search.nextLevel())
	cancel()
	products, levels := len(search.products), len(search.levels)
	calls := 0
	keepGoing, found, _ := search.trails(2, 0, map[storage.EdgeID]struct{}{}, nil, func([]pathSearchHop) bool { calls++; return true })
	require.False(t, keepGoing)
	require.False(t, found)
	require.False(t, search.nextLevel(), "a cancelled search adds no level")
	require.Zero(t, calls)
	require.Len(t, search.products, products)
	require.Len(t, search.levels, levels)
}

// A length bound in the clause's WHERE (length(p) <= $depth) ends the
// search from a bound start at that length, and a search without one ends
// once every node it reaches has its paths: on a 150-step chain and a
// 40 × 40 grid each statement returns Neo4j 5.26.30's rows (#1212). The
// grid has more paths than an enumeration of them finishes: before the
// search, each grid statement took more than 30 s (Neo4j doesn't finish the
// unbounded ones, whose rows are every node but the corner at x + y steps,
// and the corner itself by a cycle of 4); the search takes milliseconds, so
// the budget holds on a loaded host. The bound applies while searching: the
// search to length 6 reaches only the nodes 6 steps from the corner. In a
// MATCH REPEATABLE ELEMENTS (Cypher 25, which Neo4j 5.26 rejects), a walk on
// the directed chain can't repeat a relationship either, so each end has
// the one path it has without it.
func TestSelectedPathSearchLengthBound(t *testing.T) {
	chain, grid := newChainExecutor(t, 150), newGridExecutor(t, 40)
	ctx := context.Background()
	i := func(values ...int64) []interface{} {
		out := make([]interface{}, len(values))
		for index, value := range values {
			out[index] = value
		}
		return out
	}
	const budget = 30 * time.Second
	run := func(exec *StorageExecutor, depth int64, cases map[string][][]interface{}) {
		t.Helper()
		for query, want := range cases {
			deadline, cancel := context.WithTimeout(ctx, budget)
			result, err := exec.Execute(deadline, query, map[string]interface{}{"depth": depth})
			cancel()
			require.NoError(t, err, "%s (depth %d) within %v", query, depth, budget)
			require.Equal(t, want, result.Rows, query)
		}
	}
	const returns = " RETURN count(*) AS c, max(length(p)) AS l"
	run(chain, 150, map[string][][]interface{}{
		"MATCH (s:C {i: 0}) MATCH p = SHORTEST 1 (s)(()-[:NEXT]->())+(t) WHERE length(p) <= $depth" + returns:       {i(150, 150)},
		"MATCH (s:C {i: 0}) MATCH p = SHORTEST 1 (s)(()-[:NEXT]-())+(t) WHERE length(p) <= $depth" + returns:        {i(150, 150)},
		"MATCH (s:C {i: 75}) MATCH p = SHORTEST 2 (s)(()-[:NEXT]-())+(t) WHERE length(p) <= $depth" + returns:       {i(150, 75)},
		"MATCH (s:C {i: 75}) MATCH p = ALL SHORTEST (s)(()-[:NEXT]-())+(t) WHERE length(p) <= $depth" + returns:     {i(150, 75)},
		"MATCH (s:C {i: 0}) MATCH p = SHORTEST 1 (s)-[:NEXT]-+(t) WHERE length(p) <= $depth" + returns:              {i(150, 150)},
		"MATCH (s:C {i: 0}) MATCH p = SHORTEST 3 GROUPS (s)(()-[:NEXT]-())+(t) WHERE length(p) <= $depth" + returns: {i(150, 150)},
		"MATCH (s:C {i: 0}), (t:C {i: 150}) MATCH p = SHORTEST 1 (s)(()-[:NEXT]-())+(t)" + returns:                  {i(1, 150)},
	})
	// The one end has one walk: the search ends when every node is expanded.
	run(chain, 150, map[string][][]interface{}{
		"MATCH (s:C {i: 148}) MATCH REPEATABLE ELEMENTS p = SHORTEST 2 (s)-[:NEXT]->()-[:NEXT]->(t)" + returns: {i(1, 2)},
	})
	run(chain, 3, map[string][][]interface{}{
		"MATCH (s:C {i: 75}) MATCH p = SHORTEST 2 (s)(()-[:NEXT]-())+(t) WHERE length(p) <= $depth" + returns:       {i(6, 3)},
		"MATCH (s:C {i: 0}) MATCH p = SHORTEST 3 GROUPS (s)(()-[:NEXT]-())+(t) WHERE length(p) <= $depth" + returns: {i(3, 3)},
	})
	run(grid, 6, map[string][][]interface{}{
		"MATCH (s:G {x: 0, y: 0}) MATCH p = SHORTEST 1 (s)(()-[:E]-())+(t) WHERE length(p) <= $depth" + returns:   {i(28, 6)},
		"MATCH (s:G {x: 20, y: 20}) MATCH p = SHORTEST 2 (s)(()-[:E]-())+(t) WHERE length(p) <= $depth" + returns: {i(162, 6)},
		"MATCH (s:G {x: 0, y: 0}) MATCH p = SHORTEST 1 (s)(()-[:E]->())+(t) WHERE length(p) <= $depth" + returns:  {i(27, 6)},
		"MATCH (s:G {x: 0, y: 0}) MATCH p = SHORTEST 1 (s)(()-[:E]->())+(t)" + returns:                            {i(1599, 78)},
		"MATCH (s:G {x: 0, y: 0}) MATCH p = SHORTEST 1 (s)(()-[:E]-())+(t)" + returns:                             {i(1600, 78)},
	})

	plan, _, ok := compileSelectedPathPlan("(s)(()-[:E]-())+(t)", "p", "", nil)
	require.True(t, ok)
	corner, err := grid.pipelineMatchRows(ctx, pipelineRow{}, "MATCH (s:G {x: 0, y: 0})")
	require.NoError(t, err)
	source := corner[0]["s"].(*storage.Node)
	search := grid.newPathSearch(ctx, plan.forward, pipelineRow{}, 6, source)
	rows := search.run(source, 1, false, func(path pathSearchPath) (pipelineRow, bool) {
		return grid.bindSelectedPath(ctx, plan, plan.forward, false, pipelineRow{}, path)
	})
	require.Len(t, rows, 28)
	reached := map[storage.NodeID]bool{}
	for _, product := range search.products {
		reached[product.node.ID] = true
	}
	require.Len(t, reached, 28, "the nodes at most 6 steps from the corner")
}

// shortestPath() and allShortestPaths(), as a MATCH pattern and as a value,
// run the selected path search between each pair of endpoints, as SHORTEST
// does: in an auto-commit statement and in an explicit transaction, over
// relationships created earlier in the same statement too, with the WHERE
// conjuncts that read the path constraining the paths selected from. Neo4j
// 5.26.30's rows (#1212).
func TestSelectedPathSearchShortestPathRoutes(t *testing.T) {
	l := func(values ...interface{}) []interface{} { return values }
	i := func(values ...int64) []interface{} {
		out := make([]interface{}, len(values))
		for index, value := range values {
			out[index] = value
		}
		return out
	}
	const chain = "CREATE (a:S {id: 1})-[:T]->(b:S {id: 2})-[:T]->(c:S {id: 3}) WITH a, c "
	sameStatement := map[string][][]interface{}{
		chain + "MATCH p = shortestPath((a)-[:T*]->(c)) RETURN length(p) AS l":      {i(2)},
		chain + "MATCH p = allShortestPaths((a)-[:T*]-(c)) RETURN count(*) AS c":    {i(1)},
		chain + "RETURN length(shortestPath((a)-[:T*]->(c))) AS l":                  {i(2)},
		chain + "MATCH p = ANY SHORTEST (a)(()-[:T]->())+(c) RETURN length(p) AS l": {i(2)},
	}
	// 1->2->3 and 1->4->5->3 over :T, and 1->3 over :U.
	const graph = "CREATE (s1:S {id: 1})-[:T]->(s2:S {id: 2})-[:T]->(s3:S {id: 3}), (s1)-[:T]->(s4:S {id: 4})-[:T]->(s5:S {id: 5})-[:T]->(s3), (s1)-[:U]->(s3)"
	const ends = "MATCH (a:S {id: 1}), (c:S {id: 3}) "
	later := map[string][][]interface{}{
		ends + "MATCH p = shortestPath((a)-[*]->(c)) RETURN length(p) AS l":                                                          {i(1)},
		ends + "MATCH p = shortestPath((a)-[:T*]->(c)) RETURN length(p) AS l":                                                        {i(2)},
		ends + "MATCH p = shortestPath((a)-[:T*]->(c)) WHERE none(n IN nodes(p) WHERE n.id = 2) RETURN [n IN nodes(p) | n.id] AS ns": {l(i(1, 4, 5, 3))},
		ends + "MATCH p = shortestPath((a)-[:T*]->(c)) WHERE length(p) <= 1 RETURN count(*) AS c":                                    {i(0)},
		ends + "MATCH p = shortestPath((a)-[:T*]->(c)) WHERE length(p) < 0 RETURN count(*) AS c":                                     {i(0)},
		ends + "MATCH p = allShortestPaths((a)-[:T*]->(c)) WHERE length(p) >= 3 RETURN [n IN nodes(p) | n.id] AS ns":                 {l(i(1, 4, 5, 3))},
		ends + "MATCH p = allShortestPaths((a)-[r:T*]-(c)) WHERE all(x IN r WHERE startNode(x).id < 5) RETURN [n IN nodes(p) | n.id] AS ns": {
			l(i(1, 2, 3))},
		ends + "RETURN [x IN allShortestPaths((a)-[:T*]->(c)) | length(x)] AS ls":               {l(i(2))},
		ends + "MATCH p = shortestPath((a)-[:T*0..]->(c)) RETURN length(p) AS l":                {i(2)},
		"MATCH p = ANY SHORTEST (a:S {id: 1})(()-[:T]->())+(c:S {id: 3}) RETURN length(p) AS l": {i(2)},
		"MATCH (a:S {id: 1}) MATCH p = shortestPath((a)-[:T*]->(c:S)) WHERE c.id <> 1 RETURN c.id AS c, length(p) AS l ORDER BY c": {
			i(2, 1), i(3, 2), i(4, 1), i(5, 2)},
	}
	run := func(exec *StorageExecutor, ctx context.Context, query string, want [][]interface{}) {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	for _, explicit := range []bool{false, true} {
		ctx := context.Background()
		inTransaction := func(exec *StorageExecutor, body func()) {
			t.Helper()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			body()
			if explicit {
				_, err := exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}
		}
		for query, want := range sameStatement {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "routes"))
			inTransaction(exec, func() { run(exec, ctx, query, want) })
		}
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "routes"))
		inTransaction(exec, func() {
			_, err := exec.Execute(ctx, graph, nil)
			require.NoError(t, err)
			for query, want := range later {
				run(exec, ctx, query, want)
			}
		})
	}
	// A pair whose start is its end fails the statement, as in Neo4j.
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "routes"))
	_, err := exec.Execute(context.Background(), graph, nil)
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), "MATCH (a:S {id: 1}) MATCH p = shortestPath((a)-[:T*]->(c:S)) RETURN c.id AS c", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), shortestPathCommonEndNodesPrefix)
}

// A shortestPath pattern the selected path search can't read (here a
// dynamic type the statement rewrite would have resolved) is an invalid
// pattern, as a MATCH pattern and as a value; the statement rewrite rejects
// a chained one in a value first, so the value evaluator leaves it to the
// ordinary evaluator.
func TestSelectedPathSearchShortestPathUnreadable(t *testing.T) {
	exec, ctx := newShortestSelectorExecutor(t)
	_, ok, err := exec.parseShortestPathMatch(ctx, "p = shortestPath((a)-[:$(t)*]->(b))")
	require.True(t, ok)
	require.Error(t, err)
	_, handled, err := exec.evaluateShortestPathValue(ctx, "shortestPath", "(a)-[:$(t)*]->(b)", nil)
	require.True(t, handled)
	require.Error(t, err)
	_, handled, err = exec.evaluateShortestPathValue(ctx, "shortestPath", "(a)-->(b)-->(c)", nil)
	require.False(t, handled)
	require.NoError(t, err)
}

// The selected pattern reader and its helpers read every form they may get.
func TestSelectedPathSearchHelpers(t *testing.T) {
	for _, pattern := range []string{"", "(a", "(a)-->", "-->(a)", "(a)-->-->(b)", "(a) x", "(a)(b)", "(a)-[r->(b)",
		"(a)((x)-->(y))(b)", "(a)(((x)-->(y))+)+(b)", "(a)((x)-->)+(b)", "(a $p)", "(a)-[:``]->(b)", "(a)-[:$(t)]->(b)",
		"(a)-[r $p]->(b)", "(a:)", "(a)--[r]->(b)", "(a)-[r]--(b)"} {
		_, ok := parsePathPatternItems(pattern, false)
		require.False(t, ok, pattern)
	}
	items, ok := parsePathPatternItems("(a:L WHERE a.x = 1)-[r:T WHERE r.w > 1]->(b)-[:T*2]->(c)-[*..3]-(d)<-[:U*2..]-(e)", false)
	require.True(t, ok)
	require.Equal(t, []string{"a.x = 1"}, items[0].predicates)
	require.Equal(t, "(a:L )", items[0].text)
	require.Equal(t, []string{"r.w > 1"}, items[1].predicates)
	require.Equal(t, [2]int{2, 2}, [2]int{items[3].min, items[3].max})
	require.Equal(t, [2]int{1, 3}, [2]int{items[5].min, items[5].max})
	require.Equal(t, "incoming", items[7].direction)
	require.Equal(t, [2]int{2, -1}, [2]int{items[7].min, items[7].max})

	require.Equal(t, 7, atoiOr("x", 7))
	require.Equal(t, 7, atoiOr("99999999999", 7))
	require.Equal(t, 12, atoiOr("12", 7))
	require.Nil(t, propertyMapPredicates("v", ""))
	require.Empty(t, propertyMapPredicates("v", "{x}"))
	require.Equal(t, []string{"v.`a b` = (1)", "v.`c` = ('x')"}, propertyMapPredicates("v", "{`a b`: 1, c: 'x'}"))

	for term, want := range map[string]bool{"a:L": true, "{a: 1}": false, "n.a": false, "$a": false, "'a'": false,
		"[a IN l]": true, "f(a)": true, "ab": false, "{k: a}": true, "(a)": true} {
		require.Equal(t, want, readsPatternVariable(term, "a"), term)
	}
	require.False(t, readsPatternVariable("x", ""))

	for term, want := range map[string]pathLengthBound{
		"length(p) <= 3": {bound: "3"}, "length(p) < $k": {bound: "$k", strict: true}, "3 >= length(p)": {bound: "3"},
		"3 > length(p)": {bound: "3", strict: true}, "size('<=') >= length(p)": {bound: "size('<=')"},
		"length(p) <= [1][0]": {bound: "[1][0]"}, "length(p) <= size([x IN [1] WHERE x < 2])": {bound: "size([x IN [1] WHERE x < 2])"},
	} {
		bound, ok := pathLengthBoundOf(term, "p")
		require.True(t, ok, term)
		require.Equal(t, want, bound, term)
	}
	for _, term := range []string{"length(p) >= 3", "length(q) <= 3", "length(p) <= length(p)", "length(p) <= 2 <= 3",
		"x <> length(p)", "length(p) = 2"} {
		_, ok := pathLengthBoundOf(term, "p")
		require.False(t, ok, term)
	}
	_, ok = pathLengthBoundOf("length(p) <= 3", "")
	require.False(t, ok)

	for _, check := range []struct {
		value  interface{}
		strict bool
		limit  int
		known  bool
	}{{int64(3), false, 3, true}, {3, true, 2, true}, {2.5, false, 2, true}, {2.5, true, 2, true}, {"x", false, 0, false},
		{nil, false, 0, false}, {float64(1 << 40), false, -1, false}} {
		limit, known := pathLengthLimit(check.value, check.strict)
		require.Equal(t, check.known, known, check.value)
		require.Equal(t, check.limit, limit, check.value)
	}
	nan := 0.0
	_, known := pathLengthLimit(nan/nan, false)
	require.False(t, known)

	a, b := &storage.Node{ID: "a"}, &storage.Node{ID: "b"}
	x, y := &storage.Edge{ID: "x"}, &storage.Edge{ID: "y"}
	require.True(t, sameSelectedEntity(a, &storage.Node{ID: "a"}))
	require.False(t, sameSelectedEntity(a, b))
	require.True(t, sameSelectedEntity(x, &storage.Edge{ID: "x"}))
	require.False(t, sameSelectedEntity(x, y))
	require.False(t, sameSelectedEntity(a, x))
	require.False(t, sameSelectedEntity(int64(1), int64(1)))
	values := []int{1, 2, 3}
	reverseSlice(values)
	require.Equal(t, []int{3, 2, 1}, values)

	for pattern, want := range map[string]int{"(a)(()-->()){2,3}(b)-->(c)": 4, "(a)-[*1..2]->(b)": 2, "(a)-[*1..]->(b)": -1,
		"(a)(()-->()-->())+(b)": -1, "(a)": 0} {
		plan, _, ok := compileSelectedPathPlan(pattern, "p", "", nil)
		require.True(t, ok, pattern)
		require.Equal(t, want, plan.forward.maxLength, pattern)
	}
	plan, filters, ok := compileSelectedPathPlan("(a)-[*2..16777216]->(b)", "p", "", []string{"", "length(p) <= 2"})
	require.True(t, ok)
	require.Equal(t, []string{"length(p) <= 2"}, filters)
	require.Len(t, plan.lengthBounds, 1)
	_, _, ok = compileSelectedPathPlan("(a", "p", "", nil)
	require.False(t, ok)
}

// A failed match of an end's candidates is the step's error: the start's
// when it is the narrower end, the end's otherwise.
func TestSelectedPathSearchEndpointError(t *testing.T) {
	ctx := context.Background()
	base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "selected_endpoints")
	_, err := NewStorageExecutor(base).Execute(ctx, "CREATE (:S {id: 1})-[:T]->(:S {id: 5})", nil)
	require.NoError(t, err)
	failing := NewStorageExecutor(&failingNodeLookupEngine{Engine: base, byLabelErr: errEdgeRead, allNodesErr: errEdgeRead})
	for _, query := range []string{
		"MATCH p = SHORTEST 1 (a:S)(()-[:T]->())+(b:S) RETURN count(*) AS c",
		"MATCH p = SHORTEST 1 (a)(()-[:T]->())+(b:S) RETURN count(*) AS c",
	} {
		_, err = failing.Execute(ctx, query, nil)
		require.ErrorIs(t, err, errEdgeRead, query)
	}
}
