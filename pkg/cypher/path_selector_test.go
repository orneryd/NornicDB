package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newPathSelectorExecutor is a graph with cycles and a parallel pair:
// (1)->(2)->(3)->(1), (3)->(4)->(5), (1)->(3), every node :SP and every
// relationship :T.
func newPathSelectorExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "path_selector"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:SP {id: 1})-[:T]->(b:SP {id: 2})-[:T]->(c:SP {id: 3})-[:T]->(a), (c)-[:T]->(d:SP {id: 4})-[:T]->(e:SP {id: 5}), (a)-[:T]->(c)", nil)
	require.NoError(t, err)
	return exec, ctx
}

// Path selectors and path modes return Neo4j 2026.09's rows (#907).
func TestPathSelectors(t *testing.T) {
	exec, ctx := newPathSelectorExecutor(t)
	l := func(values ...interface{}) []interface{} { return values }
	i := func(value int64) []interface{} { return l(value) }
	for query, want := range map[string][][]interface{}{
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                                                      {i(3)},
		"MATCH p = ALL SHORTEST (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN length(p) AS l":                                                      {i(1)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN length(p) AS l":                                                        {i(1)},
		"MATCH p = SHORTEST 3 (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN length(p) AS l ORDER BY l":                                             {i(1), i(2), i(4)},
		"MATCH p = SHORTEST 2 GROUPS (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN length(p) AS l ORDER BY l":                                      {i(1), i(2)},
		"MATCH p = SHORTEST GROUP (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN length(p) AS l":                                                    {i(1)},
		"MATCH p = SHORTEST 2 PATH GROUPS (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN length(p) AS l ORDER BY l":                                 {i(1), i(2)},
		"MATCH p = ANY (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN count(p) AS c":                                                                {i(1)},
		"MATCH p = ANY 2 PATHS (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN count(p) AS c":                                                        {i(2)},
		"MATCH p = ALL PATHS (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN count(p) AS c":                                                          {i(4)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->+(b:SP) RETURN b.id AS b, length(p) AS l ORDER BY b":                                          {l(int64(1), int64(2)), l(int64(2), int64(1)), l(int64(3), int64(1)), l(int64(4), int64(2)), l(int64(5), int64(3))},
		"MATCH p = SHORTEST 2 (a:SP {id: 1})-->+(b) RETURN count(p) AS c":                                                                    {i(10)},
		"MATCH p = SHORTEST $k (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN length(p) AS l ORDER BY l":                                            {i(1), i(2)},
		"MATCH p = SHORTEST 2 (a:SP {id: 1})-->+(b:SP {id: 1}) RETURN length(p) AS l ORDER BY l":                                             {i(2), i(3)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-->*(b:SP {id: 1}) RETURN length(p) AS l":                                                      {i(0)},
		"MATCH SHORTEST 1 (a:SP {id: 1})-->+(b:SP {id: 4}) RETURN b.id AS b":                                                                 {i(4)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-[r:T]->+(b:SP {id: 4}) RETURN size(r) AS s":                                                     {i(2)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->+(b:SP {id: 4}) WHERE length(p) > 2 RETURN length(p) AS l":                                    {},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-[r]->+(b:SP {id: 5}) WHERE size(r) > 3 RETURN length(p) AS l":                                   {},
		"MATCH p = SHORTEST 1 (a:SP)-->+(b:SP {id: 5}) WHERE a.id = 1 RETURN length(p) AS l":                                                 {i(3)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-->+(b:SP {id: 5}) WHERE a.id = 2 RETURN length(p) AS l":                                       {},
		"MATCH p = ACYCLIC (a:SP {id: 1})-->+(b) RETURN count(p) AS c":                                                                       {i(7)},
		"MATCH p = TRAIL (a:SP {id: 1})-->+(b) RETURN count(p) AS c":                                                                         {i(16)},
		"MATCH p = WALK (a:SP {id: 1})-->{1,4}(b) RETURN count(p) AS c":                                                                      {i(12)},
		"MATCH p = SHORTEST 2 WALK (a:SP {id: 1})-->+(b:SP {id: 1}) RETURN length(p) AS l ORDER BY l":                                        {i(2), i(3)},
		"MATCH p = ACYCLIC (a:SP {id: 1})-->{0,2}(b) RETURN count(p) AS c":                                                                   {i(5)},
		"MATCH p = ACYCLIC (a:SP {id: 1})-->+(b:SP {id: 1}) RETURN count(p) AS c":                                                            {i(0)},
		"MATCH p = ACYCLIC (a:SP {id: 1})-->*(b:SP {id: 1}) RETURN count(p) AS c":                                                            {i(1)},
		"MATCH ACYCLIC (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN count(*) AS c":                                                                {i(2)},
		"MATCH p = SHORTEST 2 ACYCLIC (a:SP {id: 1})-->+(b:SP {id: 4}) RETURN length(p) AS l ORDER BY l":                                     {i(2), i(3)},
		"MATCH p = ANY ACYCLIC PATH (a:SP {id: 1})-->+(b:SP {id: 4}) RETURN length(p) AS l":                                                  {i(2)},
		"MATCH p = ALL SHORTEST WALK PATHS (a:SP {id: 1})-->+(b:SP {id: 4}) RETURN count(p) AS l":                                            {i(1)},
		"MATCH p = ACYCLIC PATHS (a:SP {id: 1})-->+(b:SP {id: 4}) RETURN count(p) AS l":                                                      {i(2)},
		"MATCH p = ALL SHORTEST (a:SP {id: 1})--+(b:SP {id: 4}) RETURN length(p) AS l":                                                       {i(2), i(2)},
		"MATCH p = SHORTEST 2 (a:SP {id: 1})--+(b:SP {id: 5}) RETURN length(p) AS l ORDER BY l":                                              {i(3), i(3)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})<--+(b:SP {id: 5}) RETURN length(p) AS l":                                                        {},
		"MATCH p = SHORTEST 2 (a:SP {id: 1})-[:T*]->(b:SP {id: 5}) RETURN length(p) AS l ORDER BY l":                                         {i(3), i(4)},
		"MATCH p = SHORTEST 2 (a:SP {id: 1})-[:T]->{1,2}(b:SP {id: 3}) RETURN length(p) AS l ORDER BY l":                                     {i(1), i(2)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-->{4,5}(b:SP {id: 5}) RETURN length(p) AS l":                                                  {i(4)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-[*2..]->(b:SP {id: 3}) RETURN length(p) AS l":                                                 {i(2)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-[*0..]->(b:SP {id: 1}) RETURN length(p) AS l":                                                 {i(0)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-[:NOPE]->+(b:SP {id: 4}) RETURN length(p) AS l":                                               {},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-[r:T WHERE r.w IS NULL]->+(b:SP {id: 5}) RETURN length(p) AS l":                               {i(3)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-[r:T {w: 1}]->+(b:SP {id: 5}) RETURN length(p) AS l":                                          {},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN [n IN nodes(p) | n.id] AS ns":                                          {l(l(int64(1), int64(3), int64(4), int64(5)))},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-[r]->+(b:SP {id: 5}) RETURN [x IN r | type(x)] AS l":                                            {l(l("T", "T", "T"))},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->+(b:SP {id: 4}) RETURN a.id AS a, b.id AS b, p IS NULL AS n":                                  {l(int64(1), int64(4), false)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-->(b) RETURN count(p) AS c":                                                                   {i(2)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->(m)-->+(b:SP {id: 5}) RETURN length(p) AS l":                                                  {i(3)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->(m:SP {id: 2})-->+(b:SP {id: 5}) RETURN length(p) AS l":                                       {i(4)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->(m)-->+(b:SP {id: 5}) WHERE m.id = 2 RETURN length(p) AS l":                                   {},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->(m WHERE m.id = 2)-->+(b:SP {id: 5}) RETURN length(p) AS l":                                   {i(4)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->+(b:SP {id: 4})-->(c) RETURN length(p) AS l, c.id AS c":                                       {l(int64(3), int64(5))},
		"MATCH p = ANY SHORTEST (a:SP {id: 1}) RETURN length(p) AS l":                                                                        {i(0)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-->(b)-->(c) RETURN count(p) AS l":                                                             {i(3)},
		"MATCH p = ACYCLIC (a:SP {id: 1})-->(b)-->(c) RETURN count(p) AS l":                                                                  {i(2)},
		"MATCH p = SHORTEST 2 ACYCLIC (a:SP {id: 1})-->(b)-->(c) RETURN count(p) AS l":                                                       {i(2)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})-->+(b:SP {id: 4}) MATCH q = SHORTEST 1 (b)-->+(c:SP {id: 5}) RETURN length(p) + length(q) AS l": {i(3)},
		"MATCH (a:SP {id: 1}), (b:SP {id: 5}) MATCH p = ANY SHORTEST (a)-->+(b) RETURN length(p) AS l":                                       {i(3)},
		"OPTIONAL MATCH p = SHORTEST 1 (a:SP {id: 5})-->+(b:SP {id: 1}) RETURN p":                                                            {l(nil)},
		"MATCH (x:SP {id: 5}) OPTIONAL MATCH p = SHORTEST 1 (x)-->+(b:SP {id: 1}) RETURN x.id AS x, p IS NULL AS n":                          {l(int64(5), true)},
		"MATCH (x:SP {id: 1}) OPTIONAL MATCH p = SHORTEST 1 (x)-->+(b:SP {id: 4}) WHERE length(p) > 5 RETURN b":                              {l(nil)},
		"MATCH (x:SP {id: 1}) OPTIONAL MATCH p = SHORTEST 1 (x)-->(m)-->+(b:SP {id: 4}) WHERE m.id = 7 RETURN m":                             {l(nil)},
		"MATCH p = ANY SHORTEST ((a:SP {id: 1})-->+(b:SP {id: 5})) RETURN length(p) AS l":                                                    {i(3)},
		"MATCH p = ANY SHORTEST ((a:SP {id: 1})-->+(b:SP {id: 5}) WHERE a.id = 1) RETURN length(p) AS l":                                     {i(3)},
		"MATCH p = ANY SHORTEST ((a:SP {id: 1})-[r]->+(b:SP {id: 5}) WHERE size(r) > 3) RETURN length(p) AS l":                               {i(4)},
		"MATCH p = any shortest (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                                                      {i(3)},
		"MATCH p = ANY SHORTEST (a:SP|X {id: 1})-->+(b:SP&!X {id: 5}) RETURN length(p) AS l":                                                 {i(3)},
		"MATCH p = ANY SHORTEST (a:X|Y {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                                                     {},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-[:T|U]->+(b:SP {id: 5}) RETURN length(p) AS l":                                                {i(3)},
		"MATCH p = ANY SHORTEST (a:SP {id: 1})-[:!T]->+(b:SP {id: 5}) RETURN length(p) AS l":                                                 {},
		"MATCH DIFFERENT RELATIONSHIPS (a:SP {id: 1})-->(b) RETURN count(*) AS l":                                                            {i(2)},
		"MATCH DIFFERENT RELATIONSHIP p = ANY SHORTEST (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                               {i(3)},
		"MATCH p = ALL (a:SP {id: 1})-->+(b:SP {id: 5}), (c:SP {id: 2}) RETURN count(*) AS l":                                                {i(4)},
		"MATCH p = ACYCLIC (a:SP {id: 1})-->+(b:SP {id: 5}), q = ACYCLIC (c:SP {id: 2})-->+(d:SP {id: 4}) RETURN count(*) AS l":              {i(0)},
		"MATCH (a:SP {id: 1}) RETURN COUNT { MATCH p = ANY SHORTEST (a)-->+(b:SP {id: 5}) } AS c":                                            {i(1)},
		"MATCH (a:SP {id: 1}) RETURN EXISTS { MATCH ANY SHORTEST (a)-->+(:SP {id: 5}) } AS c":                                                {l(true)},
		"MATCH (a:SP {id: 1}) RETURN EXISTS { MATCH p = shortestPath((a)-[*]->(:SP {id: 5})) } AS c":                                         {l(true)},
		"MATCH (a:SP {id: 5}) WHERE EXISTS { MATCH ANY SHORTEST (a)-->+(:SP {id: 1}) } RETURN a.id":                                          {},
		"CALL () { MATCH p = SHORTEST 2 (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN length(p) AS l } RETURN l ORDER BY l":                        {i(1), i(2)},
		"MATCH p = SHORTEST 2 (a:SP {id: 1})-->+(b:SP {id: 3}) RETURN length(p) AS l ORDER BY l LIMIT 1":                                     {i(1)},
	} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"k": int64(2)})
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
	result, err := exec.Execute(ctx, "MATCH p = ANY SHORTEST (a:SP {id: 1})-->+(:SP {id: 5}) RETURN *", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "p"}, result.Columns, "RETURN * lists only the variables the statement names")
}

// Path selectors and path modes fail as in Neo4j (#907).
func TestPathSelectorErrors(t *testing.T) {
	exec, ctx := newPathSelectorExecutor(t)
	for query, message := range map[string]string{
		"MATCH p = SHORTEST 0 (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN p":                              "The path count needs to be greater than 0.",
		"MATCH p = ANY 0 PATHS (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN p":                             "The path count needs to be greater than 0.",
		"MATCH p = SHORTEST 0 GROUPS (a)-->+(b) RETURN p":                                             "The group count needs to be greater than 0.",
		"MATCH p = ANY SHORTEST (a)-->+(b), (c) RETURN p":                                             "Multiple path patterns cannot be used in the same clause in combination with a selective path selector.",
		"MATCH (c), p = ANY SHORTEST (a)-->+(b) RETURN p":                                             "Multiple path patterns cannot be used in the same clause in combination with a selective path selector.",
		"MATCH p = ANY SHORTEST shortestPath((a)-[*]->(b)) RETURN p":                                  "Mixing shortestPath/allShortestPaths with path selectors",
		"MATCH p = ACYCLIC shortestPath((a)-[*]->(b)) RETURN p":                                       "Mixing shortestPath/allShortestPaths with path selectors",
		"MATCH DIFFERENT RELATIONSHIPS p = shortestPath((a:SP {id: 1})-[*]->(b:SP {id: 5})) RETURN p": "Mixing shortestPath/allShortestPaths with path selectors",
		"MATCH p = ACYCLIC (a)-[*]->(b) RETURN p":                                                     "Using a variable-length relationship such as `-[*]->` together with explicit path mode `ACYCLIC` is not available.",
		"MATCH p = SHORTEST 1 WALK (a)-[:T*1..2 {w: 1}]->(b) RETURN p":                                "explicit path mode `WALK`",
		"CREATE p = ANY SHORTEST (a:X)-[:T]->(b:X) RETURN p":                                          "Path selectors such as `SHORTEST 1 PATHS` cannot be used in a CREATE clause, but only in a MATCH clause.",
		"MERGE p = ANY SHORTEST (a:X)-[:T]->(b:X) RETURN p":                                           "cannot be used in a MERGE clause",
		"MATCH p = SHORTEST $z (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                "Count requires positive integer argument, got `0`",
		"MATCH (x:Nope) MATCH p = SHORTEST $z (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l": "Count requires positive integer argument, got `0`",
		"MATCH p = SHORTEST $n (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                "Expected Integer but got NO_VALUE",
		"MATCH p = SHORTEST $f (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                "Expected Integer but got Double",
		"MATCH p = SHORTEST $s (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                "Expected Integer but got String",
		"MATCH p = ANY $b (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                     "Expected Integer but got Boolean",
		"MATCH p = ANY $list (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                  "Expected Integer but got List",
		"MATCH p = ANY $map (a:SP {id: 1})-->+(b:SP {id: 5}) RETURN length(p) AS l":                   "Expected Integer but got Map",
		"RETURN __nornic_path_selector('ANY', 1, false, '', true) AS x":                               "a path selector or match mode can only be used in a MATCH pattern",
	} {
		params := map[string]interface{}{"z": int64(0), "n": nil, "f": 1.5, "s": "2", "b": true, "list": []interface{}{int64(1)}, "map": map[string]interface{}{"a": int64(1)}}
		_, err := exec.Execute(ctx, query, params)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), message, query)
	}
}

// The statement rewrite writes selectors and path modes in the forms every
// route reads (#907).
func TestPathSelectorRewrite(t *testing.T) {
	for query, want := range map[string]string{
		"MATCH p = ALL PATHS (a)-->(b) RETURN p":                                              "MATCH p = (a)-->(b) RETURN p",
		"MATCH p = TRAIL (a)-->(b) RETURN p":                                                  "MATCH p = (a)-->(b) RETURN p",
		"MATCH p = ACYCLIC (a)-->(b) RETURN p":                                                "MATCH p = (a)-->(b) WHERE __nornic_acyclic(p) RETURN p",
		"MATCH ACYCLIC (a)-->(b) RETURN a":                                                    "MATCH __nornic_lx0 = (a)-->(b) WHERE __nornic_acyclic(__nornic_lx0) RETURN a",
		"MATCH p = ANY SHORTEST (a)-->(b) RETURN p":                                           "MATCH p = shortestPath((a)-->(b)) WHERE __nornic_path_selector('SHORTEST', 1, false, '', true) RETURN p",
		"MATCH p = SHORTEST 2 GROUPS ACYCLIC (a:A|B)-->(b) WHERE b.x = 1 OR b.y = 2 RETURN p": "MATCH p = shortestPath((a)-->(b)) WHERE __nornic_path_selector('SHORTEST', 2, true, 'ACYCLIC', a:A|B) AND (b.x = 1 OR b.y = 2) RETURN p",
		"MATCH p = ANY $k ((a)-->(b) WHERE a.x = 1) RETURN p":                                 "MATCH p = shortestPath((a)-->(b)) WHERE __nornic_path_selector('ANY', $k, false, '', (a.x = 1)) RETURN p",
		"MATCH DIFFERENT RELATIONSHIPS (a)-->(b) RETURN a":                                    "MATCH  (a)-->(b) RETURN a",
		"MATCH (n) WHERE all(x IN [1] WHERE x = 1) RETURN n":                                  "MATCH (n) WHERE all(x IN [1] WHERE x = 1) RETURN n",
	} {
		rewritten, _, err := desugarLabelExpressions(query, nil, false)
		require.NoError(t, err, query)
		require.Equal(t, want, rewritten, query)
	}
	require.True(t, mayUsePathPatternPrefix("MATCH p = ALL (a) RETURN p"))
	require.True(t, mayUsePathPatternPrefix("MATCH ANY (a) RETURN a"))
	require.True(t, mayUsePathPatternPrefix("MATCH (b), p = any (a) RETURN a"))
	require.False(t, mayUsePathPatternPrefix("MATCH (n) WHERE any(x IN n.l WHERE x > 1) RETURN all"))
	require.False(t, mayUsePathPatternPrefix("RETURN company, allowed, anything"))
	for _, query := range []string{"MATCH WALK (a) RETURN a", "MATCH TRAIL (a)", "MATCH ACYCLIC (a)", "MATCH DIFFERENT RELATIONSHIPS (a)",
		"MATCH REPEATABLE ELEMENTS (a)", "MATCH SHORTEST 2 (a)", "RETURN shortestPath((a)--(b))", "RETURN `allShortestPaths`((a)--(b))"} {
		require.True(t, mayUsePathPatternPrefix(query), query)
	}
	require.False(t, mayUsePathPatternPrefix("RETURN 'walk', \"trail\", trailing, walks, shorter, 'shortest' AS x"))

	for _, text := range []string{"SHORTEST (a)", "SHORTEST PATHS (a)", "ANY SHORTEST x", "ANY", "ALL PATHS"} {
		_, ok, err := scanPathPatternPrefix(text, 0, len(text))
		require.NoError(t, err, text)
		require.False(t, ok, text)
	}
	prefix, ok, err := scanPathPatternPrefix("SHORTEST 3 PATHS (a)", 0, len("SHORTEST 3 PATHS (a)"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, pathPatternPrefix{start: 0, end: 17, kind: "SHORTEST", count: "3"}, prefix)

	require.False(t, hasVariableLengthRelationship("(a {l: [1, 2]})-[r WHERE r.x * 2 > 1]->(b)", 0, len("(a {l: [1, 2]})-[r WHERE r.x * 2 > 1]->(b)")))
	require.False(t, hasVariableLengthRelationship("(a)-[r", 0, len("(a)-[r")))
	require.True(t, hasVariableLengthRelationship("(a)-['x', `y`]->(b)-[*2]->(c)", 0, len("(a)-['x', `y`]->(b)-[*2]->(c)")))
}

// The helpers of selected patterns read every form a value may take.
func TestPathSelectorHelpers(t *testing.T) {
	a, b := &storage.Node{ID: "a"}, &storage.Node{ID: "b"}
	path := PathResult{Nodes: []*storage.Node{a, b, a}}
	ids, ok := pathNodeIDs(path)
	require.True(t, ok)
	require.False(t, distinctNodeIDs(ids))
	ids, ok = pathNodeIDs(&PathResult{Nodes: []*storage.Node{a, b}})
	require.True(t, ok)
	require.True(t, distinctNodeIDs(ids))
	ids, ok = pathNodeIDs(map[string]interface{}{"nodes": []interface{}{a, b}})
	require.True(t, ok)
	require.Equal(t, []storage.NodeID{"a", "b"}, ids)
	_, ok = pathNodeIDs(map[string]interface{}{"nodes": []interface{}{"x"}})
	require.False(t, ok)
	_, ok = pathNodeIDs(map[string]interface{}{})
	require.False(t, ok)
	_, ok = pathNodeIDs("x")
	require.False(t, ok)

	for text, want := range map[string]interface{}{"3": int64(3), "1.5": float64(0), "null": nil, "TRUE": true, "[1]": []interface{}{}, "{a: 1}": map[string]interface{}{}, "'x'": "'x'"} {
		require.Equal(t, want, pathSelectorCountLiteral(text), text)
	}
	for value, want := range map[interface{}]string{float32(1): "Double", int8(1): "Any"} {
		require.Equal(t, want, pathSelectorCountTypeName(value))
	}
	count, err := pathSelectorCount(withQueryParams(context.Background(), map[string]interface{}{"k": 3, "j": int32(4), "big": int64(1 << 62)}), "$k")
	require.NoError(t, err)
	require.Equal(t, 3, count)
	count, err = pathSelectorCount(withQueryParams(context.Background(), map[string]interface{}{"j": int32(4)}), "$j")
	require.NoError(t, err)
	require.Equal(t, 4, count)
	count, err = pathSelectorCount(withQueryParams(context.Background(), map[string]interface{}{"big": int64(1 << 62)}), "$big")
	require.NoError(t, err)
	require.Positive(t, count)
	_, err = pathSelectorCount(context.Background(), "-2")
	require.ErrorContains(t, err, "got `-2`")

	selector, predicate, terms, ok := splitPathSelectorTerm("__nornic_path_selector('ANY', 2, false, '', a.x = 1, 2) AND b.y = 1")
	require.True(t, ok)
	require.Equal(t, &pathSelector{count: "2"}, selector)
	require.Equal(t, "a.x = 1,2", predicate)
	require.Equal(t, []string{"b.y = 1"}, terms)
	for _, where := range []string{"", "a.x = 1", "__nornic_path_selector", "__nornic_path_selector('ANY', 2)", "__nornic_path_selector('ANY' AND x"} {
		_, _, _, ok := splitPathSelectorTerm(where)
		require.False(t, ok, where)
	}
}
