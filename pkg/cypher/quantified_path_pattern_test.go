package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Quantified path patterns return Neo4j 5.26's and 2026.09's rows (#907):
// iterations chain node to node, a variable inside the group binds a list,
// the group's WHERE applies to each iteration, and no relationship repeats.
func TestQuantifiedPathPatterns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quantified_path"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:SP {id: 1})-[:T {w: 1}]->(b:SP {id: 2})-[:T {w: 2}]->(c:SP {id: 3})-[:T {w: 3}]->(a), (c)-[:T {w: 4}]->(d:SP {id: 4})-[:T {w: 5}]->(e:SP {id: 5}), (a)-[:T {w: 6}]->(c)", nil)
	require.NoError(t, err)
	l := func(values ...interface{}) []interface{} { return values }
	i := func(values ...int64) []interface{} {
		out := make([]interface{}, len(values))
		for index, value := range values {
			out[index] = value
		}
		return out
	}
	for query, want := range map[string][][]interface{}{
		"MATCH p = (a:SP {id: 1})((x)-[:T]->(y))+(b:SP {id: 5}) RETURN length(p) AS l ORDER BY l":                                        {i(3), i(4), i(6), i(6)},
		"MATCH p = (a:SP {id: 1})((x)-[:T]->(y)){2}(b) RETURN [n IN nodes(p) | n.id] AS ids ORDER BY ids":                                {l(i(1, 2, 3)), l(i(1, 3, 1)), l(i(1, 3, 4))},
		"MATCH (a:SP {id: 1})((x)-[r:T]->(y)){1,2}(b) RETURN [n IN x | n.id] AS xs, [n IN y | n.id] AS ys, size(r) AS s ORDER BY xs, ys": {l(i(1), i(2), int64(1)), l(i(1), i(3), int64(1)), l(i(1, 2), i(2, 3), int64(2)), l(i(1, 3), i(3, 1), int64(2)), l(i(1, 3), i(3, 4), int64(2))},
		"MATCH (a:SP {id: 1})((x)-[r:T]->(y) WHERE r.w > 1){1,3}(b) RETURN [n IN x | n.id] AS xs ORDER BY xs":                            {l(i(1)), l(i(1, 3)), l(i(1, 3)), l(i(1, 3, 4))},
		"MATCH (a:SP {id: 1})((x:SP)-->(y:SP WHERE y.id > 2))+(b) RETURN [n IN y | n.id] AS ys ORDER BY ys":                              {l(i(3)), l(i(3, 4)), l(i(3, 4, 5))},
		"MATCH (a:SP {id: 1}) ((x)-[:T]->(y)-[:T]->(z)){1,2} (b) RETURN [n IN z | n.id] AS zs ORDER BY zs":                               {l(i(1)), l(i(1, 3)), l(i(3)), l(i(3, 3)), l(i(3, 5)), l(i(4))},
		"MATCH p = (:SP {id: 1})(()-->()){0,1}(b) RETURN length(p) AS l, b.id AS b ORDER BY l, b":                                        {i(0, 1), i(1, 2), i(1, 3)},
		"MATCH (a:SP {id: 1})((x)-->(y))*(b:SP {id: 1}) RETURN size(x) AS s ORDER BY s":                                                  {i(0), i(2), i(3)},
		"MATCH p = SHORTEST 1 (a:SP {id: 1})((x)-[:T]->(y))+(b:SP {id: 5}) RETURN length(p) AS l":                                        {i(3)},
		"MATCH (a:SP {id: 1})((x)-->(y))+(b:SP {id: 5}) RETURN count(*) AS c":                                                            {i(4)},
		"MATCH (a:SP {id: 1})((x)-->(y))+(b:SP {id: 5}) RETURN x[0].id AS f, y[-1].id AS l ORDER BY f, l":                                {i(1, 5), i(1, 5), i(1, 5), i(1, 5)},
		"MATCH (a:SP {id: 1})((x)-->(y))+(b) WHERE size(x) = 2 RETURN count(*) AS c":                                                     {i(3)},
		"MATCH (a:SP {id: 1})((x)-->(y)){2}(b)-->(c) RETURN count(*) AS c":                                                               {i(4)},
		"MATCH (a:SP {id: 1})((x)-->(y))+(b), (b)-->(z) RETURN count(*) AS c":                                                            {i(14)},
		"MATCH (a)((x)-->(y))+ RETURN count(*) AS c":                                                                                     {i(35)},
		"MATCH ((x)-->(y))+ RETURN count(*) AS c":                                                                                        {i(35)},
		"MATCH (a)((x)-->(y))+(b) RETURN count(*) AS c":                                                                                  {i(35)},
		"MATCH (a:SP {id: 1})((x)-->(y))+(b:SP {id: 5}) RETURN x IS :: LIST<NODE> AS t LIMIT 1":                                          {l(true)},
		"MATCH (a:SP {id: 1})((x)-[r]->(y))+(b:SP {id: 5}) WITH r RETURN [e IN r | e.w] AS ws ORDER BY ws":                               {l(i(1, 2, 3, 6, 4, 5)), l(i(1, 2, 4, 5)), l(i(6, 3, 1, 2, 4, 5)), l(i(6, 4, 5))},
		"MATCH (a:SP {id: 1})((x)-->(y) WHERE x.id < y.id)+(b) RETURN [n IN y | n.id] AS ys ORDER BY ys":                                 {l(i(2)), l(i(2, 3)), l(i(2, 3, 4)), l(i(2, 3, 4, 5)), l(i(3)), l(i(3, 4)), l(i(3, 4, 5))},
		"MATCH (a:SP {id: 1})((x:SP|X)-->(y WHERE y.id > 2) WHERE x.id = 1 OR x.id = 3)+(b) RETURN [n IN y | n.id] AS ys ORDER BY ys":    {l(i(3)), l(i(3, 4))},
		"MATCH (a:SP {id: 1})((x)-->(y) WHERE y.id = b.id)+(b:SP {id: 3}) RETURN [n IN y | n.id] AS ys":                                  {l(i(3))},
		"MATCH (a:SP {id: 1})((x)-->(y) WHERE x.id = a.id)+(b) RETURN [n IN y | n.id] AS ys ORDER BY ys":                                 {l(i(2)), l(i(3))},
		"MATCH (a:SP {id: 1})((x)-->(y))+((u)-->(v))+(b:SP {id: 5}) RETURN size(x) AS x, size(u) AS u ORDER BY x, u":                     {i(1, 2), i(1, 3), i(1, 5), i(1, 5), i(2, 1), i(2, 2), i(2, 4), i(2, 4), i(3, 1), i(3, 3), i(3, 3), i(4, 2), i(4, 2), i(5, 1), i(5, 1)},
		"MATCH (a:SP {id: 1})((x)--(y)){2}(b:SP {id: 4}) RETURN count(*) AS c":                                                           {i(2)},
		"MATCH (n:SP {id: 1}) MATCH (n)((x)-->(y))+(b) RETURN count(*) AS c":                                                             {i(16)},
		"MATCH (a:SP {id: 1})((x)-->(y))* RETURN count(*) AS c":                                                                          {i(17)},
		"MATCH (a:SP {id: 1})((x)-->(y))+ WITH count(*) AS c RETURN c":                                                                   {i(16)},
		"MATCH (a)((x)-->(y))+ RETURN a.id AS a, size(x) AS s ORDER BY a, s LIMIT 3":                                                     {i(1, 1), i(1, 1), i(1, 2)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})((x)-->(y)){1,3}(b) RETURN count(*) AS c":                                               {i(10)},
		"OPTIONAL MATCH (a:SP {id: 5})((x)-->(y))+(b) RETURN a, x":                                                                       {l(nil, nil)},
		"MATCH (a:SP {id: 1}) RETURN COUNT { MATCH (a)((x)-->(y)){2}(b) } AS c":                                                          {i(3)},
		"MATCH (a:SP {id: 1})((x)-->(y))+(b:SP {id: 5}) RETURN a, b LIMIT 0":                                                             nil,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if want == nil {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
	for query, message := range map[string]string{
		"MATCH (a)((x)-[*]->(y))+(b) RETURN 1":                     "Variable length relationships cannot be part of a quantified path pattern.",
		"MATCH (a)((x)-->+(y))+(b) RETURN 1":                       "Quantified path patterns are not allowed to be nested.",
		"MATCH (a)(((x)-->(y))+(z)-->(w))+(b) RETURN 1":            "Quantified path patterns are not allowed to be nested.",
		"MATCH (a)((x))+(b) RETURN 1":                              "A quantified path pattern needs to have at least one relationship.",
		"MERGE (a)((x)-->(y))+(b)":                                 "Quantified path patterns cannot be used in a MERGE clause, but only in a MATCH clause.",
		"CREATE (a)((x)-->(y))+(b)":                                "Quantified path patterns cannot be used in a CREATE clause, but only in a MATCH clause.",
		"MATCH REPEATABLE ELEMENTS (a)((x)-->(y))+(b) RETURN 1":    "may yield an infinite number of rows",
		"MATCH (a)((x)-->(y)){0}(b) RETURN 1":                      "A quantifier for a path pattern must not be limited by 0.",
		"MATCH (a)((x)-->(y)){2,1}(b) RETURN 1":                    "A quantifier for a path pattern must not have a lower bound which exceeds its upper bound.",
		"MATCH (a)-->{2,1}(b) RETURN 1":                            "A quantifier for a path pattern must not have a lower bound which exceeds its upper bound.",
		"MATCH (a:SP {id: 1})((x)-->(y))+(x) RETURN 1":             "The variable `x` occurs both inside and outside a quantified path pattern and needs to be renamed.",
		"MATCH (x:SP {id: 1}) MATCH (a)((x)-->(y))+(b) RETURN 1":   "The variable `x` is already defined in a previous clause",
		"WITH 1 AS r MATCH (a)((x)-[r]->(y))+(b) RETURN 1":         "The variable `r` is already defined in a previous clause",
		"MATCH (a)((x)-->(y))+(b) RETURN x.id":                     "Type mismatch",
		"MATCH (a)((x)-->(y))+ WHERE (b) RETURN 1":                 "",
		"MATCH (a)((x)-->(y))+, ((u)-->(v))+(b)-[x]->(c) RETURN 1": "",
		"MATCH (a)((x)-->(y))+ -->(c) RETURN 1":                    "invalid path pattern",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), message, query)
	}
}

// The helpers of quantified path patterns on their edges.
func TestQuantifiedPathPatternHelpers(t *testing.T) {
	require.False(t, hasQuantifiedGroup("MATCH (a)-->(b) RETURN (1) + 2"))
	require.False(t, hasQuantifiedGroup("RETURN ((1)) + 2, [((x)-->(y)) | 1]"))
	require.True(t, hasQuantifiedGroup("MATCH (a {s: 'x)+'})((x)-->(y)){1,2}(b)"))
	require.False(t, hasQuantifiedGroup("MATCH (a {s: '((x)-->(y))+'}) RETURN a"))
	require.False(t, hasQuantifiedGroup("MATCH ((a)-->(b) RETURN 1"))
	require.True(t, mayUseQuantifiedGroup("MATCH (a)((x))+(b) RETURN 1"))
	require.False(t, mayUseQuantifiedGroup("MATCH (a)-->(b) RETURN a"))
	_, _, _, ok := nextQuantifiedGroup("MATCH (a)((x)-->(y)", 0)
	require.False(t, ok)

	require.True(t, quantifiedGroupQuantifierAt("((a)-->(b))+", 11))
	require.True(t, quantifiedGroupQuantifierAt("((a)<-[r]-(b)) +", 15))
	require.False(t, quantifiedGroupQuantifierAt("((1)) + 2", 6))
	require.False(t, quantifiedGroupQuantifierAt("(1) + 2", 4))
	require.False(t, quantifiedGroupQuantifierAt("x + 2", 2))
	require.False(t, quantifiedGroupQuantifierAt(")) + 2", 3))
	require.False(t, quantifiedGroupQuantifierAt("((a) + (b))+", 11))
	require.False(t, quantifiedGroupQuantifierAt("((a)+", 4))

	chain, first, last := nameChainEnds("(:A)-->()", func() string { return "g" })
	require.Equal(t, "(g:A)-->(g)", chain)
	require.Equal(t, "g", first)
	require.Equal(t, "g", last)
	chain, first, last = nameChainEnds("(a)", func() string { return "g" })
	require.Equal(t, "(a)", chain)
	require.Equal(t, "a", first)
	require.Equal(t, "a", last)
	require.Equal(t, 11, lastNodePatternStart("(a)-['x']->(b {k: '('})"))
	require.Equal(t, 0, lastNodePatternStart("(a"))
	require.Equal(t, -1, lastNodePatternStart("-->"))

	nodes, relationships := quantifiedGroupVariables("(a)((x)-[r]->(y) WHERE x.k = 1){2}(b)-[s]->(c)")
	require.Equal(t, map[string]struct{}{"x": {}, "y": {}}, nodes)
	require.Equal(t, map[string]struct{}{"r": {}}, relationships)
	nodes, _ = quantifiedGroupVariables("(a)-->(b)")
	require.Nil(t, nodes)

	_, ok, err := parseQuantifiedPathMatch("(a)-->(b)")
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = parseQuantifiedPathMatch("(a)((x)-->(y))+(b), (c)")
	require.NoError(t, err)
	require.False(t, ok, "a part at a time, through the pattern product")
	_, ok, err = parseQuantifiedPathMatch("(a)((x)-->(y))+ -->(c)")
	require.True(t, ok)
	require.Error(t, err)
	m, ok, err := parseQuantifiedPathMatch("p = (a)((x)-->(y) WHERE y.k = b.k AND x.k = 1)+(b) WHERE a.k = 1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "p", m.pathVariable)
	require.Equal(t, "a.k = 1", m.where)
	require.Equal(t, "x.k = 1", m.pieces[1].where)
	require.Equal(t, "y.k = b.k", m.pieces[1].deferred)
}
