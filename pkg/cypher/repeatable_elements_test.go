package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// MATCH REPEATABLE ELEMENTS lets its clause repeat relationships, and only
// its clause; the rows are Neo4j 2026.09's (#907).
func TestRepeatableElements(t *testing.T) {
	exec, ctx := newPathSelectorExecutor(t)
	l := func(values ...interface{}) []interface{} { return values }
	i := func(value int64) []interface{} { return l(value) }
	for query, want := range map[string][][]interface{}{
		"MATCH REPEATABLE ELEMENTS p = (a:SP {id: 1})-->{1,4}(b) RETURN count(p) AS c":                                   {i(16)},
		"MATCH DIFFERENT RELATIONSHIPS p = (a:SP {id: 1})-->{1,4}(b) RETURN count(p) AS c":                               {i(12)},
		"MATCH REPEATABLE ELEMENTS p = (a:SP {id: 1})-->{1,4}(b:SP {id: 1}) RETURN length(p) AS l ORDER BY l":            {i(2), i(3), i(4)},
		"MATCH REPEATABLE ELEMENTS p = SHORTEST 2 (a:SP {id: 1})-->{1,5}(b:SP {id: 1}) RETURN length(p) AS l ORDER BY l": {i(2), i(3)},
		"MATCH REPEATABLE ELEMENTS p = ANY 3 (a:SP {id: 1})-->{1,6}(b:SP {id: 5}) RETURN length(p) AS l ORDER BY l":      {i(3), i(4), i(5)},
		"MATCH REPEATABLE ELEMENTS p = WALK (a:SP {id: 1})-->{1,3}(b) RETURN count(*) AS c":                              {i(10)},
		"MATCH REPEATABLE ELEMENTS (a:SP)-[r:T]->(b), (c)-[r]->(d) RETURN count(*) AS c":                                 {i(6)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-->(b)-->(c)-->(d)-->(e) RETURN count(*) AS c":                          {i(6)},
		"MATCH (a:SP {id: 1})-->(b)-->(c)-->(d)-->(e) RETURN count(*) AS c":                                              {i(3)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[r]->(b)<-[s]-(a) RETURN count(*) AS c":                                {i(2)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[r]->(b), (a)-[s]->(b) RETURN count(*) AS c":                           {i(2)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 3})-[r]->(b), (a)-[s]->(c) RETURN count(*) AS c":                           {i(4)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[*1..3]->(b) RETURN count(*) AS c":                                     {i(10)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[*..3]->(b) RETURN count(*) AS c":                                      {i(10)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[*2]->(b) RETURN count(*) AS c":                                        {i(3)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})--{2}(b) RETURN count(*) AS l":                                          {i(10)},
		"MATCH (a:SP {id: 1})--{2}(b) RETURN count(*) AS l":                                                              {i(7)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[r*1..2]->(b), (c)-[r*1..2]->(d) RETURN count(*) AS c":                 {i(5)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})<-[r]-(b)-[s]->(c) RETURN count(*) AS c":                                {i(2)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[]->{1,2}(b) RETURN count(*) AS c":                                     {i(5)},
		"MATCH REPEATABLE ELEMENTS p = (a:SP {id: 1})-->{0,1}(b) RETURN count(*) AS c":                                   {i(3)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-->(b) WHERE b.id = 2 OR b.id = 3 RETURN count(*) AS l":                 {i(2)},
		"MATCH REPEATABLE ELEMENT BINDINGS (a:SP {id: 1})-->(b) RETURN count(*) AS l":                                    {i(2)},
		"MATCH REPEATABLE ELEMENT (a:SP {id: 1})-->(b) RETURN count(*) AS l":                                             {i(2)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-->(b) MATCH (b)-->(c)-->(d)-->(b) RETURN count(*) AS l":                {i(2)},
		"MATCH (a:SP {id: 1}) MATCH REPEATABLE ELEMENTS (a)-[r]->(b)-[s]->(a) RETURN count(*) AS c":                      {i(1)},
		"MATCH (a:SP {id: 1}) WHERE EXISTS { MATCH REPEATABLE ELEMENTS (a)-[r]->(b)<-[r]-(a) } RETURN count(*) AS c":     {i(1)},
		"MATCH (a:SP {id: 1}) RETURN COUNT { MATCH REPEATABLE ELEMENTS (a)-->(b)-->(c)-->(d)-->(e) } AS c":               {i(6)},
		"OPTIONAL MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[r]->(b), (c)-[r]->(d) RETURN count(*) AS c":                  {i(2)},
		"MATCH (x:SP {id: 5}) OPTIONAL MATCH REPEATABLE ELEMENTS (x)-[r]->(b), (c)-[r]->(d) RETURN x.id AS x, b":         {l(int64(5), nil)},
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[r]->(b) WITH r MATCH (x)-[r]->(y), (y)-[s]->(z) RETURN count(*) AS c": {i(3)},
		"MATCH REPEATABLE ELEMENTS p = (a:SP {id: 1})-[r]->{2}(b) RETURN size(r) = length(p) AS same LIMIT 1":            {l(true)},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	for query, message := range map[string]string{
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-->+(b) RETURN count(*) AS c":                                 "The quantified path pattern may yield an infinite number of rows under match mode 'REPEATABLE ELEMENTS'.",
		"MATCH REPEATABLE ELEMENTS p = SHORTEST 2 (a:SP {id: 1})-->+(b:SP {id: 1}) RETURN length(p) AS l":      "may yield an infinite number of rows",
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[*]->(b) RETURN count(*) AS c":                               "may yield an infinite number of rows",
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[*2..]->(b) RETURN count(*) AS c":                            "may yield an infinite number of rows",
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-->{2,}(b) RETURN count(*) AS c":                              "may yield an infinite number of rows",
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[r]->*(b) RETURN count(*) AS c":                              "may yield an infinite number of rows",
		"MATCH REPEATABLE ELEMENTS (a:SP {id: 1})-[r WHERE r.w * 2 > 1]-*(b) RETURN count(*) AS c":             "may yield an infinite number of rows",
		"MATCH REPEATABLE ELEMENTS p = shortestPath((a:SP {id: 1})-[*]->(b:SP {id: 5})) RETURN length(p) AS l": "Mixing shortestPath/allShortestPaths with path selectors",
		"MATCH REPEATABLE ELEMENTS p = ACYCLIC (a:SP {id: 1})-->{1,3}(b) RETURN count(*) AS c":                 "REPEATABLE ELEMENTS with ACYCLIC path mode is not supported.",
		"MATCH REPEATABLE ELEMENTS p = ACYCLIC (a:SP {id: 1})-->+(b) RETURN count(*) AS c":                     "REPEATABLE ELEMENTS with ACYCLIC path mode is not supported.",
		"MATCH REPEATABLE ELEMENTS p = SHORTEST 1 TRAIL (a:SP {id: 1})-->{1,3}(b) RETURN count(*) AS c":        "REPEATABLE ELEMENTS with TRAIL path mode is not supported.",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), message, query)
	}

	_, err := exec.Execute(ctx, "RETURN __nornic_repeatable_elements() AS x", nil)
	require.ErrorContains(t, err, "a path selector or match mode can only be used in a MATCH pattern")

	rewritten, _, err := desugarLabelExpressions("MATCH REPEATABLE ELEMENTS p = ANY 2 (a)-->{1,3}(b:A|B) WHERE a.x = 1 RETURN p", nil, false)
	require.NoError(t, err)
	require.Equal(t, "MATCH  p = shortestPath((a)-[*1..3]->(b)) WHERE __nornic_repeatable_elements() AND __nornic_path_selector('ANY', 2, false, '', b:A|B) AND a.x = 1 RETURN p", rewritten)
}

// The match mode marker comes off a MATCH body whole, or not at all.
func TestStripRepeatableElements(t *testing.T) {
	for body, want := range map[string]string{
		"(a)-->(b) WHERE __nornic_repeatable_elements()":               "(a)-->(b)",
		"(a)-->(b) WHERE __nornic_repeatable_elements() AND a.x = 1":   "(a)-->(b) WHERE a.x = 1",
		"(a)-->(b) WHERE __nornic_repeatable_elements() and (a.x = 1)": "(a)-->(b) WHERE (a.x = 1)",
	} {
		stripped, ok := stripRepeatableElements(body)
		require.True(t, ok, body)
		require.Equal(t, want, stripped, body)
	}
	for _, body := range []string{
		"(a)-->(b)",
		"(a)-->(b) WHERE a.x = 1",
		"(a {k: '__nornic_repeatable_elements'})-->(b)",
		"(a)-->(b) WHERE a.x = 1 AND __nornic_repeatable_elements()",
		"(a)-->(b) WHERE __nornic_repeatable_elements() ANDx",
		"(a)-->(b) WHERE __nornic_repeatable_elements() OR a.x = 1",
	} {
		stripped, ok := stripRepeatableElements(body)
		require.False(t, ok, body)
		require.Equal(t, body, stripped, body)
	}
	require.False(t, repeatableElements(context.Background()))
	require.True(t, repeatableElements(withRepeatableElements(context.Background())))
	var nilContext context.Context
	require.False(t, repeatableElements(nilContext))

	matchModes := map[string]bool{"DIFFERENT RELATIONSHIP (a)": false, "REPEATABLE ELEMENTS (a)": true, "REPEATABLE ELEMENT BINDINGS (a)": true}
	for text, repeatable := range matchModes {
		_, gotRepeatable, ok := matchModeAt(text, 0, len(text))
		require.True(t, ok, text)
		require.Equal(t, repeatable, gotRepeatable, text)
	}
	for _, text := range []string{"(a)", "DIFFERENT (a)", "REPEATABLE NODES (a)", "DIFFERENT", "x"} {
		_, _, ok := matchModeAt(text, 0, len(text))
		require.False(t, ok, text)
	}
	require.False(t, hasUnboundedRepetition("(a)-[r", 0, len("(a)-[r")))
	require.False(t, hasUnboundedRepetition("(a {m: {k: 1}})-['x']->(b)", 0, len("(a {m: {k: 1}})-['x']->(b)")))
}
