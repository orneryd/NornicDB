package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSimpleMatchClause(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		m, ok := parseSimpleMatchClause("MATCH (u:User {id: row.uid})", "row")
		require.True(t, ok)
		require.Equal(t, "u", m.variable)
		require.Equal(t, "User", m.label)
		require.Equal(t, "id", m.propName)
		require.Equal(t, "uid", m.rowField)
	})

	t.Run("invalid with multiple props", func(t *testing.T) {
		_, ok := parseSimpleMatchClause("MATCH (u:User {id: row.uid, name: row.name})", "row")
		require.False(t, ok)
	})

	t.Run("invalid expression base", func(t *testing.T) {
		_, ok := parseSimpleMatchClause("MATCH (u:User {id: other.uid})", "row")
		require.False(t, ok)
	})
}

func TestParseSimpleCreateNodeAndEdge(t *testing.T) {
	t.Run("create node valid", func(t *testing.T) {
		node, ok := parseSimpleCreateNode("(p:Post {id: row.pid, title: 'hello', likes: 4, active: true, score: 1.5, missing: null})", "row")
		require.True(t, ok)
		require.Equal(t, "p", node.variable)
		require.Equal(t, "Post", node.label)
		require.Equal(t, "pid", node.rowFieldRefs["id"])
		require.Equal(t, "hello", node.literals["title"])
		require.EqualValues(t, int64(4), node.literals["likes"])
		require.Equal(t, true, node.literals["active"])
		require.Equal(t, 1.5, node.literals["score"])
		require.Nil(t, node.literals["missing"])
	})

	t.Run("create node invalid expression", func(t *testing.T) {
		_, ok := parseSimpleCreateNode("(p:Post {id: toString(row.pid)})", "row")
		require.False(t, ok)
	})

	t.Run("create edge valid", func(t *testing.T) {
		edge, ok := parseSimpleCreateEdge("(u)-[:AUTHORED {at: row.ts, kind: 'draft'}]->(p)", "row")
		require.True(t, ok)
		require.Equal(t, "u", edge.startVar)
		require.Equal(t, "p", edge.endVar)
		require.Equal(t, "AUTHORED", edge.relType)
		require.Equal(t, "ts", edge.rowFieldRefs["at"])
		require.Equal(t, "draft", edge.literals["kind"])
	})

	t.Run("create edge invalid arrow", func(t *testing.T) {
		_, ok := parseSimpleCreateEdge("(u)-[:AUTHORED]-(p)", "row")
		require.False(t, ok)
	})

	t.Run("create edge invalid type", func(t *testing.T) {
		_, ok := parseSimpleCreateEdge("(u)-[:BAD-TYPE]->(p)", "row")
		require.False(t, ok)
	})
}

func TestParseSimpleCreateClauseKinds(t *testing.T) {
	node, edge, kind, ok := parseSimpleCreateClause("CREATE (p:Post {id: row.pid})", "row")
	require.True(t, ok)
	require.Equal(t, byte('n'), kind)
	require.Equal(t, "p", node.variable)
	require.Empty(t, edge.startVar)

	node, edge, kind, ok = parseSimpleCreateClause("CREATE (u)-[:AUTHORED]->(p)", "row")
	require.True(t, ok)
	require.Equal(t, byte('e'), kind)
	require.Equal(t, "u", edge.startVar)
	require.Equal(t, "p", edge.endVar)
	require.Empty(t, node.variable)

	_, _, kind, ok = parseSimpleCreateClause("CREATE bad", "row")
	require.False(t, ok)
	require.Zero(t, kind)
}

func TestParsePropsBodyForUnwindFastPath(t *testing.T) {
	rowRefs, literals, ok := parsePropsBodyForUnwindFastPath("id: row.id, score: 1.25, active: true, note: 'ok', nothing: null", "row")
	require.True(t, ok)
	require.Equal(t, map[string]string{"id": "id"}, rowRefs)
	require.Equal(t, 1.25, literals["score"])
	require.Equal(t, true, literals["active"])
	require.Equal(t, "ok", literals["note"])
	require.Nil(t, literals["nothing"])

	_, _, ok = parsePropsBodyForUnwindFastPath("bad: fn(row.id)", "row")
	require.False(t, ok)
}

func TestParseUnwindMultiMatchCreatePlan(t *testing.T) {
	rest := "MATCH (u:User {id: row.uid}) CREATE (p:Post {id: row.pid, title: 't'}) CREATE (u)-[:AUTHORED {at: row.ts}]->(p)"
	plan, ok := parseUnwindMultiMatchCreatePlan(rest, "row")
	require.True(t, ok)
	require.Len(t, plan.matches, 1)
	require.Len(t, plan.nodeCreates, 1)
	require.Len(t, plan.edgeCreates, 1)
	require.Equal(t, "uid", plan.matches[0].rowField)
	require.Equal(t, "pid", plan.nodeCreates[0].rowFieldRefs["id"])
	require.Equal(t, "t", plan.nodeCreates[0].literals["title"])
	require.Equal(t, "ts", plan.edgeCreates[0].rowFieldRefs["at"])

	_, ok = parseUnwindMultiMatchCreatePlan("CREATE (p:Post {id: row.pid})", "row")
	require.False(t, ok)

	_, ok = parseUnwindMultiMatchCreatePlan("MATCH (u:User {id: row.uid})", "row")
	require.False(t, ok)

	_, ok = parseUnwindMultiMatchCreatePlan("MATCH (u:User {id: row.uid}) CREATE nonsense", "row")
	require.False(t, ok)
}

func TestBuildPropsFromSpecAndIndexMatchingParen(t *testing.T) {
	row := map[string]any{"a": "x", "n": int64(3)}
	props := buildPropsFromSpec(row, map[string]string{"k1": "a", "k2": "missing"}, map[string]any{"k3": true})
	require.Equal(t, "x", props["k1"])
	_, ok := props["k2"]
	require.False(t, ok)
	require.Equal(t, true, props["k3"])

	require.Equal(t, 5, findMatchingParen("(a(b))", 0))
	require.Equal(t, -1, findMatchingParen("a(b)", 0))
	require.Equal(t, -1, findMatchingParen("(a(b)", 0))
}

func TestIsSimpleWithPassthroughClause(t *testing.T) {
	require.True(t, isSimpleWithPassthroughClause("o, row"))
	require.True(t, isSimpleWithPassthroughClause("o AS keep, row AS source"))
	require.True(t, isSimpleWithPassthroughClause("o,row"))

	require.False(t, isSimpleWithPassthroughClause(""))
	require.False(t, isSimpleWithPassthroughClause("o,"))
	require.False(t, isSimpleWithPassthroughClause("o, row.products"))
	require.False(t, isSimpleWithPassthroughClause("o, toString(row.id) AS s"))
	require.False(t, isSimpleWithPassthroughClause("o + 1 AS x"))
}
