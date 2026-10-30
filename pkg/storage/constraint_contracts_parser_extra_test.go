package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConstraintContract_ParsersAndComparators(t *testing.T) {
	t.Run("parsePropertyInExpression", func(t *testing.T) {
		ok, values, prop, err := parsePropertyInExpression("r.kind IN ['a', 'b', 3]")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "kind", prop)
		require.Len(t, values, 3)

		ok, _, _, err = parsePropertyInExpression("r.kind IN ['a' ")
		require.NoError(t, err)
		require.False(t, ok)

		ok, _, _, err = parsePropertyInExpression("r.kind = 'a'")
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("parseRelationshipPropertyInExpression", func(t *testing.T) {
		ok, prop, values, err := parseRelationshipPropertyInExpression("r.flag IN [true, false]")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "flag", prop)
		require.Len(t, values, 2)
	})

	t.Run("parseCountPatternExpression", func(t *testing.T) {
		ok, pattern, comp, threshold, err := parseCountPatternExpression("COUNT { MATCH (n)-[:REL]->(:Target) } >= 2")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "OUTGOING", pattern.Direction)
		require.Equal(t, "REL", pattern.RelationType)
		require.Equal(t, ">=", comp)
		require.Equal(t, 2, threshold)

		ok, _, _, _, err = parseCountPatternExpression("COUNT { (n)-[:REL]->(:Target) } >= -3")
		require.NoError(t, err)
		require.True(t, ok)

		ok, _, _, _, err = parseCountPatternExpression("COUNT { MATCH (n)-[:REL]->(:Target) } >=")
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("parseNotExistsPatternExpression", func(t *testing.T) {
		ok, pattern, err := parseNotExistsPatternExpression("NOT EXISTS { MATCH (n)<-[:REL]-(:From) }")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "INCOMING", pattern.Direction)

		ok, _, err = parseNotExistsPatternExpression("NOT EXISTS { MATCH (n)-[:REL]->(:Target) ")
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("parseConstraintPattern", func(t *testing.T) {
		p, err := parseConstraintPattern("(n)-[:WORKS_AT]->(:Company:Entity)")
		require.NoError(t, err)
		require.Equal(t, "OUTGOING", p.Direction)
		require.Equal(t, "WORKS_AT", p.RelationType)
		require.Equal(t, []string{"Company", "Entity"}, p.TargetLabels)

		p, err = parseConstraintPattern("(n)<-[:MANAGES]-(:Boss)")
		require.NoError(t, err)
		require.Equal(t, "INCOMING", p.Direction)

		_, err = parseConstraintPattern("n-[:REL]->(:T)")
		require.Error(t, err)

		invalid := []string{
			"(n)-[:REL]-(:T)",           // missing >
			"(n)<-[:REL]>(:T)",          // incoming malformed tail
			"(n)[:REL]->(:T)",           // missing opening dash before rel
			"(n)-[REL]->(:T)",           // missing colon in rel type
			"(n)-[:]->(:T)",             // empty rel type
			"(n)-[:REL]->",              // missing target node
			"(n)-[:REL]->(:T) extra",    // trailing garbage
			"(n",                        // bad source node
			"(n)-[:REL]->(:T",           // bad target node
			"(n)<-[:REL]-(:T) trailing", // incoming trailing garbage
		}
		for _, raw := range invalid {
			_, err := parseConstraintPattern(raw)
			require.Error(t, err, "raw=%q", raw)
		}
	})

	t.Run("endpoint comparators", func(t *testing.T) {
		require.True(t, isDistinctEndpointsExpression("startNode(r) <> endNode(r)"))
		require.False(t, isDistinctEndpointsExpression("startNode(r) = endNode(r)"))

		ok, left, right := parseEndpointPropertyEqualityExpression("startNode(r).dept = endNode(r).dept")
		require.True(t, ok)
		require.Equal(t, "dept", left)
		require.Equal(t, "dept", right)
		ok, _, _ = parseEndpointPropertyEqualityExpression("startNode(r).dept = endNode(r)")
		require.False(t, ok)

		invalidDistinct := []string{
			"startNode() <> endNode(r)",
			"startNode(r) != endNode(r)", // wrong comparator
			"startNode(r) <> endNode()",  // missing var
			"startNode(r) <> endNode(r) x",
			"startNode(r) <>",
		}
		for _, expr := range invalidDistinct {
			require.False(t, isDistinctEndpointsExpression(expr), "expr=%q", expr)
		}

		invalidEndpointEq := []string{
			"startNode(r). = endNode(r).dept",
			"startNode(r).dept = endNode().dept",
			"startNode(r).dept = endNode(r).",
			"startNode(r).dept == endNode(r).dept",
			"startNode(r).dept = endNode(r).dept trailing",
		}
		for _, expr := range invalidEndpointEq {
			ok, _, _ := parseEndpointPropertyEqualityExpression(expr)
			require.False(t, ok, "expr=%q", expr)
		}
	})

	t.Run("relationship property comparison", func(t *testing.T) {
		ok, prop, comp, val, err := parseRelationshipPropertyComparisonExpression("r.weight >= 1.5")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "weight", prop)
		require.Equal(t, ">=", comp)
		require.Equal(t, 1.5, val)

		ok, _, _, _, err = parseRelationshipPropertyComparisonExpression("r.kind IN ['a']")
		require.NoError(t, err)
		require.False(t, ok)

		ok, _, _, _, err = parseRelationshipPropertyComparisonExpression("r.weight >= nope")
		require.Error(t, err)
		require.False(t, ok)
	})

	t.Run("literal helpers", func(t *testing.T) {
		vals, err := parseContractLiteralList("'x', 10, 3.5, true, false, null")
		require.NoError(t, err)
		require.Len(t, vals, 6)

		_, err = parseContractLiteral("unsupported_token")
		require.Error(t, err)

		parts := splitTopLevelCSV("'a,b', [1,2], {x:1}, plain")
		require.Equal(t, []string{"'a,b'", "[1,2]", "{x:1}", "plain"}, parts)
	})

	t.Run("evaluate and compare helpers", func(t *testing.T) {
		require.True(t, evaluatePropertyInExpression(int64(5), []interface{}{int64(4), int64(5)}))
		require.True(t, evaluatePropertyInExpression(nil, []interface{}{"a"}), "absent property satisfies IN")
		require.True(t, compareConstraintExpressionValue(nil, ">", int64(0)), "absent property satisfies comparisons")

		require.True(t, compareConstraintExpressionValue(int64(5), ">", int64(3)))
		require.True(t, compareConstraintExpressionValue("b", ">", "a"))
		require.True(t, compareConstraintExpressionValue("a", "<", "b"))
		require.False(t, compareConstraintExpressionValue(1, "??", 1))

		require.True(t, compareInt(3, ">=", 3))
		require.False(t, compareInt(3, "<", 2))
		require.False(t, compareInt(1, "??", 1))
	})
}
