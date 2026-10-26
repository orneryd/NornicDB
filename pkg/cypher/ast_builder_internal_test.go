package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestASTBuilderDirectHelperCoverage(t *testing.T) {
	builder := NewASTBuilder()

	t.Run("canonical clause scanner skips embedded and quoted keywords", func(t *testing.T) {
		s := "XMATCH MATCH1 MATCH RETURN"
		assert.Equal(t, 14, keywordIndexFrom(s, "MATCH", 0, defaultKeywordScanOpts(acceptedBraceView(s))))
		assert.Equal(t, 7, keywordIndexFrom("MATCH1 MATCH", "MATCH", 0, defaultKeywordScanOpts(acceptedBraceView("MATCH1 MATCH"))))
		assert.Equal(t, -1, keywordIndexFrom("XMATCH", "MATCH", 0, defaultKeywordScanOpts(acceptedBraceView("XMATCH"))))
		assert.Equal(t, -1, keywordIndexFrom("RETURNING", "RETURN", 0, defaultKeywordScanOpts(acceptedBraceView("RETURNING"))))
	})

	t.Run("quoted clause names retain their complete RETURN projection", func(t *testing.T) {
		for _, source := range []string{
			"RETURN 'RETURN, AS' AS text, [1, 2] AS values",
			"RETURN 'MATCH' AS text, {value: 'RETURN'} AS object",
			"RETURN 1 AS `MATCH`, 2 AS `RETURN`",
		} {
			ast, err := builder.Build(source)
			require.NoError(t, err)
			require.Len(t, ast.Clauses, 1, source)
			require.Equal(t, source, ast.Clauses[0].RawText)
			require.NotNil(t, ast.Clauses[0].Return)
			require.Len(t, ast.Clauses[0].Return.Items, 2)
		}
	})

	t.Run("parseMerge handles on create and on match branches", func(t *testing.T) {
		merge := builder.parseMerge(cyphertext.NormalizeSource("MERGE (n:Person {id: 1}) ON CREATE SET n.created = timestamp() ON MATCH SET n.updated = timestamp()"))
		require.Equal(t, "n", merge.Pattern.Nodes[0].Variable)
		require.Len(t, merge.OnCreate, 1)
		require.Len(t, merge.OnMatch, 1)
		assert.Equal(t, "created", merge.OnCreate[0].Property)
		assert.Equal(t, "updated", merge.OnMatch[0].Property)

		merge = builder.parseMerge(cyphertext.NormalizeSource("MERGE (n:Person {id: 1}) ON MATCH SET n.seen = true"))
		require.Empty(t, merge.OnCreate)
		require.Len(t, merge.OnMatch, 1)
	})

	t.Run("parseSetItems handles plus equals and variable-only assignments", func(t *testing.T) {
		items := builder.parseSetItems(cyphertext.NormalizeSource("n.count += 1, n = {name: 'Alice'}, ,"))
		require.Len(t, items, 2)
		assert.Equal(t, "n", items[0].Variable)
		assert.Equal(t, "count", items[0].Property)
		assert.Equal(t, "1", items[0].RawValue)
		assert.Equal(t, "n", items[1].Variable)
		assert.Equal(t, "", items[1].Property)
		assert.Equal(t, ASTExprMap, items[1].Value.Type)
		require.Contains(t, items[1].Value.Map, "name")
		assert.Equal(t, ASTExprLiteral, items[1].Value.Map["name"].Type)
		assert.Equal(t, "Alice", items[1].Value.Map["name"].Literal)
	})

	t.Run("parseRemove covers property and label removals", func(t *testing.T) {
		rem := builder.parseRemove(cyphertext.NormalizeSource("REMOVE n.legacy, n:Old:Archived, , m.extra"))
		require.Len(t, rem.Items, 3)
		assert.Equal(t, "n", rem.Items[0].Variable)
		assert.Equal(t, "legacy", rem.Items[0].Property)
		assert.Equal(t, []string{"Old", "Archived"}, rem.Items[1].Labels)
		assert.Equal(t, "m", rem.Items[2].Variable)
		assert.Equal(t, "extra", rem.Items[2].Property)
	})

	t.Run("parseReturn and parseWith handle distinct and aliases", func(t *testing.T) {
		ret := builder.parseClause(t.Context(), ASTClauseReturn, cyphertext.NormalizeSource("RETURN DISTINCT n.name AS name, count(*) AS total")).Return
		require.True(t, ret.Distinct)
		require.Len(t, ret.Items, 2)
		assert.Equal(t, "name", ret.Items[0].Alias)
		assert.Equal(t, "total", ret.Items[1].Alias)

		withClause := builder.parseClause(t.Context(), ASTClauseWith, cyphertext.NormalizeSource("WITH DISTINCT n.name AS name, n.age")).With
		require.True(t, withClause.Distinct)
		require.Len(t, withClause.Items, 2)
		assert.Equal(t, "name", withClause.Items[0].Alias)
		assert.Equal(t, "", withClause.Items[1].Alias)

		ret = builder.parseClause(t.Context(), ASTClauseReturn, cyphertext.NormalizeSource("RETURN {name: 'Alice', age: 1} AS profile, , n")).Return
		require.Len(t, ret.Items, 2)
		assert.Equal(t, ASTExprMap, ret.Items[0].Expression.Type)
		assert.Equal(t, "profile", ret.Items[0].Alias)
		assert.Equal(t, ASTExprVariable, ret.Items[1].Expression.Type)

		withClause = builder.parseClause(t.Context(), ASTClauseWith, cyphertext.NormalizeSource("WITH n, , {active: true} AS meta")).With
		require.Len(t, withClause.Items, 2)
		assert.Equal(t, ASTExprVariable, withClause.Items[0].Expression.Type)
		assert.Equal(t, ASTExprMap, withClause.Items[1].Expression.Type)
		assert.Equal(t, "meta", withClause.Items[1].Alias)
	})

	t.Run("parseOrderBy parseLimit parseSkip and parseCall cover edge branches", func(t *testing.T) {
		orderBy := builder.parseOrderBy(cyphertext.NormalizeSource("ORDER BY n.name ASC, n.age DESC, n.id, "))
		require.Len(t, orderBy.Items, 3)
		assert.False(t, orderBy.Items[0].Descending)
		assert.True(t, orderBy.Items[1].Descending)
		assert.False(t, orderBy.Items[2].Descending)

		orderBy = builder.parseOrderBy(cyphertext.NormalizeSource("ORDER BY , n.rank"))
		require.Len(t, orderBy.Items, 1)
		assert.Equal(t, ASTExprProperty, orderBy.Items[0].Expression.Type)

		limit := builder.parseClause(t.Context(), ASTClauseLimit, cyphertext.NormalizeSource("LIMIT nope")).Limit
		assert.Nil(t, limit)
		skip := builder.parseClause(t.Context(), ASTClauseSkip, cyphertext.NormalizeSource("SKIP nope")).Skip
		assert.Nil(t, skip)

		call := builder.parseCall(cyphertext.NormalizeSource("CALL db.labels"))
		assert.Equal(t, "db.labels", call.Procedure)
		assert.Empty(t, call.Yield)
	})

	t.Run("parseClause populates remove limit skip and passthrough union", func(t *testing.T) {
		clause := builder.parseClause(t.Context(), ASTClauseRemove, cyphertext.NormalizeSource("          "+"REMOVE n.legacy").Window(10, 10+len("REMOVE n.legacy")))
		require.NotNil(t, clause.Remove)
		assert.Equal(t, "legacy", clause.Remove.Items[0].Property)
		assert.Equal(t, 10+len("REMOVE n.legacy"), clause.EndPos)

		clause = builder.parseClause(t.Context(), ASTClauseLimit, cyphertext.NormalizeSource(""+"LIMIT 7").Window(0, 0+len("LIMIT 7")))
		require.NotNil(t, clause.Limit)
		assert.Equal(t, int64(7), *clause.Limit)

		clause = builder.parseClause(t.Context(), ASTClauseSkip, cyphertext.NormalizeSource(""+"SKIP 3").Window(0, 0+len("SKIP 3")))
		require.NotNil(t, clause.Skip)
		assert.Equal(t, int64(3), *clause.Skip)

		clause = builder.parseClause(t.Context(), ASTClauseUnion, cyphertext.NormalizeSource("     "+"UNION").Window(5, 5+len("UNION")))
		assert.Nil(t, clause.Call)
		assert.Nil(t, clause.Remove)
	})

	t.Run("splitOutsideBrackets respects strings and nested delimiters", func(t *testing.T) {
		parts := splitTopLevelComma(`a,'b,c',{x:[1,2]},func(1,2)`)
		assert.Equal(t, []string{"a", "'b,c'", "{x:[1,2]}", "func(1,2)"}, parts)
	})

	t.Run("parsePatterns and parseExpression cover nested literals", func(t *testing.T) {
		patterns := builder.parsePatterns(cyphertext.NormalizeSource("(a)-[r:KNOWS]->(b:Person {name: 'Alice'}), (), (c)"))
		require.Len(t, patterns, 3)
		require.Len(t, patterns[0].Nodes, 2)
		require.Len(t, patterns[0].Relationships, 1)
		assert.Equal(t, "r", patterns[0].Relationships[0].Variable)
		assert.Equal(t, "KNOWS", patterns[0].Relationships[0].Type)
		assert.Len(t, patterns[1].Nodes, 1)
		assert.Empty(t, patterns[1].Nodes[0].Variable)
		assert.Equal(t, "c", patterns[2].Nodes[0].Variable)

		patterns = builder.parsePatterns(cyphertext.NormalizeSource(", (n), , (m)"))
		require.Len(t, patterns, 2)
		assert.Equal(t, "n", patterns[0].Nodes[0].Variable)
		assert.Equal(t, "m", patterns[1].Nodes[0].Variable)

		expr := builder.parseExpression(cyphertext.NormalizeSource("{}"))
		assert.Equal(t, ASTExprMap, expr.Type)
		assert.Empty(t, expr.Map)

		expr = builder.parseExpression(cyphertext.NormalizeSource("[]"))
		assert.Equal(t, ASTExprList, expr.Type)
		assert.Empty(t, expr.List)

		expr = builder.parseExpression(cyphertext.NormalizeSource(`{"name": "Alice", nested: [1, null, false]}`))
		assert.Equal(t, ASTExprMap, expr.Type)
		require.Contains(t, expr.Map, "nested")
		assert.Equal(t, ASTExprList, expr.Map["nested"].Type)

		expr = builder.parseExpression(cyphertext.NormalizeSource(`{broken, ok: 1}`))
		// Malformed entries must decline the map shape, rather than
		// materializing only the valid siblings.
		assert.NotEqual(t, ASTExprMap, expr.Type)
		assert.Nil(t, expr.Map)

		expr = builder.parseExpression(cyphertext.NormalizeSource(`{first: 1, , second: 2}`))
		assert.NotEqual(t, ASTExprMap, expr.Type)
		assert.Nil(t, expr.Map)

		expr = builder.parseExpression(cyphertext.NormalizeSource(`"hello"`))
		assert.Equal(t, ASTExprLiteral, expr.Type)
		assert.Equal(t, "hello", expr.Literal)

		expr = builder.parseExpression(cyphertext.NormalizeSource("FALSE"))
		assert.Equal(t, ASTExprLiteral, expr.Type)
		assert.Equal(t, false, expr.Literal)

		expr = builder.parseExpression(cyphertext.NormalizeSource("sum(*)"))
		assert.Equal(t, ASTExprFunction, expr.Type)
		require.NotNil(t, expr.Function)
		assert.Empty(t, expr.Function.Arguments)

		expr = builder.parseExpression(cyphertext.NormalizeSource("sum("))
		assert.Equal(t, ASTExprVariable, expr.Type)
		assert.Equal(t, "sum(", expr.Variable)
	})

	t.Run("determineQueryType covers all write starters and defaults", func(t *testing.T) {
		assert.Equal(t, QueryMatch, builder.determineQueryType(&AST{}))
		assert.Equal(t, QueryCreate, builder.determineQueryType(&AST{Clauses: []ASTClause{{Type: ASTClauseCreate}}}))
		assert.Equal(t, QueryMerge, builder.determineQueryType(&AST{Clauses: []ASTClause{{Type: ASTClauseMerge}}}))
		assert.Equal(t, QueryDelete, builder.determineQueryType(&AST{Clauses: []ASTClause{{Type: ASTClauseDetachDelete}}}))
		assert.Equal(t, QuerySet, builder.determineQueryType(&AST{Clauses: []ASTClause{{Type: ASTClauseSet}}}))
		assert.Equal(t, QueryMatch, builder.determineQueryType(&AST{Clauses: []ASTClause{{Type: ASTClauseCall}}}))
	})
}
