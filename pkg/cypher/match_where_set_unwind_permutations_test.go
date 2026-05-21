package cypher

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchWhereSetUnwind_PermutationsAndStyles(t *testing.T) {
	type testCase struct {
		name         string
		style        string
		multiline    bool
		withProps    bool
		withFuncs    bool
		unwindParam  bool
		expectedRows int
	}

	var cases []testCase
	styles := []string{
		"standard",
		"aliasVar",
		"chainedSet",
		"whereAndParen",
		"mapMergeMixed",
		"wrappedUnwind",
	}
	for _, multiline := range []bool{false, true} {
		for _, style := range styles {
			for _, withProps := range []bool{false, true} {
				for _, withFuncs := range []bool{false, true} {
					for _, unwindParam := range []bool{false, true} {
						expectedRows := 3
						if !unwindParam && withProps && !withFuncs {
							// UNWIND tags where n has two tags in seed data.
							expectedRows = 2
						}
						cases = append(cases, testCase{
							name: fmt.Sprintf("style=%s,multiline=%t,props=%t,funcs=%t,unwindParam=%t",
								style, multiline, withProps, withFuncs, unwindParam),
							style:        style,
							multiline:    multiline,
							withProps:    withProps,
							withFuncs:    withFuncs,
							unwindParam:  unwindParam,
							expectedRows: expectedRows,
						})
					}
				}
			}
		}
	}

	for i, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			baseStore := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			// Seed data: one node should match, one should not.
			_, err := exec.Execute(ctx, `
CREATE (n:Person {id: 'p1', name: 'Alice', group: 'A', score: 10, tags: ['x','y']})
CREATE (n:Person {id: 'p2', name: 'Bob', group: 'B', score: 4, tags: ['z']})
`, nil)
			require.NoError(t, err)

			query := buildMatchWhereSetUnwindQuery(i, tc.style, tc.multiline, tc.withProps, tc.withFuncs, tc.unwindParam)
			styleName := fmt.Sprintf("case-%d", i)
			params := map[string]interface{}{
				"group":       "A",
				"targetName":  "alice",
				"targetExact": "Alice",
				"styleName":   styleName,
				"vals":        []interface{}{1, 2, 3},
				"mergeProps": map[string]interface{}{
					"kind":  "perm",
					"style": styleName,
				},
			}
			if !tc.withProps {
				params["mergeProps"] = map[string]interface{}{}
			}

			result, err := exec.Execute(ctx, query, params)
			if err != nil {
				t.Logf("permutor failed query:\n%s", query)
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			if len(result.Rows) != tc.expectedRows {
				t.Logf("permutor failed query:\n%s", query)
			}
			assert.Equal(t, tc.expectedRows, len(result.Rows), "query:\n%s", query)

			// Verify SET effects persisted for the matched node.
			verify, err := exec.Execute(ctx, `
MATCH (n:Person {id: 'p1'})
RETURN n.touched, n.kind, n.style, n.name_upper
`, nil)
			require.NoError(t, err)
			require.Len(t, verify.Rows, 1)
			assert.Equal(t, true, verify.Rows[0][0])

			if tc.withProps {
				assert.Equal(t, "perm", verify.Rows[0][1])
				assert.Equal(t, params["styleName"], verify.Rows[0][2])
			}
			if tc.withFuncs {
				assert.Equal(t, "ALICE", verify.Rows[0][3])
			}
		})
	}
}

func buildMatchWhereSetUnwindQuery(seed int, style string, multiline, withProps, withFuncs, unwindParam bool) string {
	nodeVar := "n"
	if style == "aliasVar" {
		nodeVar = "person"
	}

	matchClause := fmt.Sprintf("MATCH (%s:Person)", nodeVar)
	if withProps {
		matchClause = fmt.Sprintf("MATCH (%s:Person {group: $group})", nodeVar)
	}

	whereClause := fmt.Sprintf("WHERE %s.name = $targetExact", nodeVar)
	if withFuncs {
		whereClause = fmt.Sprintf("WHERE toLower(%s.name) = $targetName", nodeVar)
	}
	if style == "chainedSet" {
		whereClause = fmt.Sprintf("WHERE (%s.name = $targetExact)", nodeVar)
		if withFuncs {
			whereClause = fmt.Sprintf("WHERE (toLower(%s.name) = $targetName)", nodeVar)
		}
	} else if style == "whereAndParen" {
		whereClause = fmt.Sprintf("WHERE (%s.name = $targetExact AND %s.score >= 10)", nodeVar, nodeVar)
		if withFuncs {
			whereClause = fmt.Sprintf("WHERE (toLower(%s.name) = $targetName AND %s.score >= 10)", nodeVar, nodeVar)
		}
	}

	setParts := []string{fmt.Sprintf("%s.touched = true", nodeVar)}
	if withProps {
		setParts = append(setParts, fmt.Sprintf("%s.kind = 'perm'", nodeVar), fmt.Sprintf("%s.style = $styleName", nodeVar))
	}
	if withFuncs {
		setParts = append(setParts, fmt.Sprintf("%s.name_upper = toUpper(%s.name)", nodeVar, nodeVar))
	}
	setClause := "SET " + strings.Join(setParts, ", ")
	setClauseMultiline := "SET " + strings.Join(setParts, ",\n    ")
	if style == "chainedSet" {
		setClause = "SET " + strings.Join(setParts, " SET ")
		setClauseMultiline = "SET " + strings.Join(setParts, "\nSET ")
	} else if style == "mapMergeMixed" {
		setParts = []string{fmt.Sprintf("%s += $mergeProps", nodeVar), fmt.Sprintf("%s.touched = true", nodeVar)}
		if withFuncs {
			setParts = append(setParts, fmt.Sprintf("%s.name_upper = toUpper(%s.name)", nodeVar, nodeVar))
		}
		setClause = "SET " + strings.Join(setParts, ", ")
		setClauseMultiline = "SET " + strings.Join(setParts, ",\n    ")
	}

	unwindClause := "UNWIND [1, 2, 3] AS item"
	switch {
	case unwindParam:
		unwindClause = "UNWIND $vals AS item"
	case withFuncs:
		// Function call style in UNWIND.
		unwindClause = "UNWIND range(1, 3) AS item"
	case withProps:
		unwindClause = fmt.Sprintf("UNWIND %s.tags AS item", nodeVar)
	}
	if style == "wrappedUnwind" {
		unwindClause = strings.Replace(unwindClause, " AS ", ") AS ", 1)
		unwindClause = strings.Replace(unwindClause, "UNWIND ", "UNWIND (", 1)
	}

	returnClause := fmt.Sprintf("RETURN %s.id AS id, item, %s.touched AS touched", nodeVar, nodeVar)
	if style == "aliasVar" {
		returnClause = fmt.Sprintf("RETURN item AS item, %s.id AS id, %s.touched AS touched", nodeVar, nodeVar)
	}

	if !multiline {
		query := strings.Join([]string{matchClause, whereClause, setClause, unwindClause, returnClause}, " ")
		return injectArbitraryWhitespace(query, seed)
	}

	query := strings.Join([]string{
		matchClause,
		whereClause,
		setClauseMultiline,
		unwindClause,
		returnClause,
	}, "\n")
	return injectArbitraryWhitespace(query, seed)
}

func injectArbitraryWhitespace(query string, seed int) string {
	r := rand.New(rand.NewSource(int64(7919*seed + 17)))
	var b strings.Builder
	inSingle := false
	inDouble := false

	for i := 0; i < len(query); {
		ch := query[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
			b.WriteByte(ch)
			i++
			continue
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
			b.WriteByte(ch)
			i++
			continue
		}

		if !inSingle && !inDouble && (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') {
			for i < len(query) {
				if query[i] != ' ' && query[i] != '\t' && query[i] != '\n' && query[i] != '\r' {
					break
				}
				i++
			}
			b.WriteString(randomValidWhitespace(r))
			continue
		}

		b.WriteByte(ch)
		i++
	}

	return strings.TrimSpace(b.String())
}

func randomValidWhitespace(r *rand.Rand) string {
	choices := []string{
		" ",
		"  ",
		"\t",
		" \t ",
		" \n ",
		"\n",
		"\n\t",
		"\t\n ",
	}
	return choices[r.Intn(len(choices))]
}

func TestMatchWhereSetUnwind_SpecificFailedQueryRegression(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (n:Person {id: 'p1', name: 'Alice', group: 'A', score: 10, tags: ['x','y']})
CREATE (n:Person {id: 'p2', name: 'Bob', group: 'B', score: 4, tags: ['z']})
`, nil)
	require.NoError(t, err)

	query := "MATCH (n:Person) WHERE n.name = $targetExact SET n.touched = true UNWIND [1, 2, 3] AS item RETURN n.id AS id, item, n.touched AS touched"
	params := map[string]interface{}{
		"targetExact": "Alice",
	}

	result, err := exec.Execute(ctx, query, params)
	if err != nil {
		t.Logf("specific failed query:\n%s", query)
	}
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 3)

	for _, row := range result.Rows {
		require.Len(t, row, 3)
		assert.Equal(t, "p1", row[0])
		assert.Equal(t, true, row[2])
	}

	verify, err := exec.Execute(ctx, "MATCH (n:Person {id: 'p1'}) RETURN n.touched", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	assert.Equal(t, true, verify.Rows[0][0])
}

func TestMatchWhereSetUnwind_MultilineWhereAllPermutations(t *testing.T) {
	type whereCase struct {
		name       string
		whereBlock string
	}

	var cases []whereCase
	for _, withFuncs := range []bool{false, true} {
		clauses := buildAllMultilineWherePermutations("n", withFuncs)
		for i, clause := range clauses {
			cases = append(cases, whereCase{
				name:       fmt.Sprintf("funcs=%t,wherePermutation=%d", withFuncs, i),
				whereBlock: clause,
			})
		}
	}

	for i, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			baseStore := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(baseStore, "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()

			_, err := exec.Execute(ctx, `
CREATE (n:Person {id: 'p1', name: 'Alice', group: 'A', score: 10, tags: ['x','y']})
CREATE (n:Person {id: 'p2', name: 'Bob', group: 'B', score: 4, tags: ['z']})
`, nil)
			require.NoError(t, err)

			query := strings.Join([]string{
				"MATCH (n:Person)",
				tc.whereBlock,
				"SET n.touched = true",
				"UNWIND [1, 2, 3] AS item",
				"RETURN n.id AS id, item, n.touched AS touched",
			}, "\n")
			query = injectArbitraryWhitespace(query, 90000+i)

			params := map[string]interface{}{
				"targetExact": "Alice",
				"targetName":  "alice",
				"group":       "A",
				"minScore":    10,
			}

			result, err := exec.Execute(ctx, query, params)
			if err != nil {
				t.Logf("where permutation failed query:\n%s", query)
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Len(t, result.Rows, 3, "query:\n%s", query)

			for _, row := range result.Rows {
				require.Len(t, row, 3)
				assert.Equal(t, "p1", row[0], "query:\n%s", query)
				assert.Equal(t, true, row[2], "query:\n%s", query)
			}

			verify, err := exec.Execute(ctx, `
MATCH (n:Person)
RETURN n.id, n.touched
`, nil)
			require.NoError(t, err)
			require.Len(t, verify.Rows, 2)

			var p1Touched interface{}
			var p2Touched interface{}
			for _, row := range verify.Rows {
				if row[0] == "p1" {
					p1Touched = row[1]
				}
				if row[0] == "p2" {
					p2Touched = row[1]
				}
			}
			assert.Equal(t, true, p1Touched)
			assert.Nil(t, p2Touched)
		})
	}
}

func buildAllMultilineWherePermutations(nodeVar string, withFuncs bool) []string {
	namePredicate := fmt.Sprintf("%s.name = $targetExact", nodeVar)
	if withFuncs {
		namePredicate = fmt.Sprintf("toLower(%s.name) = $targetName", nodeVar)
	}

	terms := []string{
		namePredicate,
		fmt.Sprintf("%s.group = $group", nodeVar),
		fmt.Sprintf("%s.score >= $minScore", nodeVar),
	}

	orderings := permuteStrings(terms)
	clauses := make([]string, 0, len(orderings)*3)
	for _, ord := range orderings {
		a, b, c := ord[0], ord[1], ord[2]

		// Flat conjunction.
		clauses = append(clauses, strings.Join([]string{
			"WHERE " + a,
			"  AND " + b,
			"  AND " + c,
		}, "\n"))

		// Left-associated grouping.
		clauses = append(clauses, strings.Join([]string{
			"WHERE (" + a,
			"  AND " + b + ")",
			"  AND " + c,
		}, "\n"))

		// Right-associated grouping.
		clauses = append(clauses, strings.Join([]string{
			"WHERE " + a,
			"  AND (" + b,
			"  AND " + c + ")",
		}, "\n"))
	}

	return clauses
}

func permuteStrings(items []string) [][]string {
	if len(items) == 0 {
		return [][]string{{}}
	}
	first := items[0]
	restPerms := permuteStrings(items[1:])
	out := make([][]string, 0, factorial(len(items)))
	for _, rp := range restPerms {
		for i := 0; i <= len(rp); i++ {
			next := make([]string, 0, len(items))
			next = append(next, rp[:i]...)
			next = append(next, first)
			next = append(next, rp[i:]...)
			out = append(out, next)
		}
	}
	return out
}

func factorial(n int) int {
	if n <= 1 {
		return 1
	}
	return n * factorial(n-1)
}

func TestEvaluateWithWhereCondition_DirectBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	vals := map[string]interface{}{
		"n":      int64(10),
		"exists": "x",
		"nilVal": nil,
	}
	ctx := context.Background()

	assert.True(t, exec.evaluateWithWhereCondition(ctx, "exists IS NOT NULL", vals))
	assert.False(t, exec.evaluateWithWhereCondition(ctx, "nilVal IS NOT NULL", vals))
	assert.True(t, exec.evaluateWithWhereCondition(ctx, "nilVal IS NULL", vals))
	assert.True(t, exec.evaluateWithWhereCondition(ctx, "missing IS NULL", vals))

	assert.True(t, exec.evaluateWithWhereCondition(ctx, "n >= 10", vals))
	assert.True(t, exec.evaluateWithWhereCondition(ctx, "n <= 10", vals))
	assert.True(t, exec.evaluateWithWhereCondition(ctx, "n = 10", vals))
	assert.True(t, exec.evaluateWithWhereCondition(ctx, "n != 11", vals))
	assert.True(t, exec.evaluateWithWhereCondition(ctx, "n > 9", vals))
	assert.True(t, exec.evaluateWithWhereCondition(ctx, "n < 11", vals))

	// Unknown pattern defaults to pass-through.
	assert.True(t, exec.evaluateWithWhereCondition(ctx, "totally_unknown_condition", vals))
}
