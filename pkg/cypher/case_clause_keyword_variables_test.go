package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A clause keyword, as or is is a variable inside a CASE wherever an operand
// stands (CASE match WHEN 1 THEN match, THEN as, ELSE is, CASE case WHEN 1),
// and a variable named case stays one after AS. Nested CASE expressions are
// controls. Expected values are Neo4j 5.26.30's (#1162).
func TestCaseReadsClauseKeywordVariables(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_clause_keywords"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"WITH 1 AS case RETURN [CASE case WHEN 1 THEN 'a(' + toString([1][0]) ELSE 'b' END] AS v":            []interface{}{"a(1"},
		"WITH true AS a RETURN (CASE CASE WHEN a THEN 1 END WHEN 1 THEN 'one' END) AS v":                     "one",
		"UNWIND [1, 2] AS case WITH case WHERE CASE WHEN case > 1 THEN true ELSE false END RETURN case AS r": int64(2),
		"UNWIND [1] AS case RETURN case":                                                                   int64(1),
		"WITH 1 AS AS RETURN CASE AS WHEN 1 THEN AS ELSE 0 END AS v":                                       int64(1),
		"WITH 1 AS CALL RETURN CASE CALL WHEN 1 THEN CALL ELSE 0 END AS v":                                 int64(1),
		"WITH 1 AS CASE RETURN CASE CASE WHEN 1 THEN CASE ELSE 0 END AS v":                                 int64(1),
		"WITH 1 AS CREATE RETURN CASE CREATE WHEN 1 THEN CREATE ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS DELETE RETURN CASE DELETE WHEN 1 THEN DELETE ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS FOREACH RETURN CASE FOREACH WHEN 1 THEN FOREACH ELSE 0 END AS v":                        int64(1),
		"WITH 1 AS IS RETURN CASE IS WHEN 1 THEN IS ELSE 0 END AS v":                                       int64(1),
		"WITH 1 AS IS RETURN [x IN [1, 2] | CASE WHEN x = IS THEN IS ELSE x END] AS v":                     []interface{}{int64(1), int64(2)},
		"WITH 1 AS LIMIT RETURN CASE LIMIT WHEN 1 THEN LIMIT ELSE 0 END AS v":                              int64(1),
		"WITH 1 AS MATCH RETURN CASE MATCH WHEN 1 THEN MATCH ELSE 0 END AS v":                              int64(1),
		"WITH 1 AS MERGE RETURN CASE MERGE WHEN 1 THEN MERGE ELSE 0 END AS v":                              int64(1),
		"WITH 1 AS REMOVE RETURN CASE REMOVE WHEN 1 THEN REMOVE ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS RETURN RETURN CASE RETURN WHEN 1 THEN RETURN ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS SET RETURN CASE SET WHEN 1 THEN SET ELSE 0 END AS v":                                    int64(1),
		"WITH 1 AS SKIP RETURN CASE SKIP WHEN 1 THEN SKIP ELSE 0 END AS v":                                 int64(1),
		"WITH 1 AS UNION RETURN CASE UNION WHEN 1 THEN UNION ELSE 0 END AS v":                              int64(1),
		"WITH 1 AS UNWIND RETURN CASE UNWIND WHEN 1 THEN UNWIND ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS WHERE RETURN [x IN [1, 2] | CASE WHEN x = WHERE THEN WHERE ELSE x END] AS v":            []interface{}{int64(1), int64(2)},
		"WITH 1 AS WITH RETURN CASE WITH WHEN 1 THEN WITH ELSE 0 END AS v":                                 int64(1),
		"WITH 1 AS as RETURN CASE as WHEN 1 THEN as ELSE 0 END AS v":                                       int64(1),
		"WITH 1 AS call RETURN CASE call WHEN 1 THEN call ELSE 0 END AS v":                                 int64(1),
		"WITH 1 AS case RETURN CASE case WHEN 1 THEN CASE WHEN case = 1 THEN 'n' END ELSE 'x' END AS v":    "n",
		"WITH 1 AS case RETURN CASE case WHEN 1 THEN case ELSE 0 END AS v":                                 int64(1),
		"WITH 1 AS case RETURN [case] AS v":                                                                []interface{}{int64(1)},
		"WITH 1 AS case RETURN case + 1 AS v":                                                              int64(2),
		"WITH 1 AS case RETURN case AS v":                                                                  int64(1),
		"WITH 1 AS case WITH case AS c RETURN c":                                                           int64(1),
		"WITH 1 AS create RETURN CASE create WHEN 1 THEN create ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS delete RETURN CASE delete WHEN 1 THEN delete ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS foreach RETURN CASE foreach WHEN 1 THEN foreach ELSE 0 END AS v":                        int64(1),
		"WITH 1 AS is RETURN CASE is WHEN 1 THEN is ELSE 0 END AS v":                                       int64(1),
		"WITH 1 AS is RETURN [x IN [1, 2] | CASE WHEN x = is THEN is ELSE x END] AS v":                     []interface{}{int64(1), int64(2)},
		"WITH 1 AS limit RETURN CASE limit WHEN 1 THEN limit ELSE 0 END AS v":                              int64(1),
		"WITH 1 AS match RETURN CASE WHEN match = 1 THEN 'a' END + 'b' AS v":                               "ab",
		"WITH 1 AS match RETURN CASE match WHEN 1 THEN match ELSE 0 END AS v":                              int64(1),
		"WITH 1 AS merge RETURN CASE merge WHEN 1 THEN merge ELSE 0 END AS v":                              int64(1),
		"WITH 1 AS remove RETURN CASE remove WHEN 1 THEN remove ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS return RETURN CASE return WHEN 1 THEN return ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS set RETURN CASE set WHEN 1 THEN set ELSE 0 END AS v":                                    int64(1),
		"WITH 1 AS skip RETURN CASE skip WHEN 1 THEN skip ELSE 0 END AS v":                                 int64(1),
		"WITH 1 AS union RETURN CASE union WHEN 1 THEN union ELSE 0 END AS v":                              int64(1),
		"WITH 1 AS unwind RETURN CASE unwind WHEN 1 THEN unwind ELSE 0 END AS v":                           int64(1),
		"WITH 1 AS where RETURN [x IN [1, 2] | CASE WHEN x = where THEN where ELSE x END] AS v":            []interface{}{int64(1), int64(2)},
		"WITH 1 AS with RETURN CASE with WHEN 1 THEN with ELSE 0 END AS v":                                 int64(1),
		"WITH 2 AS AS RETURN CASE WHEN AS > 1 AND AS < 3 THEN AS * 2 ELSE 0 - AS END AS v":                 int64(4),
		"WITH 2 AS AS RETURN CASE WHEN false THEN 0 ELSE AS END AS v":                                      int64(2),
		"WITH 2 AS AS RETURN CASE WHEN true THEN AS END AS v":                                              int64(2),
		"WITH 2 AS IS RETURN CASE WHEN false THEN 0 ELSE IS END AS v":                                      int64(2),
		"WITH 2 AS IS RETURN CASE WHEN true THEN IS END AS v":                                              int64(2),
		"WITH 2 AS as RETURN CASE WHEN as > 1 AND as < 3 THEN as * 2 ELSE 0 - as END AS v":                 int64(4),
		"WITH 2 AS as RETURN CASE WHEN false THEN 0 ELSE as END AS v":                                      int64(2),
		"WITH 2 AS as RETURN CASE WHEN true THEN as END AS v":                                              int64(2),
		"WITH 2 AS case RETURN CASE WHEN case > 1 THEN case END AS v":                                      int64(2),
		"WITH 2 AS is RETURN CASE WHEN false THEN 0 ELSE is END AS v":                                      int64(2),
		"WITH 2 AS is RETURN CASE WHEN true THEN is END AS v":                                              int64(2),
		"WITH true AS a RETURN CASE CASE WHEN a THEN 1 ELSE 2 END WHEN 1 THEN 'one' ELSE 'other' END AS v": "one",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}

// caseKeywordAt reads a CASE right after CASE by counting ENDs up to the
// bracket that encloses them: one END is a simple CASE over the variable
// case, two a nested CASE (#1162).
func TestCaseAfterCaseIsReadByItsEnds(t *testing.T) {
	require.False(t, caseKeywordAt("(CASE case WHEN 1 THEN case END)", 6, "CASE"))
	require.True(t, caseKeywordAt("(CASE CASE WHEN a THEN 1 END WHEN 1 THEN 2 END)", 6, "CASE"))
}

// A clause inside a subquery within a CASE still starts there, so a scanner
// of the whole text (the graph-access check) sees COLLECT { MATCH … }; and a
// variable named case takes a label test (case IS Person) (#1162).
func TestCaseKeywordRulesKeepTheirScope(t *testing.T) {
	require.True(t, statementAccessesGraphText("RETURN CASE WHEN true THEN COLLECT { MATCH (n) RETURN n } ELSE [] END AS v"))
	require.False(t, insideCaseBlock("RETURN CASE WHEN true THEN COLLECT { MATCH (n) RETURN n } ELSE [] END AS v", len("RETURN CASE WHEN true THEN COLLECT { ")))
	require.True(t, insideCaseBlock("RETURN CASE match WHEN 1 THEN match END AS v", len("RETURN CASE ")))

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_label_test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Person)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (n:Person) WITH n AS case RETURN case IS Person AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}
