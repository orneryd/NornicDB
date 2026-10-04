package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGraphifyGentleUpdateDynamicParameterKeys(t *testing.T) {
	const body = `// CosineSimilarity calculates cosine similarity between two float32 vectors.
// Returns value in range [-1, 1] where 1 = identical, 0 = orthogonal, -1 = opposite.
//
// This is the STANDARD implementation for all non-GPU code.
// Uses float64 accumulation for high precision, even with float32 inputs.
//
// Example:
//
//	a := []float32{1.0, 2.0, 3.0}
//	b := []float32{4.0, 5.0, 6.0}
//	sim := CosineSimilarity(a, b)  // Returns 0.9746318461970762
func CosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dotProd, normA, normB float64
	for i := range a {
		dotProd += float64(a[i] * b[i])
		normA += float64(a[i] * a[i])
		normB += float64(b[i] * b[i])
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProd / (math.Sqrt(normA) * math.Sqrt(normB))
}`
	require.Len(t, body, 812)
	const nodeID = "pkg_math_vector_similarity_cosinesimilarity"
	const gentlePredicate = "NOT all(k IN keys(row.props) WHERE k = 'updated_at' OR n[k] = row.props[k])"
	tests := []struct {
		name        string
		body        string
		omitBody    bool
		predicate   string
		changeLabel bool
		returnOnly  bool
		wantRows    int
	}{
		{"full body unchanged", body, false, gentlePredicate, false, false, 0},
		{"full body changed label", body, false, gentlePredicate, true, false, 1},
		{"short body", "// short", false, gentlePredicate, false, false, 0},
		{"no body key", "", true, gentlePredicate, false, false, 0},
		{"positive parameter keys", body, false, "all(k IN keys(row.props) WHERE n[k] = row.props[k])", false, false, 1},
		{"static list", body, false, "all(k IN ['body'] WHERE n[k] = row.props.body)", false, false, 1},
		{"static map keys", body, false, "all(k IN keys({body: 'x'}) WHERE n[k] = row.props.body)", false, false, 1},
		{"direct comparison", body, false, "n.body = row.props.body", false, false, 1},
		{"RETURN quantifier", body, false, "", false, true, 1},
		{"unguarded update", body, false, "", true, false, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor, ctx := newUnitExecutor(t)
			original := map[string]interface{}{
				"id": nodeID, "label": "CosineSimilarity()", "file_type": "code",
				"source_file": "pkg/math/vector/similarity.go", "source_location": "L34",
				"body_start_line": int64(34), "updated_at": float64(1765762184.6631913),
			}
			if !test.omitBody {
				original["body"] = test.body
			}
			_, err := executor.Execute(ctx, "CREATE (n:Code) SET n += $props", map[string]interface{}{"props": original})
			require.NoError(t, err)
			incoming := make(map[string]interface{}, len(original))
			for key, value := range original {
				incoming[key] = value
			}
			if test.predicate == gentlePredicate {
				incoming["updated_at"] = float64(1765762185.6631913)
			}
			if test.changeLabel {
				incoming["label"] = "CosineSimilarity() updated"
			}
			query := "UNWIND $rows AS row MATCH (n:Code {id: row.id}) "
			if test.returnOnly {
				query += "RETURN all(k IN keys(row.props) WHERE k = 'updated_at' OR n[k] = row.props[k]) AS same"
			} else {
				if test.predicate != "" {
					query += "WHERE " + test.predicate + " "
				}
				query += "SET n += row.props RETURN 1 AS x"
			}
			result, err := executor.Execute(ctx, query, map[string]interface{}{
				"rows": []interface{}{map[string]interface{}{"id": nodeID, "props": incoming}},
			})
			require.NoError(t, err)
			require.Len(t, result.Rows, test.wantRows)
			if test.returnOnly {
				require.Equal(t, []string{"same"}, result.Columns)
				require.Equal(t, [][]interface{}{{true}}, result.Rows)
			} else {
				require.Equal(t, []string{"x"}, result.Columns)
				if test.wantRows != 0 {
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
				}
			}
			stored, err := executor.Execute(ctx, "MATCH (n:Code {id: $id}) RETURN properties(n) AS props", map[string]interface{}{"id": nodeID})
			require.NoError(t, err)
			require.Len(t, stored.Rows, 1)
			want := original
			if !test.returnOnly && test.wantRows != 0 {
				want = incoming
			}
			require.Equal(t, want, stored.Rows[0][0])
		})
	}
}

func TestPipelineQuantifierAcrossRepeatedHorizons(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	expressionRow := pipelineRow{
		"inputList": []interface{}{int64(1), int64(2), int64(3)},
		"list":      []interface{}{int64(1), int64(2)},
		"x":         int64(3),
	}
	_, evaluated := rowValue(t, exec, "rand()", expressionRow)
	require.True(t, evaluated)
	_, evaluated = rowValue(t, exec, "rand() < 0.5", expressionRow)
	require.True(t, evaluated)
	_, evaluated = rowValue(t, exec, "CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END", expressionRow)
	require.True(t, evaluated)
	value, evaluated := rowValue(t, exec, "CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x", expressionRow)
	require.True(t, evaluated)
	require.Len(t, value, 3)
	query := `
		WITH [1, 2, 3] AS inputList
		UNWIND inputList AS x
		WITH inputList, x, [y IN inputList WHERE rand() > 0.5 | y] AS list
		WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
		UNWIND inputList AS x
		WITH inputList, x, [y IN inputList WHERE rand() > 0.5 | y] AS list
		WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
		UNWIND inputList AS x
		WITH inputList, x, [y IN inputList WHERE rand() > 0.5 | y] AS list
		WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
		WITH list WHERE size(list) > 0
		WITH none(x IN list WHERE false) AS result, count(*) AS cnt
		RETURN result
	`
	clauses, parsed := canExecuteAsPipeline(query)
	require.True(t, parsed)
	rows := []pipelineRow{{}}
	for index, clause := range clauses {
		switch clause.kind {
		case pipelineClauseWith:
			var handled bool
			rows, handled = exec.pipelineApplyWith(ctx, rows, clause.text)
			require.True(t, handled, "WITH clause %d: %s", index, clause.text)
		case pipelineClauseUnwind:
			var handled bool
			rows, handled = exec.pipelineApplyUnwind(ctx, rows, clause.text)
			require.True(t, handled, "UNWIND clause %d: %s", index, clause.text)
		}
	}

	outcome := exec.executePipeline(ctx, query)
	require.NoError(t, outcome.err)
	require.True(t, outcome.handled())
	require.Equal(t, []string{"result"}, outcome.result.Columns)
	require.Equal(t, [][]interface{}{{true}}, outcome.result.Rows)
}
