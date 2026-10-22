package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every responsibility inherited from the retired CALL readers has a public
// contract here. These cases also extend the generated Neo4j comparison matrix.
func callScopeResponsibilityCases() []callScopeOracleCase {
	var cases []callScopeOracleCase
	add := func(name, query string) {
		cases = append(cases, callScopeOracleCase{name: "responsibility/" + name, query: query, reference: query})
	}
	for _, header := range []string{"CALL (i)", "CALL (*)", "CALL"} {
		prefix := ""
		if header == "CALL" {
			prefix = "WITH i "
		}
		add("optional-filter/"+header, "UNWIND [1,2] AS i OPTIONAL "+header+" { "+prefix+"WITH i WHERE i>1 RETURN i AS j } RETURN i,j")
	}
	add("optional-unwind", "UNWIND [1,2] AS i OPTIONAL CALL (i) { UNWIND range(1,i) AS k RETURN k } RETURN i,k")
	add("optional-aggregate", "UNWIND [1,2] AS i OPTIONAL CALL (i) { RETURN count(*) AS c } RETURN i,c")
	for _, expression := range []string{"abs(-7)", "size('1234567')", "toInteger('7')", "coalesce(null,7)"} {
		add("computed-match/"+expression, "MATCH (node:CallScopeOracleTarget {n:"+expression+",oracleRun:$oracleRun}) CALL { WITH node RETURN node.n AS wanted } RETURN wanted")
	}
	add("return-names", "CALL { RETURN 1 AS duplicate,2 AS duplicate } RETURN duplicate")
	add("nested-return-names", "CALL { CALL { RETURN 1 AS duplicate,2 AS duplicate } RETURN duplicate } RETURN duplicate")
	add("quoted-return-names", "CALL { RETURN 1 AS `a a`,2 AS `a a` } RETURN `a a`")
	add("dynamic-return-names", "CALL { CALL db.labels() YIELD * RETURN 1 AS duplicate,2 AS duplicate } RETURN duplicate")
	add("dynamic-nested-return-names", "CALL { CALL db.labels() YIELD * CALL { RETURN 1 AS duplicate,2 AS duplicate } RETURN duplicate } RETURN duplicate")
	add("first-union-import-filter", "WITH 1 AS imported CALL { WITH imported WHERE imported>0 RETURN imported+1 AS v UNION WITH imported RETURN imported+2 AS v } RETURN v")
	for _, projection := range []string{"WITH imported WHERE imported>0 RETURN imported+2 AS v", "WITH imported AS renamed RETURN renamed+2 AS v"} {
		query := "WITH 1 AS imported CALL { WITH imported RETURN imported+1 AS v UNION " + projection + " } RETURN v ORDER BY v"
		cases = append(cases, callScopeOracleCase{name: "responsibility/later-union/" + projection, query: query, reference: query, compatibility: "WITH 1 AS imported CALL (imported) { WITH imported RETURN imported+1 AS v UNION " + projection + " } RETURN v ORDER BY v", extension: "907-later-union-import"})
	}
	add("quoted-reference", "WITH 1 AS `outer name` CALL (`outer name`) { RETURN `outer name`+1 AS value } RETURN value")
	add("string-is-not-reference", "CALL { RETURN 'not a variable: `missing`' AS value } RETURN value")
	return cases
}

func TestCallScopeResponsibilityContracts(t *testing.T) {
	for _, item := range callScopeResponsibilityCases() {
		t.Run(item.name, func(t *testing.T) {
			ctx := context.Background()
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "responsibilities"))
			params := map[string]any{"oracleRun": "responsibilities"}
			_, err := exec.Execute(ctx, "CREATE (:CallScopeOracleTarget {n:7,oracleRun:$oracleRun})", params)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, item.query, params)
			switch {
			case strings.HasSuffix(item.name, "return-names") || item.name == "responsibility/first-union-import-filter":
				requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
			default:
				require.NoError(t, err, item.query)
				require.NotNil(t, result)
				switch {
				case strings.HasPrefix(item.name, "responsibility/computed-match/"):
					require.EqualValues(t, [][]any{{int64(7)}}, result.Rows)
				case item.extension == "907-later-union-import":
					require.EqualValues(t, [][]any{{int64(2)}, {int64(3)}}, result.Rows)
				case strings.Contains(item.name, "optional-filter/"):
					require.EqualValues(t, [][]any{{int64(1), nil}, {int64(2), int64(2)}}, result.Rows)
				case item.name == "responsibility/optional-unwind":
					require.EqualValues(t, [][]any{{int64(1), int64(1)}, {int64(2), int64(1)}, {int64(2), int64(2)}}, result.Rows)
				case item.name == "responsibility/optional-aggregate":
					require.EqualValues(t, [][]any{{int64(1), int64(1)}, {int64(2), int64(1)}}, result.Rows)
				default:
					require.NotEmpty(t, result.Rows)
				}
			}
		})
	}
}

func TestCallScopePatternDeclarationContinuation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "declarations"))
	for _, pattern := range []string{"MATCH (node:CallScopeOracleTarget {n:abs(-7)})", "MATCH (node:CallScopeOracleTarget {n:$value})", "CREATE (node:CallScopeOracleTarget {n:abs(-7)})", "MERGE (node:CallScopeOracleTarget {n:abs(-7)})"} {
		t.Run(pattern, func(t *testing.T) {
			clauses, ok := splitPipelineClausesAllowingProcedureCalls(pattern + " CALL { WITH node RETURN node.n AS wanted } RETURN wanted")
			require.True(t, ok)
			types, complete, err := exec.semanticScopeWalk(clauses, typeScope{}, false, subqueryScope{})
			require.NoError(t, err, fmt.Sprint(clauses))
			require.True(t, complete)
			require.Contains(t, types, "node")
			require.Contains(t, types, "wanted")
		})
	}
	for _, clause := range []string{"MATCH (node)", "OPTIONAL MATCH (node)", "CREATE (node)", "MERGE (node)"} {
		require.True(t, branchDeclaresVariable(clause+" RETURN node", "node"), clause)
	}
}

func TestCallScopeQuotedImportBindings(t *testing.T) {
	for _, name := range []string{"outer name", "a`b", "RETURN", "π"} {
		quoted := "`" + strings.ReplaceAll(name, "`", "``") + "`"
		query := "WITH 1 AS " + quoted + " CALL (" + quoted + ") { RETURN " + quoted + "+1 AS value } RETURN value"
		for _, source := range []string{"public", "pipeline", "fabric", "context", "both"} {
			t.Run(name+"/"+source, func(t *testing.T) {
				ctx := context.Background()
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted-imports"))
				if source == "fabric" || source == "both" {
					exec.fabricRecordBindings = map[string]any{"input": int64(99)}
				}
				if source == "context" || source == "both" {
					ctx = withValueBindings(ctx, map[string]any{"input": int64(1)})
				}
				var result *ExecuteResult
				var err error
				if source == "public" {
					result, err = exec.Execute(ctx, query, nil)
				} else {
					outcome := exec.executePipeline(ctx, query)
					require.True(t, outcome.terminal())
					result, err = outcome.result, outcome.err
				}
				require.NoError(t, err)
				require.Equal(t, []string{"value"}, result.Columns)
				require.EqualValues(t, [][]any{{int64(2)}}, result.Rows)
			})
		}
	}
	for _, header := range []string{"1", "input+1", "input AS renamed"} {
		t.Run("invalid/"+header, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "invalid-imports"))
			outcome := exec.executePipeline(context.Background(), "WITH 1 AS input CALL ("+header+") { RETURN 2 AS value } RETURN value")
			require.True(t, outcome.terminal())
			requireStatusCode(t, outcome.err, "Neo.ClientError.Statement.SyntaxError")
		})
	}
}

// Binding identity and a written expression's column name are separate
// contracts. Exercise the same statement repeatedly to cover cached metadata.
func TestCallScopeReturnColumnSpelling(t *testing.T) {
	for _, source := range []string{"public", "pipeline", "fabric", "context", "both"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "column-spelling"))
			if source == "fabric" || source == "both" {
				exec.fabricRecordBindings = map[string]any{"input": int64(99)}
			}
			if source == "context" || source == "both" {
				ctx = withValueBindings(ctx, map[string]any{"input": int64(1)})
			}
			for _, item := range []struct {
				query   string
				columns []string
			}{
				{"WITH 1 AS `x` CALL (x) { RETURN x+1 AS value } RETURN x + 1, `x` + 1", []string{"x + 1", "`x` + 1"}},
				{"WITH 1 AS `x` CALL (x) { RETURN x+1 AS plain, `x`+1 AS quoted } RETURN plain,quoted", []string{"plain", "quoted"}},
				{"WITH 1 AS `x` CALL (x) { RETURN x+1 AS value, `x`+1 AS value } RETURN value", nil},
				{"CALL { CALL db.labels() YIELD * CALL { RETURN 1 AS `value`,2 AS value } RETURN value } RETURN value", nil},
			} {
				for repeat := 0; repeat < 2; repeat++ {
					var result *ExecuteResult
					var err error
					if source == "public" {
						result, err = exec.Execute(ctx, item.query, nil)
					} else {
						outcome := exec.executePipeline(ctx, item.query)
						require.True(t, outcome.terminal(), item.query)
						result, err = outcome.result, outcome.err
					}
					if item.columns == nil {
						requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
						continue
					}
					require.NoError(t, err, item.query)
					require.Equal(t, item.columns, result.Columns, item.query)
					require.EqualValues(t, [][]any{{int64(2), int64(2)}}, result.Rows, item.query)
				}
			}
		})
	}
}
