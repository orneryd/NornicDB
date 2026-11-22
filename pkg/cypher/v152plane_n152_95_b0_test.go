package cypher

// NOR-66 / NOR-167 public map-literal matrix.
//
// NOR-66: a malformed map literal (an empty entry, an entry without a key or a
// value, or a missing key/value separator) is a SyntaxError/UnexpectedSyntax
// before any row executes, on both parser frontends. A statement that stages a
// write and then contains one must publish nothing.
//
// NOR-167: a backtick-quoted map key decodes with the same accepted-source
// owner that decodes an identifier, so `a\u0062` is the key ab on both parsers
// and in both evaluation owners (the text evaluator and the compiled row value
// program). Report-only for the pinned Neo4j 5.26 reference: no reference
// server is contacted here.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	norMapAutocommitMode = "autocommit"
	norMapExplicitMode   = "explicit-transaction"
)

// norMapModes is the transaction matrix the release corpus reports. The
// explicit mode is the executor's BEGIN … COMMIT state, the same state the
// Bolt transactional adapter drives.
func norMapModes() []string {
	return []string{norMapAutocommitMode, norMapExplicitMode}
}

// norMapRow is the one-column result of a RETURN map projection.
func norMapRow(value map[string]interface{}) [][]interface{} {
	return [][]interface{}{{value}}
}

// norMapKeyAliasRows is the result of a statement that returns the map and
// projects the same written name as an identifier alias.
func norMapKeyAliasRows(key string) [][]interface{} {
	return [][]interface{}{{map[string]interface{}{key: int64(1)}, int64(1)}}
}

// norMapExecuteInMode runs one statement in the named mode on exec. A failed
// explicit-transaction statement is rolled back and the transaction is
// required to be gone before the caller inspects the store.
func norMapExecuteInMode(t *testing.T, ctx context.Context, exec *StorageExecutor, mode, query string) (*ExecuteResult, error) {
	t.Helper()
	if mode == norMapAutocommitMode {
		return exec.Execute(ctx, query, nil)
	}
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	result, execErr := exec.Execute(ctx, query, nil)
	if execErr != nil {
		_, rollbackErr := exec.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, rollbackErr)
		require.False(t, exec.HasActiveTransaction())
		return result, execErr
	}
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	require.False(t, exec.HasActiveTransaction())
	return result, nil
}

// TestNOR66MalformedMapClassification pins the exact compile-time
// classification of every recorded malformed map shape on the public parser
// frontends and the public executor in both transaction modes.
func TestNOR66MalformedMapClassification(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		ctx := context.Background()
		for _, form := range []struct{ name, query string }{
			// Entries without keys or values.
			{"literal_trailing_comma", "RETURN {key: 1,} AS m"},
			{"literal_leading_comma", "RETURN {, key: 1} AS m"},
			{"literal_doubled_comma", "RETURN {key: 1,, other: 2} AS m"},
			{"literal_comma_only", "RETURN {,} AS m"},
			{"literal_value_without_key", "RETURN {1} AS m"},
			{"literal_list_without_key", "RETURN {[]} AS m"},
			{"literal_map_without_key", "RETURN {{}} AS m"},
			{"literal_entry_without_key", "RETURN {key: 1, : 2} AS m"},
			{"literal_entry_without_value", "RETURN {key:} AS m"},
			{"literal_bare_entry", "RETURN {key: 1, other} AS m"},
			{"literal_missing_separator", "RETURN {key 1} AS m"},
			// Nested and projection shapes share the same entry boundary.
			{"nested_literal_trailing_comma", "RETURN {outer: {inner: 1,}} AS m"},
			{"projection_comma_only", "WITH {a: 1} AS n RETURN n{,} AS m"},
			{"projection_trailing_comma", "WITH {a: 1} AS n RETURN n{.a,} AS m"},
			{"projection_all_trailing_comma", "WITH {a: 1} AS n RETURN n{.*, } AS m"},
			{"projection_doubled_comma", "WITH {a: 1} AS n RETURN n{.*, , a} AS m"},
		} {
			t.Run(form.name, func(t *testing.T) {
				_, err := NewParser().Parse(t.Context(), form.query)
				if err != nil {
					requireMatchSemanticDetail(t, err, "UnexpectedSyntax")
				} else {
					t.Log("parser defers semantic map-entry classification to execution admission")
				}
				_, err = NewASTBuilder().Build(t.Context(), form.query)
				if err != nil {
					requireMatchSemanticDetail(t, err, "UnexpectedSyntax")
				} else {
					t.Log("AST builder defers semantic map-entry classification to execution admission")
				}
				for _, mode := range norMapModes() {
					t.Run(mode, func(t *testing.T) {
						exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor66_classification"))
						_, err := norMapExecuteInMode(t, ctx, exec, mode, form.query)
						requireMatchSemanticDetail(t, err, "UnexpectedSyntax")
					})
				}
			})
		}
	})
}

// TestNOR167EscapedVariableHeadersExecute checks the three exact variable
// header statements through Execute on both parser frontends.
func TestNOR167EscapedVariableHeadersExecute(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor167_headers"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
		for _, tc := range []struct {
			query   string
			columns []string
			rows    [][]interface{}
		}{
			{"UNWIND ['value'] AS `ab` RETURN `ab`", []string{"ab"}, [][]interface{}{{"value"}}},
			{"UNWIND [] AS `ab` RETURN `ab`", []string{"ab"}, nil},
			{"RETURN 1 AS `a\\u0062`", []string{"ab"}, [][]interface{}{{int64(1)}}},
		} {
			result, err := exec.Execute(t.Context(), tc.query, nil)
			require.NoError(t, err, tc.query)
			require.Equal(t, tc.columns, result.Columns, tc.query)
			if tc.rows == nil {
				require.Empty(t, result.Rows, tc.query)
			} else {
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
			t.Logf("statement=%q columns=%v rows=%v", tc.query, result.Columns, result.Rows)
		}
	})
}

// TestNOR66MalformedMapStageWriteDoesNotPublish proves the compile-time
// rejection runs before execution: each statement stages a write before the
// malformed map and leaves no node or relationship behind.
func TestNOR66MalformedMapStageWriteDoesNotPublish(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		ctx := context.Background()
		for _, form := range []struct {
			name, query  string
			relationship bool
		}{
			{"create_then_return_map", "CREATE (:Nor66Stage {id: 1}) RETURN {key: 1,} AS m", false},
			{"merge_then_set_map", "MERGE (n:Nor66Stage {id: 2}) SET n += {key: 1,}", false},
			{"create_pattern_property_map", "CREATE (a:Nor66Stage {id: 3})-[:NOR66_STAGE]->(b:Nor66Stage {key: 1,})", true},
			{"write_then_projection", "WITH {key: 1} AS n CREATE (:Nor66Stage {id: 4}) RETURN n{.key,} AS m", false},
		} {
			t.Run(form.name, func(t *testing.T) {
				for _, mode := range norMapModes() {
					t.Run(mode, func(t *testing.T) {
						store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor66_stage")
						exec := NewStorageExecutor(store)
						_, err := norMapExecuteInMode(t, ctx, exec, mode, form.query)
						requireMatchSemanticDetail(t, err, "UnexpectedSyntax")

						reader := NewStorageExecutor(store)
						result, err := reader.Execute(ctx, "MATCH (n:Nor66Stage) RETURN count(n) AS c", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
						if form.relationship {
							result, err = reader.Execute(ctx, "MATCH ()-[r:NOR66_STAGE]->() RETURN count(r) AS c", nil)
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
						}
					})
				}
			})
		}
	})
}

// TestNOR66ValidMapAndProjectionControls keeps the healthy boundary next to
// the malformed matrix: empty, nested, quoted, opaque and comment text, and
// map projections remain accepted with their existing results.
func TestNOR66ValidMapAndProjectionControls(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		ctx := context.Background()
		for _, tc := range []struct {
			name, query string
			want        [][]interface{}
		}{
			{"empty_map", "RETURN {} AS m", norMapRow(map[string]interface{}{})},
			{"ordinary_map", "RETURN {key: 1} AS m", norMapRow(map[string]interface{}{"key": int64(1)})},
			{"nested_map", "RETURN {outer: {inner: {deep: 1}}} AS m", norMapRow(map[string]interface{}{
				"outer": map[string]interface{}{"inner": map[string]interface{}{"deep": int64(1)}},
			})},
			{"comment_with_delimiters", "RETURN {key: 1 /* ,} */} AS m", norMapRow(map[string]interface{}{"key": int64(1)})},
			{"quoted_delimiters", `RETURN {key: 'a,}b', other: "c{d}e"} AS m`, norMapRow(map[string]interface{}{
				"key": "a,}b", "other": "c{d}e",
			})},
			{"quoted_key_delimiters", "RETURN {`a,}b`: 2} AS m", norMapRow(map[string]interface{}{"a,}b": int64(2)})},
			{"projection_selectors", "WITH {a: 1, b: 2} AS n RETURN n{.a, .b} AS m", norMapRow(map[string]interface{}{
				"a": int64(1), "b": int64(2),
			})},
			{"projection_all", "WITH {a: 1} AS n RETURN n{.*} AS m", norMapRow(map[string]interface{}{"a": int64(1)})},
			{"projection_empty", "WITH {a: 1} AS n RETURN n{} AS m", norMapRow(map[string]interface{}{})},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, mode := range norMapModes() {
					t.Run(mode, func(t *testing.T) {
						exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor66_controls"))
						result, err := norMapExecuteInMode(t, ctx, exec, mode, tc.query)
						require.NoError(t, err, tc.query)
						require.Equal(t, tc.want, result.Rows, tc.query)
					})
				}
			})
		}
	})
}

// nor167EscapedMapKeyCases are the reported expression and the
// reference-supported escaped-name forms. written is Cypher source; key is the
// decoded name both the map key and an identifier must produce.
func nor167EscapedMapKeyCases() []struct{ name, written, key string } {
	return []struct{ name, written, key string }{
		{"reported_public_expression", "`a\\u0062`", "ab"},
		{"uppercase_escape", "`A\\u0042`", "AB"},
		{"accented_escape", "`caf\\u00e9`", "café"},
		{"surrogate_pair", "`a\\uD83D\\uDE00b`", "a😀b"},
		{"escaped_backslash_before_text", "`a\\\\u0062`", "a\\\\u0062"},
		{"unicode_backslash_then_text", "`a\\u005cu0062`", "a\\u0062"},
		{"doubled_backtick", "`a``b`", "a`b"},
		{"unquoted_key", "abc", "abc"},
		{"quoted_spaces", "`a b`", "a b"},
	}
}

// TestNOR167EscapedMapKeyDecoding executes the reported map expression and the
// supported escape variants on both parsers and both transaction modes. Each
// statement also projects the same written name as an identifier alias, so the
// map key, the identifier column and their expected decoded value are checked
// in one accepted source.
func TestNOR167EscapedMapKeyDecoding(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		ctx := context.Background()
		for _, tc := range nor167EscapedMapKeyCases() {
			t.Run(tc.name, func(t *testing.T) {
				query := "RETURN {" + tc.written + ": 1} AS m, 1 AS " + tc.written
				for _, mode := range norMapModes() {
					t.Run(mode, func(t *testing.T) {
						exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor167"))
						result, err := norMapExecuteInMode(t, ctx, exec, mode, query)
						require.NoError(t, err, query)
						assert.Equal(t, []string{"m", tc.key}, result.Columns, query)
						require.Equal(t, norMapKeyAliasRows(tc.key), result.Rows, query)
					})
				}
			})
		}
	})
}

// TestNOR167EscapedMapKeyEvaluatorOwners checks the decoded key against the
// shared identifier owner and in the text evaluator, the compiled row value
// program and the literal plan. Evaluator selection is parser-independent, so
// this test runs once per source case.
func TestNOR167EscapedMapKeyEvaluatorOwners(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor167_evaluators"))
	for _, tc := range nor167EscapedMapKeyCases() {
		t.Run(tc.name, func(t *testing.T) {
			nameSource := cyphertext.NewSource(t.Context(), tc.written, cyphertext.RawSource)
			require.NoError(t, nameSource.Err)
			identifier, ok := cyphertext.ScanIdentifier(nameSource.Text, 0)
			require.True(t, ok)
			require.Equal(t, len(nameSource.Text), identifier.End)
			require.Equal(t, tc.key, identifier.Value, "shared identifier owner")

			literalSource := cyphertext.NewSource(t.Context(), "{"+tc.written+": 1}", cyphertext.RawSource)
			require.NoError(t, literalSource.Err)
			want := map[string]interface{}{tc.key: int64(1)}

			textValue, resolved, err := exec.evaluateRowValueText(t.Context(), literalSource.Text, nil)
			require.NoError(t, err)
			require.True(t, resolved)
			require.Equal(t, want, textValue, "text evaluator")

			root, _ := (&rowValueCompiler{}).compile(t.Context(), expressionPart(literalSource.Text))
			compiledValue, resolved, err := root.eval(t.Context(), exec, nil, nil)
			require.NoError(t, err)
			require.True(t, resolved)
			require.Equal(t, want, compiledValue, "compiled row value program")

			literalValue, literal := parseLiteralValueFromComputedRow(literalSource.Text)
			if literal {
				require.Equal(t, want, literalValue, "literal plan")
			} else {
				t.Log("literal-only reader delegates map expressions to the already-tested expression owners")
			}
		})
	}
}

// TestNOR66SharedValidationUnderQueryNormalization keeps the accepted-source
// owner observable: whitespace and format-character normalization of the
// original text does not change the malformed-map classification or the
// escaped-key result, with query normalization on and off.
func TestNOR66SharedValidationUnderQueryNormalization(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor66_normalization"))
		for _, query := range []string{
			"RETURN {key: 1,} AS m",
			"RETURN {key: 1,\u00a0} AS m",
			"RETURN {key: 1,\u200b} AS m",
		} {
			_, err := exec.Execute(t.Context(), query, nil)
			requireMatchSemanticDetail(t, err, "UnexpectedSyntax")
		}
		result, err := exec.Execute(t.Context(), "RETURN {`a\\u0062`: 1} AS m", nil)
		require.NoError(t, err)
		require.Equal(t, norMapRow(map[string]interface{}{"ab": int64(1)}), result.Rows)
	})
}

// Malformed escaped names must fail in both transaction modes and never be
// installed as literal map keys. The accepted-source public regression already
// defines these invalid forms; this adds the missing transactional matrix.
func TestNOR167InvalidMapKeyEscapes(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, written := range []string{"`a\\uZZZZ`", "`a\\u006`", "`a\\u0060b`"} {
			for _, mode := range norMapModes() {
				t.Run(written+"/"+mode, func(t *testing.T) {
					exec := NewStorageExecutor(newTestMemoryEngine(t))
					_, err := norMapExecuteInMode(t, t.Context(), exec, mode, "RETURN {"+written+":1} AS m")
					require.Error(t, err)
				})
			}
		}
	})
}
