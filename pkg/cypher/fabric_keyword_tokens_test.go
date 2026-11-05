package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Exercise lexical boundaries through the public composite execution path,
// including the outer continuation and prefixes synthesized by the planner.
func TestFabricKeywordTokens(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	ctx := context.Background()
	type testCase struct {
		name, query, want string
		params            map[string]interface{}
	}
	var cases []testCase
	for _, word := range []string{"union", "use", "call", "return", "with", "create", "merge", "delete", "set", "remove", "skip", "limit", "order", "match", "as", "and"} {
		cases = append(cases, testCase{"parameter_" + word, "USE comp CALL { USE comp.one RETURN $" + word + " AS k } RETURN k", "[[7]]", map[string]interface{}{word: int64(7)}})
		cases = append(cases, testCase{"prefix_parameter_" + word, "USE comp WITH $" + word + " AS x CALL { USE comp.one WITH x RETURN x AS k } RETURN k", "[[7]]", map[string]interface{}{word: int64(7)}})
		cases = append(cases, testCase{"identifier_" + word, "USE comp CALL { USE comp.one WITH 7 AS `" + word + "` RETURN `" + word + "` AS k } RETURN k", "[[7]]", nil})
	}
	for _, tc := range []testCase{
		{"literal", "USE comp CALL { USE comp.one RETURN 'UNION USE CALL { RETURN WITH CREATE SET }' AS k } RETURN k", `[["UNION USE CALL { RETURN WITH CREATE SET }"]]`, nil},
		{"escaped_literal", `USE comp CALL { USE comp.one RETURN 'a\' UNION USE CALL { RETURN WITH }' AS k } RETURN k`, `[["a' UNION USE CALL { RETURN WITH }"]]`, nil},
		{"call_identifier", "USE comp CALL { USE comp.one WITH 7 AS `CALL { USE comp.two RETURN 9 AS z }` RETURN `CALL { USE comp.two RETURN 9 AS z }` AS k } RETURN k", "[[7]]", nil},
		{"brace_identifier", "USE comp CALL { USE comp.one WITH 7 AS `} UNION USE CALL {` RETURN `} UNION USE CALL {` AS k } RETURN k", "[[7]]", nil},
		{"prefix_comment", "USE comp WITH 7 AS x /* RETURN WITH USE UNION CALL { } */ CALL { USE comp.one WITH x RETURN x AS k } RETURN k", "[[7]]", nil},
		{"import_comment", "USE comp WITH 7 AS x CALL { WITH x /* USE comp.two RETURN UNION CALL { } */ USE comp.one RETURN x AS k } RETURN k", "[[7]]", nil},
		{"call_comment", "USE comp CALL /* UNION USE RETURN WITH { } */ { USE comp.one RETURN 7 AS k } RETURN k", "[[7]]", nil},
		{"line_comment", "USE comp WITH 7 AS x // RETURN WITH USE UNION CALL { }\n CALL { USE comp.one WITH x RETURN x AS k } RETURN k", "[[7]]", nil},
		{"union_all", "USE comp CALL { USE comp.one RETURN $union AS k UNION /* USE CALL RETURN */ ALL USE comp.two RETURN $union AS k } RETURN count(*) AS c", "[[2]]", map[string]interface{}{"union": int64(7)}},
		{"union_distinct", "USE comp CALL { USE comp.one RETURN $union AS k UNION USE comp.two RETURN $union AS k } RETURN count(*) AS c", "[[1]]", map[string]interface{}{"union": int64(7)}},
		{"nested_union", "USE comp CALL { CALL { USE comp.one RETURN $union AS k UNION ALL USE comp.two RETURN 8 AS k } RETURN k } RETURN k ORDER BY k", "[[7],[8]]", map[string]interface{}{"union": int64(7)}},
	} {
		cases = append(cases, tc)
	}
	for _, tc := range cases {
		for _, mode := range []string{"autocommit", "transaction"} {
			t.Run(fmt.Sprintf("%s/%s", tc.name, mode), func(t *testing.T) {
				runExec := exec
				if mode == "transaction" {
					runExec = fabricCallRowsCompositeExecutor(t, exec)
					_, err := runExec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, err := runExec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
				}
				result, err := runExec.Execute(ctx, tc.query, tc.params)
				require.NoError(t, err)
				encoded, err := json.Marshal(result.Rows)
				require.NoError(t, err)
				require.JSONEq(t, tc.want, string(encoded))
			})
		}
	}
}
