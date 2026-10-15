package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNestedCaseKeywordVariables(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        interface{}
	}{
		{"not_nested", "WITH 1 AS not RETURN CASE WHEN true THEN CASE WHEN true THEN not END ELSE 0 END", int64(1)},
		{"distinct_nested", "WITH 2 AS distinct RETURN CASE WHEN true THEN CASE WHEN true THEN distinct END ELSE 0 END", int64(2)},
		{"not_operator_end_variable", "WITH false AS end RETURN CASE WHEN true THEN NOT end END", true},
		{"not_operator_nested_end_variable", "WITH false AS end RETURN CASE WHEN true THEN CASE WHEN true THEN NOT end END ELSE false END", true},
		{"bound_case_end", "WITH 1 AS case, 2 AS end RETURN CASE case WHEN 1 THEN CASE end WHEN 2 THEN end ELSE case END ELSE 0 END", int64(2)},
		{"case_and_end_property", "WITH {case: 1, end: 2} AS m RETURN CASE m.case WHEN 1 THEN CASE m.end WHEN 2 THEN m.end ELSE 0 END ELSE 0 END", int64(2)},
		{"quoted_end", "WITH 1 AS not RETURN CASE WHEN true THEN CASE WHEN 'END CASE' = 'END CASE' THEN not END ELSE 0 END", int64(1)},
		{"commented_end", "WITH 1 AS not RETURN CASE WHEN true THEN CASE WHEN true /* END CASE */ THEN not END ELSE 0 END", int64(1)},
		{"nested_not_condition", "WITH false AS end, 1 AS not RETURN CASE WHEN true THEN CASE WHEN NOT end THEN not END ELSE 0 END", int64(1)},
		{"nested_boolean_not_condition", "WITH false AS end, 1 AS not RETURN CASE WHEN true THEN CASE WHEN true AND NOT end THEN not END ELSE 0 END", int64(1)},
		{"nested_comparison_not_condition", "WITH false AS end, 1 AS not RETURN CASE WHEN true THEN CASE WHEN true = (NOT end) THEN not END ELSE 0 END", int64(1)},
		{"property_not_before_end", "WITH {not: 2} AS m, 1 AS not RETURN CASE WHEN true THEN CASE WHEN false THEN m.not ELSE not END ELSE 0 END", int64(1)},
		{"distinct_other_branch", "WITH 1 AS not, 2 AS distinct RETURN CASE WHEN true THEN CASE WHEN false THEN distinct ELSE not END ELSE 0 END", int64(1)},
		{"nested_suffix", "WITH 1 AS not RETURN CASE WHEN true THEN CASE WHEN true THEN not END ELSE 0 END + 2", int64(3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nested_case"))
			result, err := exec.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
		})
	}
}

// Ambiguous END readers must ignore quoted/comment text and property names;
// malformed excess closers cannot establish that an operand consumes an END.
func TestCaseClosureBalanceLexicalBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		required   bool
	}{
		{"block_comment", "CASE WHEN true /* END */ THEN not END", true},
		{"line_comment", "CASE WHEN true // END\n THEN not END", true},
		{"property_prefix", "CASE WHEN true THEN m.not END + CASE WHEN true THEN not END", true},
		{"excess_closers", "CASE WHEN true THEN not END END END", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := strings.Index(tc.text, "not END") + len("not ")
			if tc.name == "property_prefix" {
				candidate = strings.LastIndex(tc.text, "END")
			}
			require.Equal(t, tc.required, casePrefixEndRequired(tc.text, candidate))
		})
	}
}
