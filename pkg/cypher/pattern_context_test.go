package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPatternBindingKeywordContext(t *testing.T) {
	for _, tc := range []struct {
		name, query, keyword string
		wantActual           bool
	}{
		{"match path", "MATCH call = (n) RETURN n", "CALL", false},
		{"optional match path", "OPTIONAL MATCH call = (n) RETURN n", "CALL", false},
		{"block comment", "MATCH /* binding */ call = (n) RETURN n", "CALL", false},
		{"line comment", "MATCH // binding\n call = (n) RETURN n", "CALL", false},
		{"nested pattern", "MATCH call = ((a)-->(b))+ RETURN a", "CALL", false},
		{"procedure named call", "CALL call(1)", "CALL", true},
		{"procedure named match", "CALL match(1)", "MATCH", false},
		{"real after alias named call", "WITH 1 AS call CALL () { RETURN 1 AS v } RETURN v", "CALL", true},
		{"real after alias named match", "WITH 1 AS match CALL () { RETURN 1 AS v } RETURN v", "CALL", true},
		{"real after quoted alias named match", "WITH 1 AS `match` CALL () { RETURN 1 AS v } RETURN v", "CALL", true},
		{"real after matched node", "MATCH (n) CALL { WITH n RETURN n } RETURN n", "CALL", true},
		{"quoted path name", "MATCH `call` = (n) RETURN n", "CALL", false},
		{"comment opaque", "MATCH p = (n) // CALL { }\nRETURN n", "CALL", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			positions := findAllTopLevelPipelineKeywordPositions(originalBraceView(tc.query), tc.keyword)
			if tc.wantActual {
				require.Equal(t, []int{strings.Index(tc.query, " CALL") + 1}, positions)
			} else {
				require.Empty(t, positions)
			}
		})
	}
	query := "MATCH call = (n) RETURN n"
	var got bool
	allocations := testing.AllocsPerRun(1000, func() { got = clauseKeywordUsedAsName(originalBraceView(query), 6, 10, "CALL") })
	require.True(t, got)
	require.Zero(t, allocations, "pattern-name boundary predicate must retain its allocation contract")
}
