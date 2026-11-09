package fabric

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFragmentStatementTextIsTheExecutableFragment pins the planner fragment
// owner: leading trivia is not statement text, so it is not part of the
// fragment a graph runs, and a fragment of only trivia is no statement at all.
// Trivia after the fragment's clauses stays as written, and a comment GapEnd
// reports unfinished stays with the fragment so its syntax error is visible.
func TestFragmentStatementTextIsTheExecutableFragment(t *testing.T) {
	for _, tc := range []struct{ fragment, want string }{
		{"", ""},
		{" \n\t", ""},
		{"// c\n", ""},
		{"/* c */", ""},
		{" /* first */ // second\n ", ""},
		{"CALL { USE cmp.a RETURN 1 }", "CALL { USE cmp.a RETURN 1 }"},
		{"// c\nWITH 1 AS x", "WITH 1 AS x"},
		{"/* c */ WITH 1 AS x", "WITH 1 AS x"},
		{"\n\n/* c */\nWITH 1 AS x ", "WITH 1 AS x"},
		{"WITH 1 AS x /* tail */", "WITH 1 AS x /* tail */"},
		{"/* unfinished", "/* unfinished"},
	} {
		require.Equal(t, tc.want, fragmentStatementText(tc.fragment), "%q", tc.fragment)
	}
}
