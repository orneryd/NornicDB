package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR27: function admission is compile-time checking of calls, and a Cypher
// comment is trivia. Text inside a comment is not a call at any position (the
// statement, the gap between a name and its argument list, or an argument
// position), in either comment form, while the calls around it keep their
// arity, static argument type and undefined-variable checks and quoted text
// stays quoted text. forEachStaticFunctionArgument is the one walker both the
// arity check (functionArities) and the static argument checks read a
// statement through, so these tests drive it directly and then the public
// Execute path for both parsers, with and without query normalization.

// TestFunctionAdmissionIgnoresCommentTextPublic is the reported NOR27
// regression through the public path: a comment naming functions that take
// more arguments than the ones it passes is a comment, and the statement runs.
func TestFunctionAdmissionIgnoresCommentTextPublic(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		for _, tc := range []struct {
			name, query string
			want        interface{}
		}{
			{
				name:  "block comment before the projection",
				query: "RETURN /* range('bad'), percentileCont('bad') */ substring(toLower('OKAY'),(0),(2)) AS v",
				want:  "ok",
			},
			{
				name:  "line comment before the projection",
				query: "RETURN // range('bad'), percentileCont('bad')\n substring(toLower('OKAY'),(0),(2)) AS v",
				want:  "ok",
			},
			{
				name:  "block comment holding an unbalanced parenthesis",
				query: "RETURN /* range('bad' */ substring(toLower('OKAY'),(0),(2)) AS v",
				want:  "ok",
			},
			{
				name:  "block comment in the name-argument gap",
				query: "RETURN substring /* range('bad'), percentileCont('bad') */ (toLower('OKAY'), 0, 2) AS v",
				want:  "ok",
			},
			{
				name:  "line comment in the name-argument gap",
				query: "RETURN substring // range('bad'), percentileCont('bad')\n (toLower('OKAY'), 0, 2) AS v",
				want:  "ok",
			},
			{
				name:  "block comment before the first argument",
				query: "RETURN substring(/* range('bad') */ toLower('OKAY'), 0, 2) AS v",
				want:  "ok",
			},
			{
				name:  "line comment before the first argument",
				query: "RETURN substring(// range('bad')\n toLower('OKAY'), 0, 2) AS v",
				want:  "ok",
			},
			{
				name:  "block comment before the middle argument",
				query: "RETURN substring(toLower('OKAY'), /* percentileCont('bad') */ 0, 2) AS v",
				want:  "ok",
			},
			{
				name:  "block comment before the last argument",
				query: "RETURN substring(toLower('OKAY'), 0, /* abs('bad') */ 2) AS v",
				want:  "ok",
			},
			{
				name:  "block comment after the last argument",
				query: "RETURN substring(toLower('OKAY'), 0, 2 /* range('bad') */) AS v",
				want:  "ok",
			},
			{
				name:  "line comment after the last argument",
				query: "RETURN substring(toLower('OKAY'), 0, 2 // range('bad')\n) AS v",
				want:  "ok",
			},
			{
				name:  "block comment holding commas between arguments",
				query: "RETURN substring(toLower('OKAY') /* 0, 1, 2 */, 0, 2) AS v",
				want:  "ok",
			},
			{
				name:  "block comment in a nested call's argument",
				query: "RETURN substring(substring(toLower('OKAY') /* range('bad') */, 0, 2), 0, 1) AS v",
				want:  "o",
			},
			{
				name:  "block comment inside a trim FROM form",
				query: "RETURN trim(BOTH /* range('bad') */ 'x' FROM 'xax') AS v",
				want:  "a",
			},
			{
				name:  "quoted comment text is text",
				query: "RETURN toLower('/* RANGE(BAD) */') AS v",
				want:  "/* range(bad) */",
			},
			{
				name:  "quoted line comment text is text",
				query: "RETURN substring('// range(bad)', 0, 2) AS v",
				want:  "//",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(context.Background(), tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, tc.query)
			})
		}
	})
}

// TestFunctionAdmissionKeepsErrorsAroundCommentsPublic keeps the compile-time
// errors function admission owns while comment text sits beside the call: a
// real call with the wrong number of arguments, an argument of a static type
// no signature takes, and an undefined variable are still rejected.
func TestFunctionAdmissionKeepsErrorsAroundCommentsPublic(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		for _, tc := range []struct {
			name, query, want string
		}{
			{
				name:  "too few arguments after a comment",
				query: "RETURN /* range('bad') */ range('okay') AS v",
				want:  "Insufficient parameters for function 'range'",
			},
			{
				name:  "too few arguments across the name-gap comment",
				query: "RETURN range /* range('bad') */ ('okay') AS v",
				want:  "Insufficient parameters for function 'range'",
			},
			{
				name:  "too few arguments with an argument comment",
				query: "RETURN substring(toLower('OKAY') /* range('bad') */) AS v",
				want:  "Insufficient parameters for function 'substring'",
			},
			{
				name:  "too many arguments across the name-gap comment",
				query: "RETURN abs /* range('bad') */ (1, 2) AS v",
				want:  "Too many parameters for function 'abs'",
			},
			{
				name:  "too many arguments with an argument comment",
				query: "RETURN substring(toLower('OKAY'), 0 /* range('bad') */, 2, 3) AS v",
				want:  "Too many parameters for function 'substring'",
			},
			{
				name:  "static argument type with a comment inside the call",
				query: "RETURN toUpper(/* range('bad') */ 1) AS v",
				want:  "Type mismatch: expected String",
			},
			{
				name:  "static argument type across the name-gap comment",
				query: "RETURN toUpper /* range('bad') */ (1) AS v",
				want:  "Type mismatch: expected String",
			},
			{
				name:  "undefined variable after a comment",
				query: "RETURN /* range('bad') */ missingVariable AS v",
				want:  "Variable `missingVariable` not defined",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := exec.Execute(context.Background(), tc.query, nil)
				require.Error(t, err, tc.query)
				require.Contains(t, err.Error(), tc.want, tc.query)
			})
		}
	})
}

// TestStaticFunctionArgumentWalkSkipsComments drives the walker itself, so the
// arguments it hands to the arity and static type checks are exactly the
// statement's, whichever parser and normalization setting runs later. A nested
// call's own arguments follow its enclosing call's, as they do without
// comments.
func TestStaticFunctionArgumentWalkSkipsComments(t *testing.T) {
	walked := func(t *testing.T, text string) []string {
		t.Helper()
		var arguments []string
		err := forEachStaticFunctionArgument(context.Background(), text, func(_ staticArgumentType, expression string, _ int) error {
			arguments = append(arguments, expression)
			return nil
		})
		require.NoError(t, err, text)
		return arguments
	}
	for _, tc := range []struct {
		name, text string
		want       []string
	}{
		{
			name: "block comment before the call",
			text: "RETURN /* range('bad'), percentileCont('bad') */ substring(toLower('OKAY'),(0),(2)) AS v",
			want: []string{"toLower('OKAY')", "(0)", "(2)", "'OKAY'"},
		},
		{
			name: "line comment before the call",
			text: "RETURN // range('bad'), percentileCont('bad')\n substring(toLower('OKAY'),(0),(2)) AS v",
			want: []string{"toLower('OKAY')", "(0)", "(2)", "'OKAY'"},
		},
		{
			name: "block comment in the name-argument gap",
			text: "RETURN substring /* range('bad') */ (toLower('OKAY'), 0, 2) AS v",
			want: []string{"toLower('OKAY')", "0", "2", "'OKAY'"},
		},
		{
			name: "line comment in the name-argument gap",
			text: "RETURN substring // range('bad')\n (toLower('OKAY'), 0, 2) AS v",
			want: []string{"toLower('OKAY')", "0", "2", "'OKAY'"},
		},
		{
			name: "comments at every argument position",
			text: "RETURN substring(/* range('bad') */ toLower('OKAY') /* 0, 1 */, /* abs('bad') */ 0, 2 /* range('bad') */) AS v",
			want: []string{"toLower('OKAY')", "0", "2", "'OKAY'"},
		},
		{
			name: "comment text holding brackets and quotes",
			text: "RETURN substring('OKAY', 0, 2) /* ' ) range(' */ AS v",
			want: []string{"'OKAY'", "0", "2"},
		},
		{
			name: "quoted comment text stays an argument",
			text: "RETURN substring('/* range(''bad'') */', 0, 3) AS v",
			want: []string{"'/* range(''bad'') */'", "0", "3"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, walked(t, tc.text))
		})
	}

	// A call whose arguments are commented out of the way still counts its
	// one argument: a comment is trivia, not an argument separator, so the
	// arity check still rejects the call before any argument is checked.
	var arguments []string
	err := forEachStaticFunctionArgument(context.Background(), "RETURN substring /* gap */ (toLower('OKAY'))", func(_ staticArgumentType, expression string, _ int) error {
		arguments = append(arguments, expression)
		return nil
	})
	require.ErrorContains(t, err, "Insufficient parameters for function 'substring'")
	require.Empty(t, arguments)
}

// TestFunctionAdmissionCommentOnlyUnknownFunction: a function name a comment
// mentions is not an unknown function, and unknown function reporting is
// otherwise unchanged.
func TestFunctionAdmissionCommentOnlyUnknownFunction(t *testing.T) {
	exec, _ := newTestExecutor(t)
	result, err := exec.Execute(context.Background(), "RETURN 1 AS v /* noSuchFunction('bad') */", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	_, err = exec.Execute(context.Background(), "RETURN noSuchFunction('bad') AS v", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown function: noSuchFunction")
}
