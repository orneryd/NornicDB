package cypher

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestFunctionSyntaxStructure(t *testing.T) {
	text := `substring(/* don't split , ) */ toLower('OKAY'), {k:range(1,2), ` + "`a,b`" + `: [1,2]}, 2 /* , ' */)`
	syntax := readFunctionSyntax(text)
	require.Len(t, syntax.calls, 3)
	call, ok := functionCallAt(text, 0)
	require.True(t, ok)
	require.Equal(t, len(text)-1, call.close(text))
	require.Equal(t, []string{`toLower('OKAY')`, "{k:range(1,2), `a,b`: [1,2]}", "2"}, call.arguments(nil))
	require.Equal(t, []string{"'a,b'", "[1,2]", "{x:1,y:2}", ""}, readFunctionSyntax("'a,b', [1,2], {x:1,y:2}, /* ' , */").appendArguments(nil, 0, true))
	require.Equal(t, []string{"'a,b'", "[1,2]", "{x:1,y:2}"}, readFunctionSyntax("'a,b', [1,2], {x:1,y:2}, /* ' , */").appendArguments(nil, 0, false))
	require.True(t, hasValidStartKeyword("/* } { ' \" */ RETURN 'ok' AS v"))
}

func TestFunctionSyntaxQualifiedInventory(t *testing.T) {
	text := "RETURN apoc /* ' , */ . coll . sort(collect(x)), datetime.statement(), `custom`.`fn`(1)"
	var names []string
	require.NoError(t, forEachFunctionCall(text, func(name, _ string) error {
		names = append(names, name)
		return nil
	}))
	require.Equal(t, []string{"apoc.coll.sort", "collect", "datetime.statement", "custom.fn"}, names)
	name, arguments, ok := parseFunctionCallArguments("apoc.coll.sort(/* , */ collect(x))")
	require.True(t, ok)
	require.Equal(t, "apoc.coll.sort", name)
	require.Equal(t, []string{"collect(x)"}, arguments)
}

func TestFunctionSyntaxUncachedWholeTextWalk(t *testing.T) {
	// Exceed the retention bound: the walker must keep its lexical structure
	// rather than reparsing the entire statement for each successive call.
	text := "RETURN " + strings.Repeat("coalesce(/* don't split , */ null, custom.fn(1)),", 512) + "1"
	require.Greater(t, len(text), 16384)
	count := 0
	require.NoError(t, forEachFunctionCall(text, func(name, _ string) error {
		if count%2 == 0 {
			require.Equal(t, "coalesce", name)
		} else {
			require.Equal(t, "custom.fn", name)
		}
		count++
		return nil
	}))
	require.Equal(t, 1024, count)
}

func TestFunctionSyntaxUnicodeTrivia(t *testing.T) {
	for _, gap := range []string{"\u00a0", "\u2003", "/* ' , */\u00a0"} {
		text := "custom" + gap + "." + gap + "fn" + gap + "(1,2)"
		name, arguments, ok := parseFunctionCallArguments(text)
		require.True(t, ok, text)
		require.Equal(t, "custom.fn", name)
		require.Equal(t, []string{"1", "2"}, arguments)
		name, ok = canonicalCallableName("custom" + gap + "." + gap + "fn")
		require.True(t, ok)
		require.Equal(t, "custom.fn", name)
	}
}
