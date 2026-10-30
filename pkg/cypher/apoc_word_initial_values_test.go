// Author: Codex implementation lane for NOR-207, 2026-10-26.
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAPOCWordInitialValuesNOR207(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	handlers := map[string]interface{}{}
	for _, name := range []string{"apoc.text.capitalize", "apoc.text.decapitalize", "apoc.text.capitalizeAll", "apoc.text.decapitalizeAll"} {
		info, ok := apoc.GetInfo(name)
		require.True(t, ok)
		handlers[name] = info.Handler
	}
	withPluginFunctions(t, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor207"))
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, query := range []string{
			"RETURN apoc.text.capitalize('hello world') AS v, apoc.text.decapitalize('HELLO WORLD') AS lower",
			"WITH $s AS s RETURN apoc.text.capitalize(s) AS v, apoc.text.decapitalize('HELLO WORLD') AS lower",
		} {
			t.Run(fmt.Sprintf("capitalize/programsDisabled=%t/%s", disabled, query), func(t *testing.T) {
				result, err := exec.Execute(context.Background(), query, map[string]interface{}{"s": "hello world"})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"Hello world", "hELLO WORLD"}}, result.Rows, "APOC 5.26.30 Strings.java:514,534")
			})
		}
		for _, tc := range []struct{ input, capitalized, decapitalized string }{
			{"hello world", "Hello World", "hello world"},
			{"HELLO WORLD", "HELLO WORLD", "hELLO wORLD"},
			{"  hello   World  ", "Hello   World", "hello   world"},
			{"hello\tworld", "Hello\tworld", "hello\tworld"},
			{"ǳungla 𐐀XYZ", "ǲungla 𐐀XYZ", "ǳungla 𐐨XYZ"},
			{"", "", ""},
		} {
			for _, query := range []string{
				"RETURN apoc.text.capitalizeAll($s) AS c, apoc.text.decapitalizeAll($s) AS d",
				"UNWIND [$s] AS s RETURN apoc.text.capitalizeAll(s) AS c, apoc.text.decapitalizeAll(s) AS d",
			} {
				t.Run(fmt.Sprintf("programsDisabled=%t/%q/%s", disabled, tc.input, query), func(t *testing.T) {
					result, err := exec.Execute(context.Background(), query, map[string]interface{}{"s": tc.input})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{tc.capitalized, tc.decapitalized}}, result.Rows)
				})
			}
		}
	}
}

// These are the byte-for-byte d663 converter values retained by Nornic2's
// allocation. Their known APOC discrepancies remain tracked by NOR-282.
func TestAPOCNameConverterGuardNOR207(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	handlers := map[string]interface{}{}
	for _, name := range []string{"apoc.text.camelCase", "apoc.text.upperCamelCase"} {
		info, ok := apoc.GetInfo(name)
		require.True(t, ok)
		handlers[name] = info.Handler
	}
	withPluginFunctions(t, handlers)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor207_converter_guard"))
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, tc := range []struct{ input, camel, upper string }{
			{"hello ßeta", "helloßeta", "Helloßeta"},
			{"hello ǳungla", "helloǲungla", "Helloǲungla"},
			{"hello 𐐨abc", "hello𐐀abc", "Hello𐐀abc"},
			{"ßeta world", "ßetaWorld", "ßetaWorld"},
			{"ǳungla world", "ǳunglaWorld", "ǲunglaWorld"},
			{"𐐨abc world", "𐐨abcWorld", "𐐀abcWorld"},
			{"hello foo.bar", "helloFoo.Bar", "HelloFoo.Bar"},
			{"hello_world", "helloWorld", "HelloWorld"},
		} {
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, tc.input), func(t *testing.T) {
				result, err := exec.Execute(context.Background(), "UNWIND [$s] AS s RETURN apoc.text.camelCase(s) AS c, apoc.text.upperCamelCase(s) AS u", map[string]interface{}{"s": tc.input})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{tc.camel, tc.upper}}, result.Rows)
			})
		}
	}
}
