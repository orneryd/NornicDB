package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1663 implementation lane. The shorthand forms are read by the one
// shell parameter producer (readShellParameterInput); release's legacy
// splitter was a second reader of the same input.
func TestShellParameterShorthandForms(t *testing.T) {
	text := "key1 42"
	input, err := readShellParameterInput(text)
	require.NoError(t, err)
	require.Equal(t, "key1", text[input.key.Start:input.key.End])
	require.Equal(t, "42", text[input.value.Start:input.value.End])

	// Release emitted {key1:: 42} here, which failed; the producer rejects it.
	for _, rejected := range []string{"key1: 42", "onlyone", "key:", `"" 42`} {
		_, err := readShellParameterInput(rejected)
		require.Error(t, err, rejected)
	}
}

func TestNormalizeParamMap_Variants(t *testing.T) {
	// map[string]interface{} → returned as-is.
	in1 := map[string]interface{}{"a": 1, "b": 2}
	got1, err := normalizeParamMap(in1)
	require.NoError(t, err)
	require.Equal(t, in1, got1)

	// map[interface{}]interface{} with string keys → normalized.
	in2 := map[interface{}]interface{}{"a": 1, "b": 2}
	got2, err := normalizeParamMap(in2)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"a": 1, "b": 2}, got2)

	// map[interface{}]interface{} with non-string key → error.
	in3 := map[interface{}]interface{}{42: "v"}
	_, err = normalizeParamMap(in3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "keys must be strings")

	// Wrong type entirely → error.
	_, err = normalizeParamMap("not a map")
	require.Error(t, err)
	require.Contains(t, err.Error(), "must evaluate to a map")
}
