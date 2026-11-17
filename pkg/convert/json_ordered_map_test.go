package convert

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOrderedJSONMapUsesDomainWriter(t *testing.T) {
	value := OrderedJSONMap{Keys: []string{"z", "a", "missing"}, Values: map[string]interface{}{
		"z": int64(9007199254740993), "a": OrderedJSONMap{Keys: []string{"float", "text"}, Values: map[string]interface{}{"float": 1.0, "text": "<&"}}, "ignored": true,
	}}
	encoded, err := PropertyJSON.Marshal(value)
	require.NoError(t, err)
	require.Equal(t, `{"z":9007199254740993,"a":{"float":1.0,"text":"\u003c\u0026"},"missing":null}`, string(encoded))
	var buffer bytes.Buffer
	encoder := PropertyJSON.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	require.NoError(t, encoder.Encode(value))
	require.Contains(t, buffer.String(), `"text":"<&"`)
	_, err = PropertyJSON.Marshal(OrderedJSONMap{Keys: []string{"bad"}, Values: map[string]interface{}{"bad": make(chan int)}})
	require.Error(t, err)
	var unsupported *json.UnsupportedTypeError
	require.ErrorAs(t, err, &unsupported)
}

func TestOrderedJSONMapRejectsCycles(t *testing.T) {
	values := map[string]interface{}{}
	value := OrderedJSONMap{Keys: []string{"self"}, Values: values}
	values["self"] = value
	_, err := PropertyJSON.Marshal(value)
	var cycle *json.UnsupportedValueError
	require.ErrorAs(t, err, &cycle)
	// The failed traversal must release its tracked identities before pool reuse.
	_, err = PropertyJSON.Marshal(OrderedJSONMap{Keys: []string{"ok"}, Values: map[string]interface{}{"ok": int64(1)}})
	require.NoError(t, err)
}
