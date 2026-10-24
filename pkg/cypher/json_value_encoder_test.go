package cypher

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/stretchr/testify/require"
)

// The owner's encoder writes exactly what encoding/json writes.
func TestJSONValueEncoderMatchesEncodingJSON(t *testing.T) {
	long := strings.Repeat("é<a&b>\"\\\n\u2028", bulk.CopyStride/4)
	values := []interface{}{
		nil, true, int64(-3), 2.5, 1e21, "plain", long, "\xff\xfe" + long,
		[]interface{}{}, map[string]interface{}{}, []interface{}(nil), map[string]interface{}(nil),
		[]interface{}{int64(1), "two", []interface{}{3.0}, map[string]interface{}{"z": 1, "a": []string{"x", "y"}}},
		map[string]interface{}{"b": map[string]interface{}{"c": nil}, "a": []byte{1, 2, 3}, "t": time.Unix(0, 0).UTC()},
		[]string{"a", "b"}, []int64{1, 2}, map[string]string{"k": "v"},
	}
	for _, value := range values {
		want, err := json.Marshal(value)
		require.NoError(t, err)
		got, err := encodeJSONValue(nil, value, "", "")
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))
		want, err = json.MarshalIndent(value, "", "  ")
		require.NoError(t, err)
		got, err = encodeJSONValue(nil, value, "", "  ")
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))
	}
}

// A large value's encoding stops when its statement is canceled.
func TestJSONValueEncoderStopsOnCancellation(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	large := make([]interface{}, 4*bulk.Stride)
	for i := range large {
		large[i] = map[string]interface{}{"k": int64(i)}
	}
	encoded, err := insideStopBoundary(func() ([]byte, error) {
		return encodeJSONValue(valueWalk(canceled), large, "", "  ")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, encoded)
}
