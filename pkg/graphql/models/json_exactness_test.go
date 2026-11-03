package models

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONIngestionGraphQLProperties(t *testing.T) {
	for _, input := range []interface{}{`{"v":9007199254740993,"d":2.0}`, map[string]interface{}{"v": json.Number("9007199254740993"), "d": json.Number("2.0")}} {
		var value JSON
		require.NoError(t, value.UnmarshalGQL(input))
		require.Equal(t, int64(9007199254740993), value["v"])
		require.Equal(t, float64(2), value["d"])
	}
}
