package storage

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaDefinitionCodec(t *testing.T) {
	definition := &SchemaDefinition{Version: 1, Constraints: []Constraint{{AllowedValues: []interface{}{
		int64(9007199254740993), float64(1 << 60), float64(1e20), math.Copysign(0, -1),
		map[string]interface{}{"nested": []interface{}{int64(9007199254740993)}},
	}}}}
	raw, err := EncodeSchemaDefinition(definition)
	require.NoError(t, err)
	require.Equal(t, 1, definition.Version, "encoding must not mutate the caller's definition")
	require.Contains(t, string(raw), `"version":2`)
	var copy SchemaDefinition
	require.NoError(t, DecodeSchemaDefinition(raw, &copy))
	require.Equal(t, 2, copy.Version)
	require.Equal(t, definition.Constraints, copy.Constraints)
	require.True(t, math.Signbit(copy.Constraints[0].AllowedValues[3].(float64)))
	copy.Constraints[0].AllowedValues[4].(map[string]interface{})["nested"].([]interface{})[0] = int64(7)
	require.Equal(t, int64(9007199254740993), definition.Constraints[0].AllowedValues[4].(map[string]interface{})["nested"].([]interface{})[0])
	for _, raw := range []string{`{`, `{"version":3}`, `{"version":2,"constraints":[{"allowed_values":[100000000000000000000]}]}`} {
		require.Error(t, DecodeSchemaDefinition([]byte(raw), &SchemaDefinition{}))
	}
	_, err = EncodeSchemaDefinition(nil)
	require.Error(t, err)
	_, err = EncodeSchemaDefinition(&SchemaDefinition{Constraints: []Constraint{{AllowedValues: []interface{}{math.Inf(1)}}}})
	require.Error(t, err)
}
