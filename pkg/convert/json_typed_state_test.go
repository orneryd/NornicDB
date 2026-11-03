package convert

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPropertyJSONTypedState(t *testing.T) {
	type metadata struct {
		Score  float64
		Small  float32
		Values []any
		Nested any
	}
	for _, score := range []float64{0, math.Copysign(0, -1), 1, 1e-7, 1e-6, 1e7, 1e20, -1e20, float64(1 << 60), float64(math.MaxUint64), 1e21, math.MaxFloat64} {
		value := metadata{Score: score, Small: float32(1e-6)}
		want, err := json.Marshal(value)
		require.NoError(t, err)
		got, err := PropertyJSON.MarshalState(value)
		require.NoError(t, err)
		require.Equal(t, string(want), string(got), "declared fields retain existing bytes")
		var decoded metadata
		require.NoError(t, PropertyJSON.Unmarshal(got, &decoded))
		require.Equal(t, score, decoded.Score)
		require.Equal(t, math.Signbit(score), math.Signbit(decoded.Score))
		require.Equal(t, value.Small, decoded.Small)
	}
	for _, small := range []float32{0, float32(math.Copysign(0, -1)), 2, 1e-7, 1e30, math.MaxFloat32} {
		value := metadata{Small: small}
		want, err := json.Marshal(value)
		require.NoError(t, err)
		got, err := PropertyJSON.MarshalState(value)
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))
		var decoded metadata
		require.NoError(t, PropertyJSON.Unmarshal(got, &decoded))
		require.Equal(t, small, decoded.Small)
		require.Equal(t, math.Signbit(float64(small)), math.Signbit(float64(decoded.Small)))
	}
	value := metadata{
		Score:  1,
		Values: []any{int64(9007199254740993), float64(2), math.Copysign(0, -1)},
		Nested: struct{ Value float64 }{2},
	}
	got, err := PropertyJSON.MarshalState(value)
	require.NoError(t, err)
	var decoded metadata
	require.NoError(t, PropertyJSON.Unmarshal(got, &decoded))
	require.Equal(t, int64(9007199254740993), decoded.Values[0])
	require.Equal(t, float64(2), decoded.Values[1])
	require.True(t, math.Signbit(decoded.Values[2].(float64)))
	require.Equal(t, float64(2), decoded.Nested.(map[string]any)["Value"], "dynamic envelope fields need explicit FLOAT identity")
	for _, invalid := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := PropertyJSON.MarshalState(metadata{Score: invalid})
		require.Error(t, err)
		_, err = PropertyJSON.MarshalState(metadata{Small: float32(invalid)})
		require.Error(t, err)
	}
}

func TestPropertyJSONIndentOwnsReturnedBytes(t *testing.T) {
	first, err := PropertyJSON.MarshalIndent(map[string]any{"value": float64(2)}, "", "  ")
	require.NoError(t, err)
	want := string(first)
	for i := 0; i < 100; i++ {
		_, err := PropertyJSON.MarshalIndent(map[string]any{"different": i}, "prefix", "    ")
		require.NoError(t, err)
	}
	require.Equal(t, want, string(first), "subsequent writes cannot overwrite the caller's document")
}
