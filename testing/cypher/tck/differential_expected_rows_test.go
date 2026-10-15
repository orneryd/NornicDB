package tck

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDifferentialExpectedValueNormalization(t *testing.T) {
	executor := boltDifferentialExecutor{}
	for _, test := range []struct {
		json   string
		actual any
	}{
		{"9007199254740993", int64(9007199254740993)},
		{"9223372036854775807", int64(9223372036854775807)},
		{"-9223372036854775808", int64(-9223372036854775808)},
		{"-0", int64(0)},
		{"1.0", float64(1)},
		{"1.0005e3", float64(1000.5)},
	} {
		t.Run(test.json, func(t *testing.T) {
			got := executor.NormalizeExpectedValue(json.Number(test.json))
			require.Equal(t, differentialOutcomeValue(test.actual), got)
		})
	}
	require.NotEqual(t, executor.NormalizeExpectedValue(json.Number("1")), executor.NormalizeExpectedValue(json.Number("1.0")))
	require.Equal(t,
		differentialOutcomeValue(map[string]any{"nested": []any{int64(9007199254740993), float64(1), true, nil}}),
		executor.NormalizeExpectedValue(map[string]any{"nested": []any{json.Number("9007199254740993"), json.Number("1.0"), true, nil}}))
}
