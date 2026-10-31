package convert

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPropertyJSONLegacyStateKeepsFloat64Meaning(t *testing.T) {
	raw := []byte(`{"large":1152921504606847000,"whole":2,"zero":-0,"nested":[9007199254740993,{"value":2.0}]}`)
	var previous, legacy, exact map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &previous))
	require.NoError(t, PropertyJSON.UnmarshalLegacyState(raw, &legacy))
	require.Equal(t, previous, legacy)
	require.Equal(t, float64(1<<60), legacy["large"])
	require.Equal(t, float64(2), legacy["whole"])
	require.True(t, math.Signbit(legacy["zero"].(float64)))
	require.NoError(t, PropertyJSON.Unmarshal(raw, &exact))
	require.Equal(t, int64(1152921504606847000), exact["large"])
	require.Equal(t, int64(2), exact["whole"])
	require.Error(t, PropertyJSON.UnmarshalLegacyState(append(raw, []byte(` {}`)...), &legacy))
}

func TestJSONIngestionSharedDecoder(t *testing.T) {
	type record struct {
		Properties map[string]interface{} `json:"properties"`
		Vector     []float32              `json:"vector"`
		Count      int                    `json:"count"`
		Time       time.Time              `json:"time"`
		Raw        json.RawMessage        `json:"raw"`
		Number     json.Number            `json:"number"`
		Ignored    *record                `json:"-"`
	}
	var document struct {
		Records []*record         `json:"records"`
		Maps    map[string]record `json:"maps"`
	}
	require.NoError(t, CypherInputJSON.Unmarshal([]byte(`{"records":[{"properties":{"max":9223372036854775807,"min":-9223372036854775808,"v":9007199254740993,"d":2.0,"e":2e0,"nested":[null,{"v":-9007199254740993}]},"vector":[1.25],"count":3,"time":"2020-01-01T00:00:00Z","raw":{"v":9007199254740993},"number":17},null],"maps":{"a":{"properties":{"v":9007199254740993}}}}`), &document))
	entry := document.Records[0]
	require.Equal(t, int64(math.MaxInt64), entry.Properties["max"])
	require.Equal(t, int64(math.MinInt64), entry.Properties["min"])
	require.Equal(t, int64(9007199254740993), entry.Properties["v"])
	require.Equal(t, float64(2), entry.Properties["d"])
	require.Equal(t, float64(2), entry.Properties["e"])
	require.Equal(t, []interface{}{nil, map[string]interface{}{"v": int64(-9007199254740993)}}, entry.Properties["nested"])
	require.Equal(t, []float32{1.25}, entry.Vector)
	require.Equal(t, 3, entry.Count)
	require.Equal(t, 2020, entry.Time.Year())
	require.JSONEq(t, `{"v":9007199254740993}`, string(entry.Raw))
	require.Equal(t, json.Number("17"), entry.Number)
	require.Nil(t, document.Records[1])
	require.Equal(t, int64(9007199254740993), document.Maps["a"].Properties["v"])
	entry.Ignored = entry
	require.NoError(t, CypherInputJSON.Unmarshal([]byte(`{"properties":null}`), entry))
	require.Nil(t, entry.Properties)
	for _, input := range []string{``, `{`, `{"v":1} {"v":2}`, `{"v":1} trailing`} {
		var value interface{}
		require.Error(t, CypherInputJSON.Unmarshal([]byte(input), &value), input)
	}
	require.Error(t, CypherInputJSON.Unmarshal([]byte(`1`), nil))
	// Neo4j 5.26's one external number policy: an out-of-range FLOAT reads as
	// +/-Infinity, an INTEGER outside int64 is "Unsupported type of Number".
	var overflow interface{}
	require.NoError(t, CypherInputJSON.Unmarshal([]byte(`{"v":1e999,"w":-1e999}`), &overflow))
	require.Equal(t, map[string]interface{}{"v": math.Inf(1), "w": math.Inf(-1)}, overflow)
	require.NoError(t, CypherInputJSON.NewDecoder(strings.NewReader(`[1e400,2.0]`)).Decode(&overflow))
	require.Equal(t, []interface{}{math.Inf(1), float64(2)}, overflow)
	for _, integer := range []string{"9223372036854775808", "18446744073709551616", "-9223372036854775809"} {
		err := CypherInputJSON.Unmarshal([]byte(`{"v":`+integer+`}`), &overflow)
		var unsupported *UnsupportedNumberError
		require.ErrorAs(t, err, &unsupported, integer)
		require.Equal(t, "Unsupported type of Number "+integer, unsupported.Error())
	}
	decoder := CypherInputJSON.NewDecoder(strings.NewReader(`9007199254740993 2.0`))
	var value interface{}
	require.NoError(t, decoder.Decode(&value))
	require.Equal(t, int64(9007199254740993), value)
	require.NoError(t, decoder.Decode(&value))
	require.Equal(t, float64(2), value)
}

func TestJSONIngestionNumberPolicies(t *testing.T) {
	_, err := CypherInputJSON.Numbers(json.Number("9223372036854775808"))
	var unsupported *UnsupportedNumberError
	require.ErrorAs(t, err, &unsupported)
	infinite, err := CypherInputJSON.Numbers(json.Number("1e400"))
	require.NoError(t, err)
	require.Equal(t, math.Inf(1), infinite)
	_, err = CypherInputJSON.Numbers(json.Number("invalid"))
	require.Error(t, err)
	value, err := PropertyJSON.Numbers(map[string]interface{}{"v": []interface{}{json.Number("18446744073709551615"), json.Number("2.0")}})
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"v": []interface{}{uint64(math.MaxUint64), float64(2)}}, value)
	for _, input := range []interface{}{json.Number("18446744073709551616"), []interface{}{json.Number("-9223372036854775809")}, map[string]interface{}{"v": json.Number("1e999")}} {
		_, err := PropertyJSON.Numbers(input)
		require.Error(t, err)
	}
}

func TestJSONIngestionEncodingRoundTrip(t *testing.T) {
	properties := map[string]interface{}{"v": int64(9007199254740993), "whole": float64(2), "negativeZero": math.Copysign(0, -1), "nested": []interface{}{float64(3), int64(4)}, "typed": []float64{5, 1.25}, "unsigned": uint64(math.MaxUint64), "nil": []interface{}(nil)}
	encoded, err := json.Marshal(PropertyJSON.Prepare(properties))
	require.NoError(t, err)
	var decoded map[string]interface{}
	require.NoError(t, PropertyJSON.Unmarshal(encoded, &decoded))
	require.Equal(t, int64(9007199254740993), decoded["v"])
	require.Equal(t, float64(2), decoded["whole"])
	require.True(t, math.Signbit(decoded["negativeZero"].(float64)))
	require.Equal(t, []interface{}{float64(3), int64(4)}, decoded["nested"])
	require.Equal(t, []interface{}{float64(5), 1.25}, decoded["typed"])
	require.Equal(t, uint64(math.MaxUint64), decoded["unsigned"])
	require.Nil(t, decoded["nil"])
	require.IsType(t, float64(0), properties["whole"], "encoding must not mutate storage values")
	for _, input := range []string{`{"v":18446744073709551616}`, `{"v":1e999}`, `{`, `{} {}`} {
		require.Error(t, PropertyJSON.Unmarshal([]byte(input), &decoded))
	}
	cycle := map[string]interface{}{}
	cycle["self"] = cycle
	_, err = json.Marshal(PropertyJSON.Prepare(cycle))
	require.Error(t, err)
	listCycle := make([]interface{}, 1)
	listCycle[0] = listCycle
	_, err = json.Marshal(PropertyJSON.Prepare(listCycle))
	require.Error(t, err)
}
