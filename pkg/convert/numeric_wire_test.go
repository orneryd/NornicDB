package convert

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type numericWireCustomFloat float64

func (value *numericWireCustomFloat) MarshalJSON() ([]byte, error) { return []byte(`"custom"`), nil }

type numericWireZero struct{ Present bool }

func (value *numericWireZero) IsZero() bool { return !value.Present }

type numericWireText string

func (value numericWireText) MarshalText() ([]byte, error) {
	return []byte("text:" + string(value)), nil
}

func TestNumericWireFloatText(t *testing.T) {
	for _, test := range []struct {
		value float64
		bits  int
		want  string
	}{
		{0, 64, "0.0"}, {math.Copysign(0, -1), 64, "-0.0"}, {0.001, 64, "0.001"},
		{9999999, 64, "9999999.0"}, {1e7, 64, "1.0E7"}, {1.25e-4, 64, "1.25E-4"},
		{float64(float32(1.2)), 32, "1.2"}, {math.Inf(1), 64, "Infinity"}, {math.Inf(-1), 64, "-Infinity"}, {math.NaN(), 64, "NaN"},
	} {
		require.Equal(t, test.want, FormatFloat(test.value, test.bits))
		if math.IsInf(test.value, 0) || math.IsNaN(test.value) {
			require.Equal(t, test.want, PropertyJSON.Float(test.value, test.bits))
		}
	}
}

func TestNumericWireJSONEnvelope(t *testing.T) {
	type Embedded struct {
		Promoted float64
		Hidden   string `json:"-"`
	}
	type envelope struct {
		Embedded
		Value  float64                `json:"value"`
		Quoted float64                `json:"quoted,string"`
		Empty  float64                `json:"empty,omitempty"`
		Zero   float64                `json:"zero"`
		Binary []byte                 `json:"binary"`
		Nested map[string]interface{} `json:"nested"`
		Raw    json.RawMessage        `json:"raw"`
		Time   time.Time              `json:"time"`
		Nil    *int                   `json:"nil"`
	}
	value := envelope{Embedded: Embedded{Promoted: 3, Hidden: "secret"}, Value: 2, Quoted: 2,
		Binary: []byte{1, 2}, Raw: json.RawMessage(`{"v":2.0}`), Time: time.Unix(0, 0).UTC(),
		Nested: map[string]interface{}{"large": uint64(math.MaxUint64), "signed": int64(9007199254740993), "list": []float32{2, 1.25}}}
	raw, err := PropertyJSON.Marshal(value)
	require.NoError(t, err)
	indented, err := PropertyJSON.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	require.JSONEq(t, string(raw), string(indented))
	var actual map[string]interface{}
	require.NoError(t, PropertyJSON.Unmarshal(raw, &actual))
	require.Equal(t, float64(2), actual["value"])
	require.Equal(t, float64(3), actual["Promoted"])
	require.Equal(t, float64(0), actual["zero"])
	require.Equal(t, "2", actual["quoted"])
	require.Equal(t, "AQI=", actual["binary"])
	require.Equal(t, "1970-01-01T00:00:00Z", actual["time"])
	require.Contains(t, actual, "nil")
	require.Nil(t, actual["nil"])
	require.NotContains(t, actual, "empty")
	require.NotContains(t, actual, "Hidden")
	require.Equal(t, float64(2), actual["raw"].(map[string]interface{})["v"])
	require.Equal(t, uint64(math.MaxUint64), actual["nested"].(map[string]interface{})["large"])
	require.Equal(t, []interface{}{float64(2), 1.25}, actual["nested"].(map[string]interface{})["list"])
	require.IsType(t, float64(0), value.Value) // Encoding borrows the original DTO.
	var buffer bytes.Buffer
	encoder := PropertyJSON.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	require.NoError(t, encoder.Encode(map[string]interface{}{"v": float64(2), "s": "<x>"}))
	require.Contains(t, buffer.String(), `"v": 2.0`)
	require.Contains(t, buffer.String(), `"<x>"`)
}

func TestNumericWireJSONCompatibility(t *testing.T) {
	custom := &struct{ Value numericWireCustomFloat }{2}
	customJSON, err := PropertyJSON.Marshal(custom)
	require.NoError(t, err)
	require.JSONEq(t, `{"Value":"custom"}`, string(customJSON))
	type A struct {
		V     int
		Other int `json:"chosen"`
	}
	type B struct {
		V      int
		Choice int `json:"chosen"`
	}
	type C struct {
		A
		B
		V int
	}
	type Recursive struct {
		*Recursive
		V int
	}
	for _, value := range []interface{}{
		C{A: A{V: 1, Other: 2}, B: B{V: 3, Choice: 4}, V: 5},
		struct {
			A
			Chosen int `json:"chosen"`
		}{A: A{Other: 1}, Chosen: 2},
		struct {
			Nil   *A
			Empty []byte
			Omit  *A `json:",omitempty"`
		}{},
		map[int]string{1: "a", 2: "b"}, json.Number("2.0"),
		struct{ *A }{},
		struct{ *A }{&A{V: 1}},
		struct {
			*A
			B
		}{A: &A{V: 1}, B: B{V: 2}},
		struct {
			S      string    `json:",omitempty"`
			Z      int       `json:",omitzero"`
			Time   time.Time `json:",omitzero"`
			Ignore int       `json:"-"`
		}{},
		struct {
			Zero    int  `json:",omitzero"`
			Nil     *int `json:",omitzero"`
			Present int  `json:",omitzero"`
		}{Present: 1},
		struct {
			ignored int
			X       int `json:"invalid\\name"`
		}{X: 1},
		struct {
			Value numericWireZero `json:",omitzero"`
			Any   interface{}     `json:",omitzero"`
		}{Any: &numericWireZero{}},
		struct {
			Value *numericWireZero `json:",omitzero"`
		}{},
		struct {
			Value interface{ IsZero() bool } `json:",omitzero"`
		}{Value: (*numericWireZero)(nil)},
		struct{ Value numericWireText }{Value: "2"},
		&struct{ Value numericWireText }{Value: "2"},
		Recursive{V: 1},
		struct {
			Array   [1]int          `json:",omitempty"`
			Present struct{ V int } `json:",omitempty"`
		}{},
	} {
		expected, err := json.Marshal(value)
		require.NoError(t, err)
		actual, err := PropertyJSON.Marshal(value)
		require.NoError(t, err)
		require.JSONEq(t, string(expected), string(actual))
	}
	cyclic := map[string]interface{}{}
	cyclic["self"] = cyclic
	for _, value := range []interface{}{cyclic, math.NaN(), math.Inf(1), make(chan int), map[bool]int{true: 1}} {
		_, err := PropertyJSON.Marshal(value)
		require.Error(t, err)
	}
	type recursive struct{ Next *recursive }
	pointer := &recursive{}
	pointer.Next = pointer
	_, err = PropertyJSON.Marshal(pointer)
	require.Error(t, err)
}

func TestNumericWireBoltDomain(t *testing.T) {
	_, numeric := AppendBoltNumber(nil, "2")
	require.False(t, numeric, "numeric text remains a STRING")
	for _, raw := range [][]byte{nil, {0xC0}, {0x80}} {
		_, _, recognized, err := DecodeBoltNumber(raw)
		require.False(t, recognized)
		require.NoError(t, err)
	}
	for _, value := range []interface{}{int64(math.MinInt64), int64(-129), int64(-17), int64(127), int64(32768), int64(math.MaxInt64), uint64(math.MaxInt64), float64(2), math.Copysign(0, -1)} {
		raw, ok := AppendBoltNumber(nil, value)
		require.True(t, ok)
		decoded, size, ok, err := DecodeBoltNumber(raw)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, len(raw), size)
		if unsigned, ok := value.(uint64); ok {
			require.Equal(t, int64(unsigned), decoded)
		} else {
			require.Equal(t, value, decoded)
		}
		if raw[0] == 0xC1 {
			require.Equal(t, math.Float64bits(value.(float64)), math.Float64bits(decoded.(float64)))
		}
		for end := 1; end < len(raw); end++ {
			_, _, ok, err := DecodeBoltNumber(raw[:end])
			require.True(t, ok)
			require.Error(t, err)
		}
	}
	for _, value := range []uint64{uint64(math.MaxInt64) + 1, math.MaxUint64} {
		raw, ok := AppendBoltNumber(nil, value)
		require.True(t, ok)
		require.Equal(t, byte(0xD0), raw[0])
		require.Equal(t, int(raw[1]), len(raw)-2)
		if value == math.MaxUint64 {
			require.Equal(t, "18446744073709551615", string(raw[2:]))
		}
	}
}
