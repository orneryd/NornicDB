package jsontext

import (
	"bytes"
	std "encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

func TestJSONExactStringAndNumberTree(t *testing.T) {
	high := string(cyphertext.AppendCodepoint(nil, 0xd800))
	low := string(cyphertext.AppendCodepoint(nil, 0xdc00))
	cases := []struct {
		raw  string
		want any
	}{
		{`{"\ud800":"\udc00","\udc00":"\ud800"}`, map[string]any{high: low, low: high}},
		{`{"literal\\uD800":"\\ud800","pair":"\ud83d\ude00"}`, map[string]any{`literal\uD800`: `\ud800`, "pair": "😀"}},
		{`{"a\ud800":1,"a\ud800":2,"a\udc00":3,"replacement":"\ufffd"}`, map[string]any{"a" + high: std.Number("2"), "a" + low: std.Number("3"), "replacement": "�"}},
		{`{"large":9007199254740993,"exponent":1e100,"negativeZero":-0,"array":[true,null,"\ud800"]}`, map[string]any{"large": std.Number("9007199254740993"), "exponent": std.Number("1e100"), "negativeZero": std.Number("-0"), "array": []any{true, nil, high}}},
		{`"\u005cuD800"`, `\uD800`},
		{`[]`, []any{}}, {`{}`, map[string]any{}}, {`null`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := UnmarshalValue([]byte(tc.raw))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			wire, err := Marshal(got)
			require.NoError(t, err)
			require.True(t, std.Valid(wire))
			require.True(t, utf8.Valid(wire), "wire cannot contain raw WTF8")
			back, err := UnmarshalValue(wire)
			require.NoError(t, err)
			require.True(t, reflect.DeepEqual(got, back))
		})
	}
}

func TestJSONTypedBindingDynamicHooks(t *testing.T) {
	type request struct {
		Statement  string     `json:"statement"`
		Parameters NumberMap  `json:"parameters"`
		Metadata   NumberMap  `json:"tx_metadata"`
		List       NumberList `json:"list"`
		Raw        RawMessage `json:"raw"`
	}
	raw := []byte(`{"statement":"RETURN '\ud800'","parameters":{"\ud800":9007199254740993,"nested":{"s":"\udc00","i":9007199254740993}},"tx_metadata":{"unit":"\udc00"},"list":[9007199254740993,"\ud800"],"raw":{"\ud800":"\udc00"}}`)
	high := string(cyphertext.AppendCodepoint(nil, 0xd800))
	low := string(cyphertext.AppendCodepoint(nil, 0xdc00))
	var got request
	require.NoError(t, Unmarshal(raw, &got))
	require.Equal(t, "RETURN '"+high+"'", got.Statement)
	require.Equal(t, std.Number("9007199254740993"), got.Parameters[high])
	require.Equal(t, low, got.Metadata["unit"])
	require.Equal(t, []any{std.Number("9007199254740993"), high}, []any(got.List))
	require.True(t, std.Valid(got.Raw))
	require.True(t, utf8.Valid(got.Raw))
	require.Contains(t, string(got.Raw), `\ud800`)
	wire, err := Marshal(got)
	require.NoError(t, err)
	require.True(t, std.Valid(wire))
	require.True(t, utf8.Valid(wire))
	var back request
	require.NoError(t, Unmarshal(wire, &back))
	require.Equal(t, got, back)
	for _, bad := range []string{`{"parameters":[]}`, `{"list":{}}`} {
		require.Error(t, Unmarshal([]byte(bad), &back))
	}
}

func TestJSONExistingValidPayloadSemantics(t *testing.T) {
	values := []any{"ascii", "café", "😀", "<&>\u2028\u2029", map[string]any{"b": std.Number("9007199254740993"), "a": []any{float64(1), nil, true, `\uD800`}}, []byte{0, 1, 255}, map[string]string{"<key>": "\n\r\t\b\f\"\\"}, std.RawMessage(`{"literal":"\\ud800"}`), float64(1e30)}
	for _, value := range values {
		old, err := std.Marshal(value)
		require.NoError(t, err)
		got, err := Marshal(value)
		require.NoError(t, err)
		var oldValue, newValue any
		require.NoError(t, std.Unmarshal(old, &oldValue))
		require.NoError(t, std.Unmarshal(got, &newValue))
		require.Equal(t, oldValue, newValue, "existing payload meaning for %T", value)
	}
	for _, raw := range []string{`{"s":"valid","n":123}`, `{"s":"literal\\uD800","n":0}`, `{"s":"😀","n":-2}`} {
		var old, got struct {
			S string `json:"s"`
			N int    `json:"n"`
		}
		require.NoError(t, std.Unmarshal([]byte(raw), &old))
		require.NoError(t, Unmarshal([]byte(raw), &got))
		require.Equal(t, old, got)
	}
}

func TestJSONFramingAndInvalidBoundaries(t *testing.T) {
	for _, raw := range []string{`{"invalid":"\uZZZZ"}`, `{"truncated":"\ud80"}`, `{"trailing":1} {}`, `{"missing":}`, `"unterminated`, `1e1000x`} {
		var out any
		require.Error(t, Unmarshal([]byte(raw), &out), raw)
		_, err := UnmarshalValue([]byte(raw))
		require.Error(t, err, raw)
	}
	decoder := NewDecoder(strings.NewReader(`{"a":9007199254740993} ["\ud800"]`))
	first, err := decoder.DecodeValue()
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": std.Number("9007199254740993")}, first)
	second, err := decoder.DecodeValue()
	require.NoError(t, err)
	require.Len(t, second, 1)
	_, err = decoder.DecodeValue()
	require.ErrorIs(t, err, io.EOF)
	raw := append([]byte(`{"s":"`), 0xed, 0xa0, 0x80)
	raw = append(raw, []byte(`"}`)...)
	var old, got map[string]string
	require.NoError(t, std.Unmarshal(raw, &old))
	require.NoError(t, Unmarshal(raw, &got))
	require.Equal(t, old, got, "raw invalid UTF8 retains legacy replacement behavior")
	generic, err := UnmarshalValue(raw)
	require.NoError(t, err)
	require.Equal(t, old["s"], generic.(map[string]any)["s"])
	var limit map[string]any
	require.Error(t, NewDecoder(io.LimitReader(bytes.NewReader([]byte(`{"long":"value"}`)), 8)).Decode(&limit))
}

func TestJSONOriginalTypeErrorOffset(t *testing.T) {
	raw := []byte(`{"s":"\ud800","n":"bad"}`)
	var target struct {
		S string `json:"s"`
		N int    `json:"n"`
	}
	oldErr := std.Unmarshal(raw, &target)
	err := Unmarshal(raw, &target)
	require.Error(t, err)
	var oldType *std.UnmarshalTypeError
	require.ErrorAs(t, oldErr, &oldType)
	var current *std.UnmarshalTypeError
	require.ErrorAs(t, err, &current)
	require.Equal(t, oldType.Offset, current.Offset)
	require.Equal(t, oldType.Field, current.Field)
}

type countedJSON struct {
	calls   *int
	payload []byte
	failure error
}

func (v countedJSON) MarshalJSON() ([]byte, error) { *v.calls++; return v.payload, v.failure }
func TestJSONOpaqueRawAndSingleMarshalerInvocation(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`"\u0008\u000c"`), []byte("\"literal\u2028\u2029\""), []byte(`{"literal":"\\ud800"}`)} {
		var got struct {
			Raw RawMessage `json:"raw"`
		}
		input := append(append([]byte(`{"raw":`), raw...), '}')
		require.NoError(t, Unmarshal(input, &got))
		require.Equal(t, raw, []byte(got.Raw), "opaque valid JSON remains exact before enclosing serialization")
		calls := 0
		encoded, err := Marshal(countedJSON{calls: &calls, payload: raw})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		var old, new any
		require.NoError(t, std.Unmarshal(raw, &old))
		require.NoError(t, std.Unmarshal(encoded, &new))
		require.Equal(t, old, new)
	}
}

func TestJSONTypeErrorContractMatrix(t *testing.T) {
	type inner struct {
		N int `json:"number"`
	}
	type outer struct {
		Prefix string  `json:"prefix"`
		Inside inner   `json:"inside"`
		Items  []inner `json:"items"`
	}
	for _, raw := range []string{`{"prefix":"\ud800","inside":{"number":"bad"}}`, `{"prefix":"café","inside":{"number":[]}}`, `{"prefix":"\ud800","inside":{"number":{}}}`, `{"prefix":"\ud800","items":[{"number":true}]}`} {
		var before, after outer
		oldErr := std.Unmarshal([]byte(raw), &before)
		newErr := Unmarshal([]byte(raw), &after)
		var oldType, newType *std.UnmarshalTypeError
		require.ErrorAs(t, oldErr, &oldType)
		require.ErrorAs(t, newErr, &newType)
		require.Equal(t, oldType.Offset, newType.Offset, raw)
		require.Equal(t, oldType.Field, newType.Field, raw)
		require.Equal(t, oldType.Struct, newType.Struct, raw)
		require.Equal(t, oldType.Type, newType.Type, raw)
	}
}

func BenchmarkJSONValidTypedRequest(b *testing.B) {
	type request struct {
		Statement string `json:"statement"`
		Name      string `json:"name"`
		Count     int64  `json:"count"`
	}
	raw := []byte(`{"statement":"MATCH (n:Person) RETURN n.name","name":"ordinary café","count":9007199254740993}`)
	b.Run("stdlib", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var target request
			if err := std.Unmarshal(raw, &target); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("canonical", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var target request
			if err := Unmarshal(raw, &target); err != nil {
				b.Fatal(err)
			}
		}
	})
}
func BenchmarkJSONCatalogPayload(b *testing.B) {
	value := map[string]any{"nornic": map[string]any{"name": "nornic", "created_at": "2026-10-26T00:00:00Z", "options": map[string]any{"weight": float64(1), "description": "catalog ordinary value"}}, "composite": map[string]any{"name": "composite", "constituents": []any{map[string]any{"alias": "sales", "database_name": "sales"}}}}
	b.Run("stdlib", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := std.Marshal(value); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("canonical", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := Marshal(value); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestJSONAppendStringExactUnits(t *testing.T) {
	high := string(cyphertext.AppendCodepoint(nil, 0xd800))
	for _, value := range []string{"ordinary", `literal\ud800`, high, "a\b\f\n\"\\ café 😀\u2028"} {
		raw := AppendString(make([]byte, 0, 128), value)
		require.True(t, std.Valid(raw))
		require.True(t, utf8.Valid(raw))
		restored, err := UnmarshalValue(raw)
		require.NoError(t, err)
		require.Equal(t, value, restored)
	}
	require.Zero(t, testing.AllocsPerRun(100, func() { buf := make([]byte, 0, 64); _ = AppendString(buf, "ordinary") }), "ASCII appends retain zero allocations")
}

// Recursive value slices occur in graph component responses, including the
// finite leaf whose children are nil. Their type graph is recursive even
// though the encoded value is not cyclic.
func TestJSONRecursiveValueSlice(t *testing.T) {
	type component struct {
		Name       string         `json:"name"`
		Properties map[string]any `json:"properties,omitempty"`
		Children   []component    `json:"children,omitempty"`
	}
	want := component{Name: "root", Properties: map[string]any{"kind": "root"}, Children: []component{{Name: "leaf", Properties: map[string]any{"kind": "leaf"}}}}
	raw, err := Marshal(want)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"root","properties":{"kind":"root"},"children":[{"name":"leaf","properties":{"kind":"leaf"}}]}`, string(raw))
	var got component
	require.NoError(t, Unmarshal(raw, &got))
	require.Equal(t, want, got)
}
