package convert_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/convert"
)

type writerTextKey int

type numericRecursiveA struct {
	Next  *numericRecursiveB
	Value any
}
type numericRecursiveB struct{ Parent *numericRecursiveA }

func TestNumericWireDecoderRemainsOpaque(t *testing.T) {
	standard := reflect.TypeOf((*json.Decoder)(nil))
	for _, decoder := range []any{
		convert.PropertyJSON.NewDecoder(strings.NewReader(`{}`)),
		convert.CypherInputJSON.NewDecoder(strings.NewReader(`{}`)),
	} {
		typ := reflect.TypeOf(decoder)
		if typ.ConvertibleTo(standard) {
			t.Fatalf("domain decoder exposes the standard decoder by conversion: %v", typ)
		}
		for i := 0; i < typ.Elem().NumField(); i++ {
			if typ.Elem().Field(i).IsExported() {
				t.Fatalf("domain decoder exposes a field: %v", typ.Elem().Field(i))
			}
		}
		if _, exposed := typ.MethodByName("UseNumber"); exposed {
			t.Fatalf("number policy remains caller-selectable: %v", typ)
		}
	}
}

func TestNumericWireRecursiveDecoderPlans(t *testing.T) {
	const document = `{"Next":{"Parent":{"Value":18446744073709551615}},"Value":9007199254740993}`
	var property, input numericRecursiveA
	if err := convert.PropertyJSON.Unmarshal([]byte(document), &property); err != nil {
		t.Fatal(err)
	}
	// The input domain's own (Neo4j) policy reaches the nested leaf: it rejects
	// the uint64 value the property plan accepted, so the plan was not shared.
	var unsupported *convert.UnsupportedNumberError
	if err := convert.CypherInputJSON.Unmarshal([]byte(document), &input); !errors.As(err, &unsupported) {
		t.Fatalf("recursive plan captured the property policy: %v %T", err, input.Next.Parent.Value)
	}
	if property.Value != int64(9007199254740993) || property.Next.Parent.Value != uint64(math.MaxUint64) {
		t.Fatalf("recursive property plan lost exact values: %#v %#v", property.Value, property.Next.Parent.Value)
	}
	var rootedAtB numericRecursiveB
	if err := convert.PropertyJSON.Unmarshal([]byte(`{"Parent":{"Value":18446744073709551615}}`), &rootedAtB); err != nil {
		t.Fatal(err)
	}
	if rootedAtB.Parent.Value != uint64(math.MaxUint64) {
		t.Fatalf("partial recursive type plan was cached: %T %v", rootedAtB.Parent.Value, rootedAtB.Parent.Value)
	}
}

func (value writerTextKey) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("key:%d", value)), nil
}

type writerJSONValue struct{ calls *int }

func (value writerJSONValue) MarshalJSON() ([]byte, error) {
	*value.calls++
	return []byte(` {"whole":2.0,"html":"<>&","line":"\u2028"} `), nil
}

func TestNumericWireWriterEnvelopeCompatibility(t *testing.T) {
	type promoted struct{ Visible string }
	values := []any{
		map[string]any{"key<>&": "<>&\u2028\u2029\x00\b\f\n\r\t\x01\"\\", "invalid\xff": "\xff\xfe"},
		map[writerTextKey]string{2: "two", 1: "one"},
		map[int]string{10: "ten", -2: "minus two"},
		struct {
			promoted
			Quoted string `json:",string"`
			Number int64  `json:"number,string"`
			Empty  string `json:",omitempty"`
		}{promoted: promoted{"visible"}, Quoted: "<>&\n", Number: math.MinInt64},
		struct {
			Nil     *int
			Bytes   []byte
			Raw     json.RawMessage
			Created time.Time
		}{Bytes: []byte{0, 255, 1}, Raw: json.RawMessage(` {"raw":2.0,"html":"<>&"} `), Created: time.Date(2026, 1, 2, 3, 4, 5, 123, time.UTC)},
		json.Number("2.0"),
		[]any{nil, true, "x", int64(math.MinInt64), uint64(math.MaxUint64)},
	}
	for i, value := range values {
		for _, escape := range []bool{true, false} {
			var expected, actual bytes.Buffer
			// Author: #1665 implementation lane (Codex). TJ's retained string
			// and framing owner is the reference; numeric policy stays above it.
			standard := jsontext.NewEncoder(&expected)
			standard.SetEscapeHTML(escape)
			if err := standard.Encode(value); err != nil {
				t.Fatal(err)
			}
			owned := convert.PropertyJSON.NewEncoder(&actual)
			owned.SetEscapeHTML(escape)
			owned.SetIndent("", "  ")
			if err := owned.Encode(value); err != nil {
				t.Fatal(err)
			}
			wantTree, err := jsontext.UnmarshalValue(expected.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			gotTree, err := jsontext.UnmarshalValue(actual.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotTree, wantTree) {
				t.Errorf("case %d escape=%v: lossless string/number tree differs: got %q, want %q", i, escape, actual.String(), expected.String())
			}
			if !strings.HasSuffix(actual.String(), "\n") {
				t.Fatal("encoder omitted the frame newline")
			}
			if len(expected.Bytes()) > 0 && (expected.Bytes()[0] == '{' || expected.Bytes()[0] == '[') && !strings.Contains(actual.String(), "\n  ") {
				t.Fatal("encoder omitted requested indentation")
			}
			if escape && strings.ContainsAny(actual.String(), "<>&") {
				t.Fatal("encoder omitted requested HTML escaping")
			}
		}
	}
	for _, escape := range []bool{true, false} {
		calls := 0
		var output bytes.Buffer
		owned := convert.PropertyJSON.NewEncoder(&output)
		owned.SetEscapeHTML(escape)
		if err := owned.Encode(map[string]any{"value": writerJSONValue{&calls}}); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("custom marshaler called %d times, want once", calls)
		}
		if !strings.Contains(output.String(), `"whole":2.0`) {
			t.Fatalf("opaque FLOAT changed: %s", output.String())
		}
		if escape && strings.Contains(output.String(), "<>&") {
			t.Fatal("custom JSON bypassed HTML escaping")
		}
	}
}

func TestNumericWireWriterErrors(t *testing.T) {
	type namedFloat32 float32
	for _, value := range []any{float32(math.NaN()), namedFloat32(math.Inf(1)), math.Inf(-1), json.Number("invalid"), make(chan int), map[bool]int{true: 1}, map[bool]int(nil)} {
		_, expected := json.Marshal(value)
		// Author: #1665 implementation lane (Codex). Unsupported opaque Go
		// types use the canonical binder's error; numeric errors retain their
		// domain contract and the standard numeric reference above.
		switch value.(type) {
		case chan int, map[bool]int:
			_, expected = jsontext.Marshal(value)
		}
		_, actual := convert.PropertyJSON.Marshal(value)
		if expected == nil || actual == nil {
			t.Fatalf("missing error for %T: std=%v owned=%v", value, expected, actual)
		}
		if reflect.TypeOf(expected) != reflect.TypeOf(actual) || expected.Error() != actual.Error() {
			t.Errorf("%T: got %T %v, want %T %v", value, actual, actual, expected, expected)
		}
	}
	boom := errors.New("writer failed")
	output := &numericErrorWriter{err: boom}
	encoder := convert.PropertyJSON.NewEncoder(output)
	if err := encoder.Encode(map[string]any{"v": float64(2)}); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if err := encoder.Encode(make(chan int)); !errors.Is(err, boom) {
		t.Fatalf("writer error was not sticky: %v", err)
	}
	if output.calls != 1 {
		t.Fatalf("failed writer invoked %d times", output.calls)
	}
}

func TestNumericWireWriterIgnoredStringOption(t *testing.T) {
	value := struct {
		Properties map[string]any `json:"properties,string"`
		Values     []float64      `json:"values,string"`
	}{Properties: map[string]any{"whole": float64(2), "signed": int64(9007199254740993)}, Values: []float64{3, math.SmallestNonzeroFloat64}}
	data, err := convert.PropertyJSON.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Properties map[string]any `json:"properties"`
		Values     []any          `json:"values"`
	}
	if err := convert.PropertyJSON.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Properties["whole"] != float64(2) || decoded.Properties["signed"] != int64(9007199254740993) || decoded.Values[0] != float64(3) || decoded.Values[1] != math.SmallestNonzeroFloat64 {
		t.Fatalf("an ignored ,string option changed numeric types: %s", data)
	}
}

type numericErrorWriter struct {
	err   error
	calls int
}

func (writer *numericErrorWriter) Write(data []byte) (int, error) {
	writer.calls++
	return 0, writer.err
}
