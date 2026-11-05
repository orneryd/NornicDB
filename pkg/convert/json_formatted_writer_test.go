package convert_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/convert"
)

// formattedSink retains one formatted document so the compiler cannot remove
// the measured work.
var formattedSink []byte

// formattedCustomContainer is an opaque marshaler that returns a container
// with loose whitespace: the domain writer's fragment indentation must place it
// at its parent container's depth exactly like encoding/json does.
type formattedCustomContainer struct{ calls *int }

func (value formattedCustomContainer) MarshalJSON() ([]byte, error) {
	*value.calls++
	return []byte(` { "nested" : [1, {"deep": true}] } `), nil
}

// formattedOracle is the relation MarshalIndent owns: formatted output equals
// encoding/json's indentation of the domain writer's compact output. Comparing
// against the compact owner keeps FLOAT spelling (for example "2.0") in the
// domain writer and leaves only the indentation decision under test.
func formattedOracle(t *testing.T, value interface{}, prefix, indent string) {
	t.Helper()
	compact, err := convert.PropertyJSON.Marshal(value)
	if err != nil {
		t.Fatalf("compact %s prefix %q indent %q: %v", formattedName(value), prefix, indent, err)
	}
	var want bytes.Buffer
	if err := json.Indent(&want, compact, prefix, indent); err != nil {
		t.Fatalf("compact %s is not valid JSON: %v", formattedName(value), err)
	}
	got, err := convert.PropertyJSON.MarshalIndent(value, prefix, indent)
	if err != nil {
		t.Fatalf("formatted %s prefix %q indent %q: %v", formattedName(value), prefix, indent, err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("formatted %s prefix %q indent %q differs from the compact indentation oracle\n got: %s\nwant: %s", formattedName(value), prefix, indent, got, want.Bytes())
	}
	formattedSink = got
}

func formattedName(value interface{}) string {
	if value == nil {
		return "nil"
	}
	return fmt.Sprintf("%T", value)
}

// formattedShapes are the structural cases the writer's own formatting
// operations must cover: empty containers, every nesting depth, quoted
// punctuation and escapes, opaque fragments and FLOAT text.
func formattedShapes() []interface{} {
	return []interface{}{
		nil,
		true,
		int64(-1),
		float64(2),
		math.Copysign(0, -1),
		"quoted {,}:\"[]{}",
		map[string]any{},
		[]any{},
		map[string]any{"a": 1},
		map[string]any{"a": map[string]any{}, "b": []any{}, "c": []any{map[string]any{}, []any{}, map[string]any{"d": []any{}}}},
		[]any{1, 2, 3, map[string]any{"a": []any{true, false, nil}}},
		map[string]any{"a": map[string]any{"b": map[string]any{"c": []any{1, []any{2, []any{3, []any{4}}}}}}},
		map[string]any{"s": "{,}:\"[]{}", "t": "a\\b\\\\c", "u": "\u00e9\U0001f600\u2028"},
		map[string]any{"wide": strings.Repeat("x", 200)},
		[]any{[]any{[]any{[]any{[]any{[]any{[]any{[]any{[]any{[]any{1}}}}}}}}}},
		map[string]any{"raw": json.RawMessage(` { "a" : [ 1 , { } ] } `)},
		json.RawMessage(`{"top":[{"nested":true}]}`),
		[]any{map[string]any{"inner": json.RawMessage(`{"a":[{"b":1}]}`)}},
		struct {
			Raw   json.RawMessage `json:"raw"`
			At    time.Time       `json:"at"`
			Bytes []byte          `json:"bytes"`
		}{Raw: json.RawMessage(`{"deep":[{"value":1}]}`), At: time.Unix(0, 0).UTC(), Bytes: []byte{0, 255, 1}},
		formattedDocument(formattedValues(32, 1.25)),
		formattedDeepDocument(8),
	}
}

// TestPropertyJSONIndentMatchesCompactIndent checks the formatting contract for
// every structural shape and prefix/indent spelling, including the empty
// prefix and indent MarshalIndent must still pretty-print.
func TestPropertyJSONIndentMatchesCompactIndent(t *testing.T) {
	prefixes := [][2]string{
		{"", "  "},
		{"", "\t"},
		{"", ""},
		{"  ", "    "},
		{"> ", "| "},
		{"\u00b7 ", "\u00a0\u00a0"},
		{"  ", ""},
		{strings.Repeat(">", 8), ""},
		{"", strings.Repeat(" ", 40)},
		{strings.Repeat(">", 20), strings.Repeat(" ", 60)},
	}
	for _, shape := range formattedShapes() {
		for _, spec := range prefixes {
			formattedOracle(t, shape, spec[0], spec[1])
		}
	}
}

// TestPropertyJSONIndentMatchesCompactIndentForRandomDocuments repeats the
// contract over generated mixed-depth documents, including punctuation and
// escapes inside strings, at randomly selected prefixes.
func TestPropertyJSONIndentMatchesCompactIndentForRandomDocuments(t *testing.T) {
	random := rand.New(rand.NewSource(1665))
	prefixes := [][2]string{
		{"", "  "}, {"", "\t"}, {"", ""}, {"  ", "    "}, {"> ", "| "}, {"", strings.Repeat(" ", 32)},
	}
	for iteration := 0; iteration < 400; iteration++ {
		document := formattedRandomValue(random, iteration%6)
		spec := prefixes[random.Intn(len(prefixes))]
		formattedOracle(t, document, spec[0], spec[1])
	}
}

// TestPropertyJSONMarshalIndentEmptyStringsStillPretty keeps the standard
// library contract: empty prefix and indent disable streaming indentation but
// MarshalIndent still writes line breaks, indentation-free, plus a space after
// each colon.
func TestPropertyJSONMarshalIndentEmptyStringsStillPretty(t *testing.T) {
	document := map[string]any{"a": 1, "nested": map[string]any{"b": []any{1, 2}}}
	formattedOracle(t, document, "", "")
	formatted, err := convert.PropertyJSON.MarshalIndent(document, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(formatted), "\n") || !strings.Contains(string(formatted), `"a": 1`) {
		t.Fatalf("MarshalIndent(\"\", \"\") is not pretty: %q", formatted)
	}
	var compact bytes.Buffer
	if err := convert.PropertyJSON.NewEncoder(&compact).Encode(document); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	encoder := convert.PropertyJSON.NewEncoder(&stream)
	encoder.SetIndent("", "")
	if err := encoder.Encode(document); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(compact.Bytes(), stream.Bytes()) {
		t.Fatalf("SetIndent(\"\", \"\") did not disable stream formatting\n got: %q\nwant: %q", stream.Bytes(), compact.Bytes())
	}
}

// TestPropertyJSONEncoderIndentOutput keeps streaming Encode on the same
// formatted bytes as indenting its own compact frame, across HTML escaping and
// prefix/indent spellings.
func TestPropertyJSONEncoderIndentOutput(t *testing.T) {
	for _, value := range []interface{}{
		map[string]any{"v": float64(2), "zero": math.Copysign(0, -1), "s": "<x>&", "list": []any{}},
		[]any{map[string]any{"a": json.RawMessage(`{"b":[1,{}]}`)}, "line\u2028", nil},
		formattedDocument(formattedValues(8, int64(2))),
	} {
		for _, escape := range []bool{true, false} {
			var compact bytes.Buffer
			compactEncoder := convert.PropertyJSON.NewEncoder(&compact)
			compactEncoder.SetEscapeHTML(escape)
			if err := compactEncoder.Encode(value); err != nil {
				t.Fatal(err)
			}
			if !bytes.HasSuffix(compact.Bytes(), []byte("\n")) {
				t.Fatal("compact frame omitted its newline")
			}
			body := compact.Bytes()[:compact.Len()-1]
			for _, spec := range [][2]string{{"", "  "}, {"> ", "    "}, {"", "\t"}} {
				var want bytes.Buffer
				if err := json.Indent(&want, body, spec[0], spec[1]); err != nil {
					t.Fatalf("%T: %v", value, err)
				}
				want.WriteByte('\n')
				var got bytes.Buffer
				encoder := convert.PropertyJSON.NewEncoder(&got)
				encoder.SetEscapeHTML(escape)
				encoder.SetIndent(spec[0], spec[1])
				if err := encoder.Encode(value); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got.Bytes(), want.Bytes()) {
					t.Fatalf("%T escape=%v prefix %q indent %q: formatted stream differs\n got: %q\nwant: %q", value, escape, spec[0], spec[1], got.Bytes(), want.Bytes())
				}
				if escape && bytes.Contains(got.Bytes(), []byte("<x>&")) {
					t.Fatal("indented stream omitted HTML escaping")
				}
			}
		}
	}
}

// TestPropertyJSONEncoderIndentOpaqueAndErrors keeps the custom marshaler
// invocation count, nonfinite rejection, cycle rejection and sticky writer
// error contracts under streamed formatting.
func TestPropertyJSONEncoderIndentOpaqueAndErrors(t *testing.T) {
	for _, spec := range [][2]string{{"", "  "}, {"", ""}, {"> ", "| "}} {
		calls := 0
		var output bytes.Buffer
		encoder := convert.PropertyJSON.NewEncoder(&output)
		encoder.SetIndent(spec[0], spec[1])
		if err := encoder.Encode(map[string]any{"value": formattedCustomContainer{&calls}}); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("custom marshaler called %d times, want once", calls)
		}
		if spec[0] != "" || spec[1] != "" {
			if !strings.Contains(output.String(), `"nested": [`) || !strings.Contains(output.String(), "\n") {
				t.Fatalf("opaque container fragment was not indented at its parent depth: %q", output.String())
			}
		} else if output.String() != "{\"value\":{\"nested\":[1,{\"deep\":true}]}}\n" {
			t.Fatalf("SetIndent(\"\", \"\") changed the compact opaque frame: %q", output.String())
		}

		for _, value := range []any{math.NaN(), math.Inf(1), float32(math.Inf(-1))} {
			_, marshalErr := convert.PropertyJSON.Marshal(value)
			var rejected bytes.Buffer
			brokenEncoder := convert.PropertyJSON.NewEncoder(&rejected)
			brokenEncoder.SetIndent(spec[0], spec[1])
			encodeErr := brokenEncoder.Encode(value)
			if marshalErr == nil || encodeErr == nil || marshalErr.Error() != encodeErr.Error() {
				t.Fatalf("%T: formatted stream error %v, compact error %v", value, encodeErr, marshalErr)
			}
		}

		cycle := map[string]any{}
		cycle["self"] = cycle
		_, marshalErr := convert.PropertyJSON.Marshal(cycle)
		var rejected bytes.Buffer
		cycleEncoder := convert.PropertyJSON.NewEncoder(&rejected)
		cycleEncoder.SetIndent(spec[0], spec[1])
		cycleErr := cycleEncoder.Encode(cycle)
		if marshalErr == nil || cycleErr == nil || marshalErr.Error() != cycleErr.Error() {
			t.Fatalf("cycle: formatted stream error %v, compact error %v", cycleErr, marshalErr)
		}

		if _, err := convert.PropertyJSON.MarshalIndent(cycle, spec[0], spec[1]); err == nil {
			t.Fatal("MarshalIndent accepted a cycle")
		}
		if _, err := convert.PropertyJSON.MarshalIndent(math.NaN(), spec[0], spec[1]); err == nil {
			t.Fatal("MarshalIndent accepted NaN")
		}
	}

	boom := fmt.Errorf("writer failed")
	writer := &formattedErrorWriter{err: boom}
	encoder := convert.PropertyJSON.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(map[string]any{"v": float64(2)}); err == nil || err.Error() != boom.Error() {
		t.Fatalf("writer error: %v", err)
	}
	if err := encoder.Encode(map[string]any{"v": float64(3)}); err == nil || err.Error() != boom.Error() {
		t.Fatalf("writer error was not sticky: %v", err)
	}
	if writer.calls != 1 {
		t.Fatalf("failed writer invoked %d times, want once", writer.calls)
	}
}

// TestPropertyJSONMarshalStateUnaffected keeps the compact typed-state contract
// outside the formatted writer: declared numeric fields keep stdlib bytes and
// dynamic values keep their exact FLOAT tokens, with no added formatting.
func TestPropertyJSONMarshalStateUnaffected(t *testing.T) {
	type metadata struct {
		Score float64
		Small float32
		Any   any
	}
	for _, value := range []metadata{
		{Score: 2, Small: 0.5, Any: float64(2)},
		{Score: -1e20, Small: float32(1e-6), Any: math.Copysign(0, -1)},
	} {
		want, err := json.Marshal(struct {
			Score float64
			Small float32
		}{value.Score, value.Small})
		if err != nil {
			t.Fatal(err)
		}
		got, err := convert.PropertyJSON.MarshalState(value)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(got, want[:len(want)-1]) {
			t.Fatalf("declared fields changed: got %s, want prefix %s", got, want)
		}
		if bytes.Contains(got, []byte("\n")) {
			t.Fatalf("MarshalState formatted its output: %s", got)
		}
	}
	dynamic, err := convert.PropertyJSON.MarshalState(struct{ Value any }{float64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if string(dynamic) != `{"Value":2.0}` {
		t.Fatalf("dynamic FLOAT identity changed: %s", dynamic)
	}
	negativeZero, err := convert.PropertyJSON.MarshalState(struct{ Value any }{math.Copysign(0, -1)})
	if err != nil {
		t.Fatal(err)
	}
	if string(negativeZero) != `{"Value":-0.0}` {
		t.Fatalf("dynamic negative zero changed: %s", negativeZero)
	}
}

func formattedDocument(values []any) map[string]any {
	return map[string]any{
		"nodes": []any{map[string]any{
			"id":         int64(1),
			"labels":     []any{"Probe"},
			"properties": map[string]any{"value": values},
		}},
		"relationships": nil,
	}
}

func formattedValues(count int, value any) []any {
	values := make([]any, count)
	for i := range values {
		values[i] = value
	}
	return values
}

func formattedDeepDocument(depth int) any {
	value := any(formattedDocument(formattedValues(1, int64(1))))
	for i := 0; i < depth; i++ {
		value = []any{value}
	}
	return value
}

func formattedRandomValue(random *rand.Rand, depth int) interface{} {
	if depth <= 0 {
		switch random.Intn(6) {
		case 0:
			return nil
		case 1:
			return random.Intn(2000) - 1000
		case 2:
			return float64(random.Intn(2001)-1000) / 8
		case 3:
			return random.Intn(2) == 0
		case 4:
			return formattedRandomString(random)
		default:
			return int64(random.Intn(1 << 20))
		}
	}
	switch random.Intn(4) {
	case 0:
		values := make([]any, random.Intn(5))
		for i := range values {
			values[i] = formattedRandomValue(random, depth-1)
		}
		return values
	case 1:
		object := make(map[string]any, 4)
		for i := 0; i < random.Intn(5); i++ {
			object[fmt.Sprintf("k%d", i)] = formattedRandomValue(random, depth-1)
		}
		return object
	default:
		return formattedRandomValue(random, 0)
	}
}

func formattedRandomString(random *rand.Rand) string {
	alphabet := []string{"a", "Z", "0", "{", "}", "[", "]", ",", ":", " ", "\"", "\\", "\n", "\t", "\u00e9", "\U0001f600", "\u2028", "'"}
	var builder strings.Builder
	for i := 0; i < random.Intn(12); i++ {
		builder.WriteString(alphabet[random.Intn(len(alphabet))])
	}
	return builder.String()
}

type formattedErrorWriter struct {
	err   error
	calls int
}

func (writer *formattedErrorWriter) Write(data []byte) (int, error) {
	writer.calls++
	return 0, writer.err
}
