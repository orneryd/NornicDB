package convert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

var indentCapacitySink []byte

// indentCapacityPreGrowth is the pre-change indented path: the compact document
// in the pooled state, then a fresh bytes.Buffer that json.Indent grows by
// itself. It is the allocation baseline for the capacity grown buffer.
func indentCapacityPreGrowth(value interface{}, prefix, indent string) ([]byte, error) {
	state := jsonWriterStates.Get().(*jsonWriterState)
	defer jsonWriterStates.Put(state)
	state.reset(true)
	if err := state.any(value); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := json.Indent(&output, state.data, prefix, indent); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

// TestIndentedJSONCapacityMatchesIndentExactly checks the counted capacity
// against encoding/json for the structural shapes Indent treats specially:
// empty containers, every depth of nesting, ':' and ',' punctuation, and
// quoted punctuation, escaped quotes and backslashes that are not structural.
// It also proves the counted capacity is enough: json.Indent writing into a
// buffer grown to it never reallocates and emits the same bytes.
func TestIndentedJSONCapacityMatchesIndentExactly(t *testing.T) {
	sources := []string{
		`{}`,
		`[]`,
		`null`,
		`0`,
		`-1.25e+21`,
		`"quoted {,}:\"[]{}"`,
		`{"a":1}`,
		`{"a":{},"b":[],"c":[{},[],{"d":[]}]}`,
		`[1,2,3,{"a":[true,false,null]}]`,
		`{"a":{"b":{"c":[1,[2,[3,[4]]]]}}}`,
		`{"s":"{,}:\"[]{}","t":"a\\b\\\\c","u":"\u00e9\ud83d\ude00\u2028"}`,
		`{"wide":"` + strings.Repeat("x", 200) + `"}`,
		`[[[[[[[[[[1]]]]]]]]]]`,
	}
	prefixes := [][2]string{
		{"", "  "},
		{"", "\t"},
		{"", ""},
		{"  ", "    "},
		{"> ", "| "},
		{"· ", "\u00a0\u00a0"},
		{"", strings.Repeat(" ", 40)},
		{strings.Repeat(">", 20), strings.Repeat(" ", 60)},
	}
	for _, source := range sources {
		for _, spec := range prefixes {
			exact, ok := indentedJSONCapacity([]byte(source), spec[0], spec[1])
			if !ok {
				t.Fatalf("source %q prefix %q indent %q: capacity reported as unrepresentable", source, spec[0], spec[1])
			}
			var reference bytes.Buffer
			if err := json.Indent(&reference, []byte(source), spec[0], spec[1]); err != nil {
				t.Fatalf("source %q is not valid compact JSON: %v", source, err)
			}
			want := reference.Len()
			if floor := 2 * len(source); floor > want {
				want = floor
			}
			if exact != want {
				t.Fatalf("source %q prefix %q indent %q: counted capacity %d, need %d", source, spec[0], spec[1], exact, want)
			}
			var output bytes.Buffer
			output.Grow(exact)
			grown := output.Cap()
			if err := json.Indent(&output, []byte(source), spec[0], spec[1]); err != nil {
				t.Fatalf("source %q: %v", source, err)
			}
			if output.Cap() != grown {
				t.Fatalf("source %q prefix %q indent %q: json.Indent reallocated a buffer grown to %d (capacity %d)", source, spec[0], spec[1], exact, output.Cap())
			}
			if !bytes.Equal(output.Bytes(), reference.Bytes()) {
				t.Fatalf("source %q prefix %q indent %q: indented bytes differ\n got: %s\nwant: %s", source, spec[0], spec[1], output.Bytes(), reference.Bytes())
			}
		}
	}
}

// TestIndentedJSONCapacityMatchesIndentForRandomDocuments repeats the exactness
// check over generated documents: mixed depths, empty containers, punctuation
// and escapes inside strings, and arbitrary prefix/indent strings.
func TestIndentedJSONCapacityMatchesIndentForRandomDocuments(t *testing.T) {
	random := rand.New(rand.NewSource(1665))
	prefixes := [][2]string{
		{"", "  "}, {"", "\t"}, {"", ""}, {"  ", "    "}, {"> ", "| "}, {"", strings.Repeat(" ", 32)},
	}
	for iteration := 0; iteration < 400; iteration++ {
		document := indentCapacityRandomValue(random, iteration%6)
		compact, err := PropertyJSON.Marshal(document)
		if err != nil {
			t.Fatalf("iteration %d: %v", iteration, err)
		}
		spec := prefixes[random.Intn(len(prefixes))]
		exact, ok := indentedJSONCapacity(compact, spec[0], spec[1])
		if !ok {
			t.Fatalf("iteration %d: capacity reported as unrepresentable", iteration)
		}
		var reference bytes.Buffer
		if err := json.Indent(&reference, compact, spec[0], spec[1]); err != nil {
			t.Fatalf("iteration %d: compact form rejected: %v", iteration, err)
		}
		want := reference.Len()
		if floor := 2 * len(compact); floor > want {
			want = floor
		}
		if exact != want {
			t.Fatalf("iteration %d: counted capacity %d, need %d for compact %s", iteration, exact, want, compact)
		}
		var output bytes.Buffer
		output.Grow(exact)
		grown := output.Cap()
		if err := json.Indent(&output, compact, spec[0], spec[1]); err != nil {
			t.Fatalf("iteration %d: %v", iteration, err)
		}
		if output.Cap() != grown || !bytes.Equal(output.Bytes(), reference.Bytes()) {
			t.Fatalf("iteration %d: pre-grown buffer reallocated or differs", iteration)
		}
	}
}

// TestIndentedJSONCapacityAllocationFree keeps the counting pass free of
// allocation: it may cost a second scan of the compact document, never a copy.
func TestIndentedJSONCapacityAllocationFree(t *testing.T) {
	document := indentCapacityDocument(64, 1.25)
	compact, err := PropertyJSON.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	var capacity int
	var ok bool
	allocations := testing.AllocsPerRun(100, func() {
		capacity, ok = indentedJSONCapacity(compact, "> ", "    ")
	})
	if allocations != 0 {
		t.Fatalf("%.0f allocations for one counting pass", allocations)
	}
	if !ok || capacity < len(compact) {
		t.Fatalf("capacity %d, ok %t for compact document of %d bytes", capacity, ok, len(compact))
	}
}

// TestPropertyJSONIndentCapacityReachesPublicPath proves the domain entry point
// returns a document whose buffer is at least the counted capacity and whose
// bytes are identical to the pre-change path for the same value.
func TestPropertyJSONIndentCapacityReachesPublicPath(t *testing.T) {
	for _, count := range []int{1, 8, 32, 64, 128, 4096} {
		for _, value := range []interface{}{int64(2), float64(2), 1.25} {
			document := indentCapacityDocument(count, value)
			compact, err := PropertyJSON.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			capacity, ok := indentedJSONCapacity(compact, "", "  ")
			if !ok {
				t.Fatalf("%d values: capacity reported as unrepresentable", count)
			}
			want, err := indentCapacityPreGrowth(document, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got, err := PropertyJSON.MarshalIndent(document, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("%d values %T: indented bytes differ from the pre-change path\n got: %s\nwant: %s", count, value, got, want)
			}
			if cap(got) < capacity {
				t.Fatalf("%d values %T: returned buffer capacity %d below counted capacity %d", count, value, cap(got), capacity)
			}
			// The allocation and byte win itself is reported by
			// BenchmarkPropertyJSONIndentCapacity, whose sub-benchmarks pair this path
			// with the pre-change path on the same document.
		}
	}
}

// indentCapacityDocument is the nested node document whose property array holds
// count copies of value: the shape numeric wire preservation produced.
func indentCapacityDocument(count int, value interface{}) map[string]any {
	values := make([]any, count)
	for i := range values {
		values[i] = value
	}
	return map[string]any{
		"nodes": []any{map[string]any{
			"id":         int64(1),
			"labels":     []any{"Probe"},
			"properties": map[string]any{"value": values},
		}},
		"relationships": nil,
	}
}

func indentCapacityRandomValue(random *rand.Rand, depth int) interface{} {
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
			return indentCapacityRandomString(random)
		default:
			return int64(random.Intn(1 << 20))
		}
	}
	switch random.Intn(4) {
	case 0:
		values := make([]any, random.Intn(5))
		for i := range values {
			values[i] = indentCapacityRandomValue(random, depth-1)
		}
		return values
	case 1:
		object := make(map[string]any, 4)
		for i := 0; i < random.Intn(5); i++ {
			object[fmt.Sprintf("k%d", i)] = indentCapacityRandomValue(random, depth-1)
		}
		return object
	default:
		return indentCapacityRandomValue(random, 0)
	}
}

func indentCapacityRandomString(random *rand.Rand) string {
	alphabet := []string{"a", "Z", "0", "{", "}", "[", "]", ",", ":", " ", "\"", "\\", "\n", "\t", "é", "😀", "\u2028", "'"}
	var builder strings.Builder
	for i := 0; i < random.Intn(12); i++ {
		builder.WriteString(alphabet[random.Intn(len(alphabet))])
	}
	return builder.String()
}

// indentCapacityDeepDocument nests count values under depth arrays.
func indentCapacityDeepDocument(count, depth int) interface{} {
	value := interface{}(indentCapacityDocument(count, int64(1)))
	for i := 0; i < depth; i++ {
		value = []any{value}
	}
	return value
}

// benchmarkIndentCapacity compares the pre-change path, the domain writer and
// encoding/json on one document, and reports the output and compact sizes so
// the byte counts are comparable across widths and indent strings.
func benchmarkIndentCapacity(b *testing.B, prefix, indent string, document interface{}) {
	b.Helper()
	domain, err := indentCapacityPreGrowth(document, prefix, indent)
	if err != nil {
		b.Fatal(err)
	}
	standard, err := json.MarshalIndent(document, prefix, indent)
	if err != nil {
		b.Fatal(err)
	}
	compact, err := PropertyJSON.Marshal(document)
	if err != nil {
		b.Fatal(err)
	}
	// The domain writer keeps FLOAT text that differs from encoding/json for a
	// whole float, so each path is checked against its own reference bytes.
	runs := []struct {
		name string
		want []byte
		run  func() ([]byte, error)
	}{
		{"before", domain, func() ([]byte, error) { return indentCapacityPreGrowth(document, prefix, indent) }},
		{"current", domain, func() ([]byte, error) { return PropertyJSON.MarshalIndent(document, prefix, indent) }},
		{"standard", standard, func() ([]byte, error) { return json.MarshalIndent(document, prefix, indent) }},
	}
	for _, run := range runs {
		b.Run(run.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(run.want)), "out-B")
			b.ReportMetric(float64(len(compact)), "compact-B")
			for iteration := 0; iteration < b.N; iteration++ {
				data, err := run.run()
				if err != nil {
					b.Fatal(err)
				}
				if iteration == 0 && !bytes.Equal(data, run.want) {
					b.Fatalf("%s output differs from its reference path\n got: %s\nwant: %s", run.name, data, run.want)
				}
				indentCapacitySink = data
			}
		})
	}
}

// BenchmarkPropertyJSONIndentCapacity reports the widths an export uses for
// INTEGER, whole FLOAT and fractional FLOAT values.
func BenchmarkPropertyJSONIndentCapacity(b *testing.B) {
	values := []struct {
		name  string
		value interface{}
	}{
		{"INTEGER", int64(2)},
		{"wholeFLOAT", float64(2)},
		{"fraction", 1.25},
	}
	for _, value := range values {
		for _, count := range []int{1, 8, 32, 64, 128} {
			document := indentCapacityDocument(count, value.value)
			name := value.name + "/" + strconv.Itoa(count)
			b.Run(name, func(b *testing.B) { benchmarkIndentCapacity(b, "", "  ", document) })
		}
	}
}

// BenchmarkPropertyJSONIndentCapacityShapes reports the structural cases the
// counting pass must handle: deep nesting, empty containers, quoted punctuation
// and escapes, and a wide prefix/indent.
func BenchmarkPropertyJSONIndentCapacityShapes(b *testing.B) {
	shapes := []struct {
		name           string
		prefix, indent string
		document       interface{}
	}{
		{"deepnest", "", "  ", indentCapacityDeepDocument(32, 8)},
		{"emptycontainers", "", "  ", map[string]any{"a": map[string]any{}, "b": []any{}, "c": []any{map[string]any{}, []any{}, map[string]any{"d": map[string]any{}}}}},
		{"quoted", "", "  ", map[string]any{"p": []any{"{,}:\"[]{}", "a\\b\\\\c", "e\u00e9\u2028f", strings.Repeat(`\"{}\",`, 16)}}},
		{"wide", strings.Repeat(">", 8), strings.Repeat(" ", 40), indentCapacityDocument(32, 1.25)},
		// A document dominated by one long string has an exact indented length
		// below json.Indent's 2x pre-growth, so the counted capacity keeps parity
		// with the pre-change path instead of improving on it.
		{"longstring", "", "  ", map[string]any{"wide": strings.Repeat("x", 200)}},
	}
	for _, shape := range shapes {
		b.Run(shape.name, func(b *testing.B) {
			benchmarkIndentCapacity(b, shape.prefix, shape.indent, shape.document)
		})
	}
}

// BenchmarkIndentedJSONCapacity reports the cost of the counting pass alone, so
// the extra scan is measured rather than assumed.
func BenchmarkIndentedJSONCapacity(b *testing.B) {
	for _, count := range []int{1, 8, 32, 64, 128} {
		compact, err := PropertyJSON.Marshal(indentCapacityDocument(count, float64(2)))
		if err != nil {
			b.Fatal(err)
		}
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(compact)), "compact-B")
			for iteration := 0; iteration < b.N; iteration++ {
				if _, ok := indentedJSONCapacity(compact, "", "  "); !ok {
					b.Fatal("capacity reported as unrepresentable")
				}
			}
		})
	}
}
