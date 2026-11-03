package convert_test

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/convert"
)

// indentDocument is a nested node document whose array holds the given values:
// the shape numeric wire preservation introduced.
func indentDocument(values []any) map[string]any {
	return map[string]any{
		"nodes": []any{map[string]any{
			"id":         int64(1),
			"labels":     []any{"Probe"},
			"properties": map[string]any{"value": values},
		}},
		"relationships": nil,
	}
}

func indentValues(count int, value any) []any {
	values := make([]any, count)
	for i := range values {
		values[i] = value
	}
	return values
}

var indentSink []byte

// indentTwoBufferControl is the compact-then-indent implementation the domain
// writer replaced: the compact document, then a second buffer json.Indent grows
// while it writes. It is the allocation and byte control for the current
// caller-owned implementation.
func indentTwoBufferControl(document map[string]any) ([]byte, error) {
	compact, err := convert.PropertyJSON.Marshal(document)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := json.Indent(&out, compact, "", "  "); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// TestPropertyJSONIndentMatchesStandardEncoder keeps the indented domain writer
// byte-identical to encoding/json for values both encode the same way.
func TestPropertyJSONIndentMatchesStandardEncoder(t *testing.T) {
	document := indentDocument(indentValues(8, 1.25))
	control, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := convert.PropertyJSON.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, control) {
		t.Fatalf("indented document differs from encoding/json\n got: %s\nwant: %s", actual, control)
	}
}

// TestPropertyJSONIndentAllocationContract keeps the indented domain writer on
// one traversal of the borrowed value: it may not copy the compact document
// into a second buffer before indenting it. The bound is per document, not per
// element, and covers the returned document plus json.Indent's own pre-growth
// and overflow growth, exactly as encoding/json's MarshalIndent pays them.
func TestPropertyJSONIndentAllocationContract(t *testing.T) {
	for _, count := range []int{1, 64, 4096} {
		document := indentDocument(indentValues(count, float64(2)))
		control := testing.AllocsPerRun(100, func() {
			data, err := indentTwoBufferControl(document)
			if err != nil {
				t.Fatal(err)
			}
			indentSink = data
		})
		allocations := testing.AllocsPerRun(100, func() {
			data, err := convert.PropertyJSON.MarshalIndent(document, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			indentSink = data
		})
		if allocations > control {
			t.Fatalf("%d elements: %.0f allocations for one indented document, two-buffer control %.0f", count, allocations, control)
		}
	}
}

// BenchmarkPropertyJSONIndent reports the indented writer against the
// compact-then-indent control and encoding/json at the widths an export uses.
func BenchmarkPropertyJSONIndent(b *testing.B) {
	for _, count := range []int{1, 8, 32, 64, 128} {
		document := indentDocument(indentValues(count, float64(2)))
		b.Run("control/"+strconv.Itoa(count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, err := indentTwoBufferControl(document)
				if err != nil {
					b.Fatal(err)
				}
				indentSink = data
			}
		})
		b.Run("current/"+strconv.Itoa(count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, err := convert.PropertyJSON.MarshalIndent(document, "", "  ")
				if err != nil {
					b.Fatal(err)
				}
				indentSink = data
			}
		})
		b.Run("standard/"+strconv.Itoa(count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, err := json.MarshalIndent(document, "", "  ")
				if err != nil {
					b.Fatal(err)
				}
				indentSink = data
			}
		})
	}
}
