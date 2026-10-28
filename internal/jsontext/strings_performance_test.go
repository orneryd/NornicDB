package jsontext

import (
	"bytes"
	std "encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// projectedUnicodeAllocationBudget bounds the allocations retained by the
// no-edit projection path for non-ASCII input. The path may hold its result
// bytes once; it must not materialize a batch copy per non-ASCII codepoint.
const projectedUnicodeAllocationBudget = 2.0

// repeatedUnicodePayload builds one JSON string owning many ordinary non-ASCII
// codepoints, repeated escaped quotes/backslashes and repeated canonical WTF8
// surrogate units produced by cyphertext.AppendCodepoint. No lowercase-u
// escape of a non-surrogate unit occurs, so the payload is scanned end to end
// without any source edit.
func repeatedUnicodePayload(repeats int) []byte {
	high := cyphertext.AppendCodepoint(nil, 0xd800)
	low := cyphertext.AppendCodepoint(nil, 0xdc00)
	var raw strings.Builder
	raw.WriteString(`{"text":"`)
	for i := 0; i < repeats; i++ {
		raw.WriteString(`café \\ \" `)
		raw.Write(high)
		raw.WriteString(` `)
		raw.Write(low)
		raw.WriteString(` 😀 naïve `)
	}
	raw.WriteString(`"}`)
	return []byte(raw.String())
}

// wtf8JSONPayload is the same shape without ordinary non-ASCII codepoints; the
// canonical surrogate units alone force the lossless projection path.
func wtf8JSONPayload(repeats int) []byte {
	high := cyphertext.AppendCodepoint(nil, 0xd800)
	low := cyphertext.AppendCodepoint(nil, 0xdc00)
	var raw strings.Builder
	raw.WriteString(`{"text":"`)
	for i := 0; i < repeats; i++ {
		raw.WriteString(`plain \\ \" `)
		raw.Write(high)
		raw.WriteString(` `)
		raw.Write(low)
		raw.WriteString(` `)
	}
	raw.WriteString(`"}`)
	return []byte(raw.String())
}

// TestJSONProjectedNoEditAllocationBudget fixes the no-edit projection cost for
// non-ASCII input. Canonical WTF8 units keep the lossless path active while the
// escaped ASCII pairs must be copied, so the whole payload is scanned without
// producing any source edit.
func TestJSONProjectedNoEditAllocationBudget(t *testing.T) {
	high := string(cyphertext.AppendCodepoint(nil, 0xd800))
	low := string(cyphertext.AppendCodepoint(nil, 0xdc00))
	for _, tc := range []struct {
		name    string
		raw     []byte
		trailer string
	}{
		{"normalunicode", repeatedUnicodePayload(64), " 😀 naïve "},
		{"wtf8", wtf8JSONPayload(64), " "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.raw
			require.False(t, utf8.Valid(raw), "payload must reach the lossless projection path")
			projection := projectJSONStrings(raw, projectedJSON)
			require.Empty(t, projection.edits, "payload must not produce source edits")
			require.Equal(t, raw, projection.bytes, "no-edit projection returns the exact input bytes")
			allocs := testing.AllocsPerRun(20, func() {
				_ = projectJSONStrings(raw, projectedJSON)
			})
			require.LessOrEqual(t, allocs, projectedUnicodeAllocationBudget, "no-edit projection allocates per non-ASCII codepoint")

			text := projectedJSONText(t, projection.bytes)
			require.True(t, strings.HasSuffix(text, high+" "+low+tc.trailer), "canonical bytes survive projection without edits")
		})
	}
}

// projectedJSONText binds the projected payload through the codec without
// changing any projection semantics.
func projectedJSONText(t *testing.T, projected []byte) string {
	t.Helper()
	var bound map[string]string
	require.NoError(t, Unmarshal(projected, &bound, ProjectedInput))
	return bound["text"]
}

var projectionBenchmarkSink stringProjection

func benchmarkProjectionPolicies(b *testing.B, normal, wtf8Payload []byte) {
	var sink stringProjection
	for _, tc := range []struct {
		name   string
		policy stringPolicy
		raw    []byte
	}{
		{"wire", wireJSON, normal},
		{"raw", rawJSON, normal},
		{"projected", projectedJSON, normal},
		{"opaque", opaqueJSON, normal},
		{"wtf8-wire", wireJSON, wtf8Payload},
		{"wtf8-raw", rawJSON, wtf8Payload},
		{"wtf8-projected", projectedJSON, wtf8Payload},
		{"wtf8-opaque", opaqueJSON, wtf8Payload},
	} {
		b.Run(tc.name, func(b *testing.B) {
			raw := tc.raw
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = projectJSONStrings(raw, tc.policy)
			}
			b.StopTimer()
			requireProjectionShape(b, raw, tc.policy)
			projectionBenchmarkSink = sink
		})
	}
}

// requireProjectionShape keeps the benchmark sink observable after timing.
func requireProjectionShape(tb testing.TB, raw []byte, policy stringPolicy) {
	tb.Helper()
	projection := projectJSONStrings(raw, policy)
	if len(projection.bytes) == 0 {
		tb.Fatal("projection produced no bytes")
	}
	if policy == projectedJSON && !bytes.Equal(projection.bytes, raw) {
		tb.Fatal("no-edit projected payload changed")
	}
}

func BenchmarkJSONProjectionSmallUnicode(b *testing.B) {
	benchmarkProjectionPolicies(b, repeatedUnicodePayload(4), wtf8JSONPayload(4))
}

func BenchmarkJSONProjectionLargeUnicode(b *testing.B) {
	benchmarkProjectionPolicies(b, repeatedUnicodePayload(2048), wtf8JSONPayload(2048))
}

// joinPayload concatenates byte fragments without embedding raw WTF8 in source.
func joinPayload(parts ...any) []byte {
	var out []byte
	for _, part := range parts {
		switch value := part.(type) {
		case string:
			out = append(out, value...)
		case []byte:
			out = append(out, value...)
		}
	}
	return out
}

type projectionVector struct {
	name     string
	payload  []byte
	policy   stringPolicy
	expected []byte
	edits    int
}

// TestJSONProjectionExactOutputAndEditMapping fixes the projection contract for
// every policy across normal Unicode, escaped UTF16 pairs, canonical WTF8 units
// and malformed UTF8. Lowercase-u escapes stay parser-owned; wireJSON rewrites
// only lone units, and opaque re-emits accepted escapes instead of internal
// WTF8.
func TestJSONProjectionExactOutputAndEditMapping(t *testing.T) {
	high := cyphertext.AppendCodepoint(nil, 0xd800)
	low := cyphertext.AppendCodepoint(nil, 0xdc00)
	units := joinPayload(`{"s":"`, high, low, `"}`)
	unitsRaw := joinPayload(`{"s":"`, strings.Repeat(`\ufffd`, 6), `"}`)
	unitsWire := joinPayload(`{"s":"\ud800\udc00"}`)
	broken := joinPayload(`{"s":"ok`, []byte{0xed, 0xa0}, `"}`)
	brokenWant := joinPayload(`{"s":"ok\ufffd\ufffd"}`)
	plain := joinPayload(`{"s":"café naïve 😀","e":"a\"b\\c"}`)
	escaped := joinPayload(`{"s":"A\ud83d\ude00B"}`)
	vectors := []projectionVector{
		{"normalunicode/raw", plain, rawJSON, plain, 0},
		{"normalunicode/projected", plain, projectedJSON, plain, 0},
		{"normalunicode/wire", plain, wireJSON, plain, 0},
		{"normalunicode/opaque", plain, opaqueJSON, plain, 0},
		{"escapedpair/raw", escaped, rawJSON, escaped, 0},
		{"escapedpair/projected", escaped, projectedJSON, escaped, 0},
		// wireJSON operates on raw WTF8 units; a lowercase-u escape already
		// spells the pair and stays parser-owned.
		{"escapedpair/wire", escaped, wireJSON, escaped, 0},
		{"escapedpair/opaque", escaped, opaqueJSON, escaped, 0},
		{"surrogateunits/raw", units, rawJSON, unitsRaw, 6},
		{"surrogateunits/projected", units, projectedJSON, units, 0},
		{"surrogateunits/wire", units, wireJSON, unitsWire, 2},
		{"surrogateunits/opaque", units, opaqueJSON, unitsWire, 2},
		{"malformedutf8/raw", broken, rawJSON, brokenWant, 2},
		{"malformedutf8/projected", broken, projectedJSON, brokenWant, 2},
		{"malformedutf8/wire", broken, wireJSON, brokenWant, 2},
		{"malformedutf8/opaque", broken, opaqueJSON, brokenWant, 2},
	}
	for _, vector := range vectors {
		t.Run(vector.name, func(t *testing.T) {
			got := projectJSONStrings(vector.payload, vector.policy)
			require.Equal(t, vector.expected, got.bytes, "%s bytes", vector.name)
			require.Len(t, got.edits, vector.edits, "%s edit count", vector.name)
			requireEditMapping(t, vector.payload, got)
		})
	}
}

// requireEditMapping re-derives accepted spans from the edits and asserts every
// unchanged byte still equals its original, so indices remain exact.
func requireEditMapping(t *testing.T, raw []byte, projection stringProjection) {
	t.Helper()
	if len(projection.edits) == 0 {
		require.Equal(t, raw, projection.bytes)
		return
	}
	last := 0
	accepted := 0
	for _, edit := range projection.edits {
		require.LessOrEqual(t, last, edit.Original.Start, "original spans are ordered")
		require.LessOrEqual(t, edit.Original.Start, edit.Original.End)
		gap := edit.Original.Start - last
		require.Equal(t, accepted+gap, edit.Accepted.Start, "unchanged bytes retain their extent")
		require.Equal(t, raw[last:edit.Original.Start], projection.bytes[accepted:edit.Accepted.Start], "unchanged bytes retain their original offsets")
		last = edit.Original.End
		accepted = edit.Accepted.End
	}
	require.Equal(t, raw[last:], projection.bytes[accepted:], "trailing unchanged bytes retain their original offsets")
}

// TestJSONProjectionCodecContract checks the public codec still renders and
// rebinds each payload identically for standard JSON consumers.
func TestJSONProjectionCodecContract(t *testing.T) {
	units := joinPayload(`{"s":"`, cyphertext.AppendCodepoint(nil, 0xd800), cyphertext.AppendCodepoint(nil, 0xdc00), `"}`)
	for _, payload := range [][]byte{
		joinPayload(`{"s":"café naïve 😀","e":"a\"b\\c"}`),
		joinPayload(`{"s":"A\ud83d\ude00B"}`),
		units,
		joinPayload(`{"s":"ok`, []byte{0xed, 0xa0}, `"}`),
	} {
		value, err := UnmarshalValue(payload)
		require.NoError(t, err)
		wire, err := Marshal(value)
		require.NoError(t, err)
		require.True(t, std.Valid(wire))
		require.True(t, utf8.Valid(wire))
		var stdValue any
		require.NoError(t, std.Unmarshal(payload, &stdValue))
		var roundTrip any
		require.NoError(t, std.Unmarshal(wire, &roundTrip))
		require.Equal(t, stdValue, roundTrip)
	}
}
