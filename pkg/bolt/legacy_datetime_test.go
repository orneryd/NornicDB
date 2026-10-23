package bolt

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// These local times are unambiguous. Legacy named-zone structures cannot
// distinguish the two occurrences of a clock time during a fall-back overlap.
func legacyDateTimeCases(t *testing.T) []cypher.CypherDateTime {
	t.Helper()
	var values []cypher.CypherDateTime
	for _, c := range []struct{ text, zone string }{
		{"1970-01-01T02:15:00.000000042+01:00", ""},
		{"1969-12-31T23:59:59.999999999+05:45", ""},
		{"1969-12-30T23:15:00.123456789-07:00", ""},
		{"1970-01-01T02:15:00.000000042+01:00", "Europe/Paris"},
		{"1969-12-31T23:59:59.999999999+01:00", "Europe/Paris"},
		{"2026-03-29T01:59:59.123456789+01:00", "Europe/Paris"},
		{"2026-03-29T03:00:00.000000042+02:00", "Europe/Paris"},
	} {
		instant, err := time.Parse(time.RFC3339Nano, c.text)
		require.NoError(t, err)
		if c.zone != "" {
			location, err := time.LoadLocation(c.zone)
			require.NoError(t, err)
			instant = instant.In(location)
		} else {
			_, offset := instant.Zone()
			instant = instant.In(time.FixedZone("Offset", offset))
		}
		values = append(values, cypher.CypherDateTime{Time: instant, ZoneID: c.zone})
	}
	return values
}

// Build the expected wire representation independently from the datetime codec.
func dateTimeWire(value cypher.CypherDateTime, utc bool) []byte {
	seconds := value.Time.Unix()
	signature := byte(0x49)
	if !utc {
		v := value.Time
		seconds = time.Date(v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), 0, time.UTC).Unix()
		signature = 0x46
	}
	if value.ZoneID != "" {
		signature += 0x20
	}
	wire := encodePackStreamIntInto([]byte{0xB3, signature}, seconds)
	wire = encodePackStreamIntInto(wire, int64(value.Time.Nanosecond()))
	if value.ZoneID != "" {
		return encodePackStreamStringInto(wire, value.ZoneID)
	}
	_, offset := value.Time.Zone()
	return encodePackStreamIntInto(wire, int64(offset))
}

func assertDateTimeInstant(t *testing.T, want cypher.CypherDateTime, got any) {
	t.Helper()
	actual, ok := got.(cypher.CypherDateTime)
	require.True(t, ok, "expected datetime, got %T", got)
	require.Equal(t, want.Time.Unix(), actual.Time.Unix())
	require.Equal(t, want.Time.Nanosecond(), actual.Time.Nanosecond())
	require.Equal(t, want.Time.Format(time.RFC3339Nano), actual.Time.Format(time.RFC3339Nano))
	require.Equal(t, want.ZoneID, actual.ZoneID)
}

func TestLegacyDateTimeCodec(t *testing.T) {
	for i, value := range legacyDateTimeCases(t) {
		for _, utc := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/utc=%t", i, utc), func(t *testing.T) {
				want := dateTimeWire(value, utc)
				t.Run("encode typed", func(t *testing.T) {
					require.Equal(t, want, encodePackStreamCypherDateTimeInto(nil, value, utc))
				})
				t.Run("encode time", func(t *testing.T) {
					require.Equal(t, want, encodePackStreamZonedDateTimeIntoWithUTC(nil, value.Time, utc))
				})
				t.Run("decode", func(t *testing.T) {
					got, consumed, err := decodePackStreamValue(want, 0)
					require.NoError(t, err)
					require.Equal(t, len(want), consumed)
					assertDateTimeInstant(t, value, got)
				})
				t.Run("nested encode", func(t *testing.T) {
					// A single map key keeps the byte comparison deterministic.
					input := []any{value, map[string]any{"v": value.Time}}
					expected := append([]byte{0x92}, want...)
					expected = append(expected, 0xA1, 0x81, 'v')
					expected = append(expected, want...)
					require.Equal(t, expected, encodePackStreamValueIntoWithUTC(nil, input, utc))
				})
				t.Run("nested decode", func(t *testing.T) {
					wire := append([]byte{0x91, 0xA1, 0x81, 'v'}, want...)
					got, _, err := decodePackStreamValue(wire, 0)
					require.NoError(t, err)
					assertDateTimeInstant(t, value, got.([]any)[0].(map[string]any)["v"])
				})
			})
		}
	}
}

// The benchmark owner runs this fixture on both base and head; it is kept out
// of correctness runs so timing work stays with that owner.
func BenchmarkEncodePackStreamDateTime(b *testing.B) {
	paris, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		b.Fatal(err)
	}
	offset := time.Date(1970, 1, 1, 2, 15, 0, 42, time.FixedZone("Offset", 3600))
	zoned := time.Date(2026, 3, 29, 3, 0, 0, 123456789, paris)
	for _, utc := range []bool{false, true} {
		for _, value := range []struct {
			name  string
			value any
		}{
			{"offset time", offset},
			{"offset typed", cypher.CypherDateTime{Time: offset}},
			{"zone time", zoned},
			{"zone typed", cypher.CypherDateTime{Time: zoned, ZoneID: "Europe/Paris"}},
		} {
			b.Run(fmt.Sprintf("utc=%t/%s", utc, value.name), func(b *testing.B) {
				dst := make([]byte, 0, 128)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					dst = encodePackStreamValueIntoWithUTC(dst[:0], value.value, utc)
				}
			})
		}
	}
}
