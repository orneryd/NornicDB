package bolt

import (
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodePackStreamValue_LocalDateTimeStructure(t *testing.T) {
	want := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	data := encodePackStreamLocalDateTimeInto(nil, want.Unix(), int64(want.Nanosecond()))

	got, _, err := decodePackStreamValue(data, 0)
	if err != nil {
		t.Fatalf("decode localdatetime failed: %v", err)
	}

	local, ok := got.(cypher.CypherLocalDateTime)
	require.True(t, ok, "decoded type = %T", got)
	require.Equal(t, want, local.Time)
}

func TestDecodePackStreamValueHydratesTemporalStructures(t *testing.T) {
	date := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	clock := time.Date(1970, 1, 1, 12, 31, 14, 645876123, time.UTC)
	zonedClock := time.Date(1970, 1, 1, 12, 31, 14, 645876123, time.FixedZone("", 3600))
	localDateTime := time.Date(2026, 9, 14, 12, 31, 14, 645876123, time.UTC)

	tests := []struct {
		name   string
		value  interface{}
		assert func(*testing.T, interface{})
	}{
		{name: "date", value: cypher.CypherDate{Time: date}, assert: func(t *testing.T, got interface{}) {
			require.Equal(t, cypher.CypherDate{Time: date}, got)
		}},
		{name: "local time", value: cypher.CypherLocalTime{Time: clock}, assert: func(t *testing.T, got interface{}) {
			require.Equal(t, cypher.CypherLocalTime{Time: clock}, got)
		}},
		{name: "time", value: cypher.CypherTime{Time: zonedClock}, assert: func(t *testing.T, got interface{}) {
			value, ok := got.(cypher.CypherTime)
			require.True(t, ok, "decoded type = %T", got)
			require.Equal(t, zonedClock.Hour(), value.Time.Hour())
			_, offset := value.Time.Zone()
			require.Equal(t, 3600, offset)
		}},
		{name: "local datetime", value: cypher.CypherLocalDateTime{Time: localDateTime}, assert: func(t *testing.T, got interface{}) {
			require.Equal(t, cypher.CypherLocalDateTime{Time: localDateTime}, got)
		}},
		{name: "duration", value: &cypher.CypherDuration{Months: 2, Days: 3, Seconds: 4, Nanos: 5}, assert: func(t *testing.T, got interface{}) {
			require.Equal(t, &cypher.CypherDuration{Months: 2, Days: 3, Seconds: 4, Nanos: 5}, got)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := encodePackStreamValue(test.value)
			got, consumed, err := decodePackStreamValue(encoded, 0)
			require.NoError(t, err)
			require.Equal(t, len(encoded), consumed)
			test.assert(t, got)
		})
	}
}

func TestBoltIntegration_LocalDateTimeParamRoundTrip_ServerStack(t *testing.T) {
	want := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("single param create", func(t *testing.T) {
		roundTripLocalDateTimeScenario(
			t,
			buildRunMessageWithLocalDateTimeParam(
				"CREATE (:T {uuid:'single', created_at:$dt})",
				"dt",
				want.Unix(),
				int64(want.Nanosecond()),
			),
			"MATCH (n:T {uuid:'single'}) RETURN n.created_at AS ca",
			want,
		)
	})

	t.Run("unwind bulk row", func(t *testing.T) {
		roundTripLocalDateTimeScenario(
			t,
			buildRunMessageWithLocalDateTimeRows(
				"UNWIND $rows AS row MERGE (n:T {uuid:row.uuid}) SET n.created_at = row.created_at",
				"bulk",
				want.Unix(),
				int64(want.Nanosecond()),
			),
			"MATCH (n:T {uuid:'bulk'}) RETURN n.created_at AS ca",
			want,
		)
	})
}

func roundTripLocalDateTimeScenario(t *testing.T, writeMessage []byte, readQuery string, want time.Time) {
	t.Helper()

	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "bolt_localdatetime_roundtrip")
	_, port := startBoltIntegrationServer(t, store)
	conn := openBoltTestConn(t, port)

	runBoltStatementNoRecords(t, conn, BuildRunMessage("MATCH (n:T) DETACH DELETE n", nil, nil))
	runBoltStatementNoRecords(t, conn, writeMessage)

	rows := runBoltQueryAndCollectRecords(t, conn, readQuery)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("expected one row with one field, got %#v", rows)
	}

	local, ok := rows[0][0].(cypher.CypherLocalDateTime)
	require.True(t, ok, "round-trip type = %T", rows[0][0])
	require.Equal(t, want, local.Time)
}

func runBoltStatementNoRecords(t *testing.T, conn net.Conn, message []byte) {
	t.Helper()

	requireNoError(t, SendMessage(conn, message))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendPull(t, conn, nil))
	requireNoError(t, ReadSuccess(t, conn))
}

func encodePackStreamLocalDateTimeInto(dst []byte, sec, nanos int64) []byte {
	dst = append(dst, 0xB2, 0x64) // struct(2), LocalDateTime
	dst = encodePackStreamIntInto(dst, sec)
	dst = encodePackStreamIntInto(dst, nanos)
	return dst
}

func buildRunMessageWithLocalDateTimeParam(query, paramName string, sec, nanos int64) []byte {
	buf := []byte{0xB1, MsgRun}
	buf = append(buf, encodePackStreamString(query)...)
	buf = append(buf, 0xA1)
	buf = append(buf, encodePackStreamString(paramName)...)
	buf = encodePackStreamLocalDateTimeInto(buf, sec, nanos)
	buf = append(buf, 0xA0)
	return buf
}

func buildRunMessageWithLocalDateTimeRows(query, uuid string, sec, nanos int64) []byte {
	buf := []byte{0xB1, MsgRun}
	buf = append(buf, encodePackStreamString(query)...)
	buf = append(buf, 0xA1)
	buf = append(buf, encodePackStreamString("rows")...)
	buf = append(buf, 0x91)
	buf = append(buf, 0xA2)
	buf = append(buf, encodePackStreamString("uuid")...)
	buf = append(buf, encodePackStreamString(uuid)...)
	buf = append(buf, encodePackStreamString("created_at")...)
	buf = encodePackStreamLocalDateTimeInto(buf, sec, nanos)
	buf = append(buf, 0xA0)
	return buf
}
