package cypher

import (
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/require"
)

// Each typed value is sent as the driver value of its Cypher type.
func TestRemoteParameterValues(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	instant := time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC)
	// A zone id the time isn't in is loaded; an unknown one keeps the offset.
	zoned := CypherDateTime{Time: instant, ZoneID: "Europe/Berlin"}.RemoteParameterValue().(time.Time)
	require.Equal(t, berlin.String(), zoned.Location().String())
	require.True(t, zoned.Equal(instant))
	offset := CypherDateTime{Time: instant, ZoneID: "Nowhere/Unknown"}.RemoteParameterValue().(time.Time)
	require.Equal(t, "Offset", offset.Location().String())
	// Nanoseconds carry into seconds, a negative remainder borrowing one.
	require.Equal(t, dbtype.Duration{Months: 0, Days: 0, Seconds: 1, Nanos: 999999999}, CypherDuration{Seconds: 3, Nanos: -1000000001}.RemoteParameterValue())
	require.Equal(t, dbtype.Duration{Seconds: 2, Nanos: 5}, CypherDuration{Seconds: 1, Nanos: 1000000005}.RemoteParameterValue())
	require.Equal(t, []float64{1.5}, CypherVector{Type: VectorFloat64, Floats: []float64{1.5}}.RemoteParameterValue())
	require.Equal(t, []int64{2}, CypherVector{Type: VectorInteger64, Ints: []int64{2}}.RemoteParameterValue())
	uuid, ok := parseCypherUUID("0190a6b4-5c3d-7e8f-9a0b-1c2d3e4f5a6b")
	require.True(t, ok)
	require.Equal(t, "0190a6b4-5c3d-7e8f-9a0b-1c2d3e4f5a6b", uuid.RemoteParameterValue())
}

// Every item of a CALL scope clause is a variable; text that is not a scope
// clause (a call's arguments, a clause with quotes or brackets inside) is
// left to the other readers.
func TestScopeClauseQuotedNames(t *testing.T) {
	for _, tt := range []struct{ query, want string }{
		{"WITH 1 AS `null` CALL (null) { RETURN `null` AS v } RETURN v", "CALL (nornicq"},
		{"WITH 1 AS `null` CALL (/* c */ null, `null`) { RETURN `null` AS v } RETURN v", "CALL (/* c */ nornicq"},
		{"WITH 1 AS `null` CALL db.labels() YIELD label RETURN label, `null`", "CALL db.labels()"},
		{"WITH 1 AS `null` CALL (null) RETURN `null`", "CALL (null)"},
		{"WITH 1 AS `null` CALL ('x', null) { RETURN `null` AS v } RETURN v", "CALL ('x', null)"},
		{"WITH 1 AS `null` CALL ((null)) { RETURN `null` AS v } RETURN v", "CALL ((null))"},
		{"WITH 1 AS `null` CALL (null { RETURN `null` AS v } RETURN v", "CALL (null {"},
		{"WITH 1 AS `x y` CALL (`unclosed) { RETURN 1 AS v } RETURN v", "CALL (`unclosed)"},
	} {
		canonical, _ := canonicalizeQuotedVariables(tt.query)
		require.Contains(t, canonical, tt.want, tt.query)
	}
}
