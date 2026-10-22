package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestCompoundCASEKeepsTypedValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "casebinding"))
	point := CypherPoint{SRID: 7203, X: 1.25, Y: 2.5}
	params := map[string]any{"p": point, "b": []byte{0, 127, 128, 255}, "__nornic_expression_0": "client"}
	for _, query := range []string{
		"RETURN (CASE WHEN true THEN $p ELSE null END) = $p",
		"RETURN (CASE WHEN true THEN $b ELSE null END) = $b",
		"RETURN (CASE WHEN true THEN $p ELSE null END) = (CASE WHEN true THEN $p ELSE null END)",
		"RETURN (CASE WHEN true THEN $p ELSE null END) = $p AND $__nornic_expression_0 = 'client'",
	} {
		r, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err)
		require.Equal(t, [][]any{{true}}, r.Rows)
		// The scalar evaluator also serves expression contexts outside the row
		// pipeline and must preserve opaque CASE values there.
		fixtureAdmission912Context, fixtureAdmission912 := statementTestContext(withQueryParams(context.Background(), params), query)
		_ = fixtureAdmission912
		require.Equal(t, true, exec.evaluateExpressionWithContext(fixtureAdmission912, fixtureAdmission912Context, strings.TrimPrefix(query, "RETURN "), nil, nil))
	}
	require.Equal(t, "client", params["__nornic_expression_0"])
}

func TestGoTimePropertyZoneIdentity(t *testing.T) {
	for _, location := range []*time.Location{time.UTC, time.Local, time.FixedZone("Offset", 3600)} {
		value := time.Unix(1792888200, 0).In(location)
		require.Empty(t, normalizePropValue(value).(CypherDateTime).ZoneID)
	}
	loc, ok := loadPinnedTemporalLocation("America/New_York")
	require.True(t, ok)
	require.Equal(t, "America/New_York", normalizePropValue(time.Unix(1793511000, 0).In(loc)).(CypherDateTime).ZoneID)
}

func TestPrintfCollectionDisplay(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "display"))
	r, err := exec.Execute(context.Background(), "RETURN format('%s', {`a b`: [null, 'x', {k: false}]})", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"{'a b': [null, 'x', {k: false}]}"}}, r.Rows)
	for key, want := range map[string]string{"": "{'': null}", "1key": "{'1key': null}"} {
		r, err := exec.Execute(context.Background(), "RETURN format('%s', $m)", map[string]any{"m": map[string]any{key: nil}})
		require.NoError(t, err)
		require.Equal(t, [][]any{{want}}, r.Rows)
	}
}

func TestInvalidDateTimeTextShapes(t *testing.T) {
	for _, text := range []string{"2026-01-01", "invalidT00:00:00", "2026-01-01T00:00:00]", "2026-01-01Txx[Europe/London]", "2026-01-01T00:00:00[Not/AZone]"} {
		_, ok, err := parseTemporalTextWithError("datetime", text)
		require.False(t, ok)
		require.NoError(t, err)
	}
	_, ok, err := parseTemporalTextWithError("unsupported", "2026-01-01")
	require.False(t, ok)
	require.NoError(t, err)
}

func TestNormalizeNilPointerOfOtherTypes(t *testing.T) {
	value, changed := normalizeParameterValue((*int)(nil))
	require.True(t, changed)
	require.Nil(t, value)
	v := int64(1)
	value, changed = normalizeParameterValue(&v)
	require.False(t, changed)
	require.Same(t, &v, value)
}
