package cypher

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTemporalConstructorsResolveNamedZoneTransitions(t *testing.T) {
	cases := []struct{ name, query, want string }{
		{"map spring gap", "RETURN toString(datetime({year:2026,month:3,day:8,hour:2,minute:30,timezone:'America/New_York'}))", "2026-03-08T03:30:00-04:00[America/New_York]"},
		{"string spring gap", "RETURN toString(datetime('2026-03-08T02:30:00[America/New_York]'))", "2026-03-08T03:30:00-04:00[America/New_York]"},
		{"map overlap later", "RETURN toString(datetime({year:2026,month:10,day:25,hour:1,minute:30,timezone:'Europe/London'}))", "2026-10-25T01:30:00Z[Europe/London]"},
		{"string overlap earlier", "RETURN toString(datetime('2026-10-25T01:30:00[Europe/London]'))", "2026-10-25T01:30:00+01:00[Europe/London]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(newTestMemoryEngine(t))
			r, err := e.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, r.Rows)
		})
	}
	t.Run("parameter map gap", func(t *testing.T) {
		e := NewStorageExecutor(newTestMemoryEngine(t))
		r, err := e.Execute(context.Background(), "RETURN toString(datetime($fields))", map[string]interface{}{"fields": map[string]interface{}{"year": int64(2026), "month": int64(3), "day": int64(8), "hour": int64(2), "minute": int64(30), "timezone": "America/New_York"}})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"2026-03-08T03:30:00-04:00[America/New_York]"}}, r.Rows)
	})
}
