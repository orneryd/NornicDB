package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNamedZoneDateTimeTextResolution(t *testing.T) {
	cases := map[string]string{
		"2026-10-25T01:30:00[Europe/London]":          "2026-10-25T01:30:00+01:00[Europe/London]",
		"2026-03-29T01:30:00[Europe/London]":          "2026-03-29T02:30:00+01:00[Europe/London]",
		"2026-11-01T01:30:00[America/New_York]":       "2026-11-01T01:30:00-04:00[America/New_York]",
		"2026-03-08T02:30:00[America/New_York]":       "2026-03-08T03:30:00-04:00[America/New_York]",
		"2026-10-25T01:30:00+02:00[Europe/London]":    "",
		"2026-03-29T01:30:00Z[Europe/London]":         "",
		"2026-03-29T01:30:00+01:00[Europe/London]":    "",
		"2026-03-08T02:30:00-05:00[America/New_York]": "",
		"2026-03-08T02:30:00-04:00[America/New_York]": "",
		"2026-11-01T01:30:00-06:00[America/New_York]": "",
		"2026-07-01T12:00:00Z[Europe/London]":         "",
		"2026-01-01T12:00:00+01:00[Europe/London]":    "",
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "textzone"))
	for text, want := range cases {
		t.Run(text, func(t *testing.T) {
			for _, q := range []string{"RETURN toString(datetime($s))", "RETURN toString(datetime('" + text + "'))"} {
				r, err := exec.Execute(context.Background(), q, map[string]any{"s": text})
				if want == "" {
					require.Error(t, err)
					require.Equal(t, "Timezone and offset do not match: "+text, err.(*SemanticError).Message)
					require.Equal(t, "Neo.ClientError.Statement.ArgumentError", err.(interface{ BoltErrorCode() string }).BoltErrorCode())
				} else {
					require.NoError(t, err)
					require.Equal(t, [][]any{{want}}, r.Rows)
				}
			}
		})
	}
}
