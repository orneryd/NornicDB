package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A comprehension's CASE/map body must retain its outer UNWIND record.
// Both records are present: this is independent of empty-input execution.
func TestOuterRowBindingInComprehensionProjection(t *testing.T) {
	query, err := os.ReadFile("testdata/outer_row_binding.cypher")
	require.NoError(t, err)
	parameters, err := os.ReadFile("testdata/outer_row_binding.params.json")
	require.NoError(t, err)
	for _, explicit := range []bool{false, true} {
		mode := "auto"
		if explicit {
			mode = "explicit"
		}
		t.Run(mode, func(t *testing.T) {
			base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "outer_row_binding"))
			ctx := context.Background()
			var params map[string]interface{}
			require.NoError(t, json.Unmarshal(parameters, &params))
			if explicit {
				_, err := executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
			}
			result, err := executor.Execute(ctx, string(query), params)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2)
			for index, category := range []string{"first", "second"} {
				require.Len(t, result.Rows[index], 2)
				require.Equal(t, []interface{}{map[string]interface{}{
					"scheme": "code0", "value": "value0", "issuer": nil,
					"jurisdiction": nil, "period": nil, "category": category,
				}}, result.Rows[index][0])
				require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, result.Rows[index][1])
			}
			require.NotEqual(t, result.Rows[0][1], result.Rows[1][1], "the second record must retain its own category")
			if explicit {
				_, err := executor.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
