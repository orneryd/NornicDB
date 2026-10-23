package cypher_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/stretchr/testify/require"
)

// Every registered function and procedure that takes a list, a map or any
// value ends promptly when its statement is canceled while it works on a
// large one (the generated bulk-input sweep, embedded).
func TestBulkInputCancellationSweep(t *testing.T) {
	executor := testutil.SetupTestExecutor(t)
	size := testutil.DefaultBulkInputSize
	rebuild := func(t *testing.T) {
		for _, statement := range testutil.BulkInputFixture(size) {
			_, err := executor.Execute(context.Background(), statement, nil)
			require.NoError(t, err)
		}
	}
	rebuild(t)
	calls := testutil.BulkInputCalls(t, executor, size)
	require.NotEmpty(t, calls)
	testutil.RunBulkInputSweep(t, calls, func(ctx context.Context, query string, params map[string]interface{}, started func()) error {
		started()
		_, err := executor.Execute(ctx, query, params)
		return err
	}, testutil.DefaultBulkInputBudget, rebuild)
}
