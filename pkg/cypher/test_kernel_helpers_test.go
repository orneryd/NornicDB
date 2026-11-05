package cypher

import (
	"context"
	"errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file owns the fixtures and kernels that more than one cypher test file
// shares. Each owner keeps its assertion contract and canonical underlying
// call; callers keep their own queries, namespaces, error codes and residual
// state checks.

// newCypherTestExecutor returns a fresh executor over an automatically closed
// in-memory store in the requested namespace. The CREATE, MERGE and mutation
// pipeline tests all build on exactly this setup.
func newCypherTestExecutor(t testing.TB, namespace string) (*StorageExecutor, *storage.NamespacedEngine) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace)
	return NewStorageExecutor(store), store
}

// guardedNodeScan is the forbidden-scan gate both plan guards use. When the
// guard is armed the scan fails with the guard's own message instead of
// touching storage; otherwise the canonical underlying read runs.
func guardedNodeScan(forbid bool, message string, read func() ([]*storage.Node, error)) ([]*storage.Node, error) {
	if forbid {
		return nil, errors.New(message)
	}
	return read()
}

// createDeleteStep is one CREATE ... WITH ... DELETE statement the shared
// kernel executes. wantErrCode is the Bolt status the statement must fail
// with; check asserts a successful statement's counters, projection or type.
type createDeleteStep struct {
	query       string
	wantErrCode string
	check       func(t *testing.T, result *ExecuteResult)
}

// runCreateDeleteSteps executes every step once, in order, against the one
// executor the caller built, so the caller's residual-state assertions still
// observe the same population the statement sequence produced.
func runCreateDeleteSteps(t *testing.T, exec *StorageExecutor, ctx context.Context, steps []createDeleteStep) {
	t.Helper()
	for _, step := range steps {
		result, err := exec.Execute(ctx, step.query, nil)
		if step.wantErrCode != "" {
			require.Error(t, err, step.query)
			assert.Contains(t, statusText(err), step.wantErrCode, step.query)
			continue
		}
		require.NoError(t, err, step.query)
		if step.check != nil {
			step.check(t, result)
		}
	}
}

// requireErrorStatus owns the status classification every client-facing
// failure assertion checks: err must be present and must report exactly
// wantCode to Bolt and HTTP clients, with the caller's statement or context
// plus the reported message and error in the diagnostic. Callers pass their
// own expected code, so a syntax failure and a constraint failure stay
// distinct data instead of one forwarded default.
func requireErrorStatus(t *testing.T, err error, wantCode, statement string) {
	t.Helper()
	require.Error(t, err, statement)
	code, message := nornicerrors.Neo4jStatus(err)
	require.Equal(t, wantCode, code, "%s: %s: %v", statement, message, err)
}

// readSchemaNames owns ordered schema-name projection; callers retain their
// original namespace, context, own ordered SHOW query and residual-state
// assertions.
func readSchemaNames(t *testing.T, ctx context.Context, exec *StorageExecutor, query string) []string {
	t.Helper()
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	names := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		names = append(names, row[0].(string))
	}
	return names
}
