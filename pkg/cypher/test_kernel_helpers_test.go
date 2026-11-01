package cypher

import (
	"context"
	"errors"
	"testing"

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
