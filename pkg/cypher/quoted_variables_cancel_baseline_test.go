package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The optional-method bridge lets this exact regression run against the
// original owner before its checked API exists. The fallback is the original
// restoration, not a substitute implementation of restoration.
func TestQuotedVariableCanceledRestoreOriginal(t *testing.T) {
	_, names := canonicalizeQuotedVariables("WITH 1 AS `a b` RETURN `a b`")
	require.NotNil(t, names)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &pipelineCancellation{ctx: ctx}
	result := &ExecuteResult{Columns: []string{names.internalOrder[0]}, Rows: [][]interface{}{{int64(1)}}}
	calls := 0
	parse := func(string) []returnItem { calls++; return nil }
	var restored *ExecuteResult
	var err error
	if checked, ok := interface{}(names).(interface {
		restoreWithCancellation(*ExecuteResult, error, func(string) []returnItem, *pipelineCancellation) (*ExecuteResult, error)
	}); ok {
		restored, err = checked.restoreWithCancellation(result, context.DeadlineExceeded, parse, p)
	} else {
		restored, err = names.restore(result, context.DeadlineExceeded, parse)
	}
	require.Zero(t, calls, "canceled deferred restoration must not reparse the statement")
	require.Same(t, result, restored)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the existing execution error keeps precedence")
	require.ErrorIs(t, p.err, context.Canceled)
}
