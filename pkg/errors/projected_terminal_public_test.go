// Author: Codex, NOR-180 public classification projection ownership, 2026-11-01.
package errors_test

import (
	stderrors "errors"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTerminalOutcomeSurvivesPublicClassificationProjection(t *testing.T) {
	primary := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "Projection", Properties: []string{"id"}, Message: "original unique failure"}
	wal := stderrors.New("owned commit record diagnostic")
	finished := nornicerrors.NewTerminalFailure(nornicerrors.TerminalCommit, true, primary, wal, localization.ExternalDiagnostic(wal))
	// The public projection keeps the original cause and supplies the owned
	// classification through its public errors.As contract.
	projected := localization.ProjectError(primary, finished.Error(), nil, finished)
	require.ErrorIs(t, projected, primary)
	var viaAs *nornicerrors.TerminalFailure
	require.ErrorAs(t, projected, &viaAs)
	require.Same(t, finished, viaAs)
	want, _ := nornicerrors.Neo4jStatus(finished)
	require.False(t, nornicerrors.IsRetriableStatus(want))
	forms := []struct {
		name string
		err  error
	}{
		{"direct", projected},
		{"single_wrapper", fmt.Errorf("outer: %w", projected)},
		{"joined", stderrors.Join(projected, stderrors.New("other primary"))},
		{"multi_wrapper", projectionForest{error: projected, children: []error{primary, stderrors.New("other primary")}}},
		{"empty_multi_wrapper", projectionForest{error: projected}},
	}
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			classified := nornicerrors.MarkMergeCommitTimeUniqueConflict(form.err)
			got, message := nornicerrors.Neo4jStatus(classified)
			assert.Equal(t, want, got, "a finished public classification cannot become a retryable MERGE race")
			assert.False(t, nornicerrors.IsMergeCommitTimeUniqueConflict(classified))
			assert.True(t, nornicerrors.HasTerminalOutcome(form.err))
			assert.Contains(t, message, wal.Error())
			combined := nornicerrors.CombineTerminalOutcomes(form.err, stderrors.New("cleanup primary"))
			combinedCode, combinedMessage := nornicerrors.Neo4jStatus(combined)
			assert.Equal(t, want, combinedCode)
			assert.Contains(t, combinedMessage, wal.Error())
		})
	}
}

// This public error contract delegates classification to errors.As and exposes
// its actual causes through the multi-error contract. It adds no status policy.
type projectionForest struct {
	error
	children []error
}

func (e projectionForest) As(target any) bool { return stderrors.As(e.error, target) }
func (e projectionForest) Unwrap() []error    { return e.children }

func TestTerminalOutcomeProjectionCannotReplaceOwnedChild(t *testing.T) {
	actualWAL := stderrors.New("actual child diagnostic")
	actual := nornicerrors.NewTerminalFailure(nornicerrors.TerminalAbort, false, nil, actualWAL, localization.ExternalDiagnostic(actualWAL))
	projectionWAL := stderrors.New("projection diagnostic")
	projection := nornicerrors.NewTerminalFailure(nornicerrors.TerminalCommit, true, nil, projectionWAL, localization.ExternalDiagnostic(projectionWAL))
	want, _ := nornicerrors.Neo4jStatus(actual)
	for _, wrapped := range []error{
		localization.ProjectError(actual, "projected text", nil, projection),
		projectionForest{error: projection, children: []error{actual}},
	} {
		code, message := nornicerrors.Neo4jStatus(wrapped)
		assert.Equal(t, want, code)
		assert.Contains(t, message, actualWAL.Error())
		assert.NotContains(t, message, projectionWAL.Error())
		combined := nornicerrors.CombineTerminalOutcomes(wrapped, stderrors.New("other primary"))
		code, message = nornicerrors.Neo4jStatus(combined)
		assert.Equal(t, want, code)
		assert.Contains(t, message, actualWAL.Error())
		assert.NotContains(t, message, projectionWAL.Error())
	}
}
