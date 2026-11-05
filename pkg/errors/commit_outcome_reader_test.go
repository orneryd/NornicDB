package errors

import (
	stderrors "errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCommitRollbackReaderUsesAuthoritativeResponseTruth(t *testing.T) {
	cause := stderrors.New("earlier child kept nothing")
	child := MarkCommitRolledBack(NewTerminalFailure(TerminalCommit, false, cause, cause, localization.Message{}))
	require.True(t, IsCommitRolledBack(child))
	require.True(t, IsCommitRolledBack(MarkCommitRolledBack(cause)))
	committed := NewTerminalFailure(TerminalCommit, true, cause, cause, localization.Message{})
	for _, response := range []error{CombineTerminalOutcomes(child, committed), CombineTerminalOutcomes(committed, child)} {
		require.False(t, IsCommitRolledBack(response))
		require.ErrorIs(t, response, ErrCommitRolledBack, "historical child remains diagnostic evidence")
		require.ErrorIs(t, response, cause)
	}
	var retained TerminalOutcomes
	retained, _ = retained.Record(child)
	retained, _ = retained.RecordCommit()
	response := retained.Retain(nil)
	require.False(t, IsCommitRolledBack(response))
	require.ErrorIs(t, response, ErrCommitRolledBack)
	require.True(t, IsCommitRolledBack(child), "recording truth never mutates the retained child")
}
