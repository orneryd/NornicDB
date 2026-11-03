// Author: Codex, NOR-180 implementation lane, 2026-10-27.
package errors

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

type statusDiagnosticFixture struct {
	error
	message localization.Message
}

func (e *statusDiagnosticFixture) Unwrap() error                          { return e.error }
func (e *statusDiagnosticFixture) StatusDiagnostic() localization.Message { return e.message }

func TestStatusDiagnosticsPreservePrimary(t *testing.T) {
	cause := stderrors.New("terminal append unavailable")
	descriptor := localization.ExternalDiagnostic(cause)
	diagnostic := &statusDiagnosticFixture{cause, descriptor}
	require.True(t, HasStatusDiagnostic(fmt.Errorf("wrapped: %w", diagnostic)))
	require.False(t, HasStatusDiagnostic(cause))
	primary := MarkStatementTimeout(context.DeadlineExceeded, ErrDatabaseQueryTimeLimit)
	for _, err := range []error{stderrors.Join(primary, diagnostic), MarkStatementTimeout(stderrors.Join(context.DeadlineExceeded, fmt.Errorf("cleanup: %w", diagnostic)), ErrDatabaseQueryTimeLimit)} {
		code, message := Neo4jStatus(err)
		require.Equal(t, TransactionTimedOut, code)
		require.Equal(t, TransactionTimedOutMessage+"\n"+cause.Error(), message)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, err, cause)
	}
	require.Equal(t, "unchanged", AppendStatusDiagnostics(nil, "unchanged", nil))
	require.Equal(t, "unchanged", AppendStatusDiagnostics(stderrors.New("ordinary cause"), "unchanged", nil))
	// Both nested joined branches remain visible; the diagnostic's own cause is
	// not rendered for a second time.
	require.Equal(t, "primary\nterminal append unavailable\nsecond", AppendStatusDiagnostics(stderrors.Join(fmt.Errorf("wrap: %w", diagnostic), &statusDiagnosticFixture{stderrors.New("second"), localization.ExternalDiagnostic(stderrors.New("second"))}), "primary", nil))
}

func TestStatusDiagnosticsSurviveLocalization(t *testing.T) {
	primary := localization.NewLocalizedError("primary.code", localization.ApplicationMemoryPressure(), nil)
	diagnostic := &statusDiagnosticFixture{stderrors.New("wal: closed"), localization.ExternalDiagnostic(stderrors.New("wal: closed"))}
	err := stderrors.Join(primary, diagnostic)
	calls := 0
	render := func(message localization.Message) string { calls++; return "localized: " + message.Fallback }
	// A protocol's own status and message stay as they are unless the
	// localized primary has that status (Author: Claude, NOR-180, one render pass).
	classify := func(code string) func(error) (string, string, bool) {
		return func(error) (string, string, bool) { return code, "old message", true }
	}
	_, message := RenderStatusWithFallback(err, render, classify("primary.code"))
	require.Equal(t, "localized: "+primary.Error()+"\nlocalized: wal: closed", message)
	require.Equal(t, 2, calls)
	_, message = RenderStatusWithFallback(err, render, classify("other.code"))
	require.Equal(t, "old message", message)
	_, message = RenderStatusWithFallback(err, nil, classify("primary.code"))
	require.Equal(t, "old message", message)
}

func TestStatusDiagnosticsKeepAlreadyRenderedMessage(t *testing.T) {
	cause := stderrors.New("terminal append unavailable")
	diagnostic := &statusDiagnosticFixture{cause, localization.ExternalDiagnostic(cause)}
	err := stderrors.Join(fmt.Errorf("primary: %w", diagnostic), diagnostic)
	message := "primary\nterminal append unavailable"
	require.Equal(t, message, AppendStatusDiagnostics(err, message, nil))
	// A presenter which retained only the primary still gets the descriptor,
	// while revisiting the same outcome does not duplicate the visible text.
	require.Equal(t, message, AppendStatusDiagnostics(err, "primary", nil))
}

func TestStatusDiagnosticsKeepNestedTerminalOutcomes(t *testing.T) {
	firstCause := stderrors.New("first terminal append failed")
	secondCause := stderrors.New("second terminal append failed")
	first := &statusDiagnosticFixture{firstCause, localization.ExternalDiagnostic(firstCause)}
	second := &statusDiagnosticFixture{stderrors.Join(first, secondCause), localization.ExternalDiagnostic(secondCause)}
	err := stderrors.Join(first, second)
	require.ErrorIs(t, err, firstCause)
	require.ErrorIs(t, err, secondCause)
	require.Equal(t, "primary\nfirst terminal append failed\nsecond terminal append failed", AppendStatusDiagnostics(err, "primary", nil))
}
