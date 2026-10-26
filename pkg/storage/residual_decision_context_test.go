package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). The seam transports cancellation
// to the installed semantic owner; it does not decide ahead of that owner.
func TestResidualDecisionReceivesCancelledContext(t *testing.T) {
	ownerOutcome := errors.New("semantic owner outcome")
	calls, actions := 0, 0
	base, cancel := context.WithCancel(context.Background())
	var ctx context.Context
	ctx = WithPublicationDecision(base, func(actual context.Context, _ PublicationIdentity, action func() error) error {
		calls++
		require.Same(t, ctx, actual)
		require.ErrorIs(t, actual.Err(), context.Canceled)
		return ownerOutcome
	})
	cancel()
	err := DecidePublication(ctx, func() error { actions++; return nil })
	require.ErrorIs(t, err, ownerOutcome)
	require.Equal(t, 1, calls)
	require.Zero(t, actions)
	// Without an installed semantic owner, the existing admission default
	// still rejects a cancelled caller before touching its action.
	err = DecidePublication(base, func() error { actions++; return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, actions)
}
