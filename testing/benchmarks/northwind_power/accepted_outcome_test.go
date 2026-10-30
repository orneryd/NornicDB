// Author: NOR-288 lane (Codex), 2026-10-27.
package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAcceptedOutcomeDoesNotResubmitSeedCommand(t *testing.T) {
	for _, state := range []storage.CommitOutcomeState{storage.CommitRecoveryPending, storage.CommitApplied} {
		t.Run(fmt.Sprintf("state=%d", state), func(t *testing.T) {
			cause := fmt.Errorf("conflict detected: concurrent transaction modified data: %w", storage.ErrConflict)
			receipt, owner := storage.NewCommitReceipt(nil)
			require.NoError(t, owner.Resolve(storage.CommitOutcome{State: state, Err: cause}))
			outcome := receipt.WaitOutcome(t.Context())
			var accepted *storage.AcceptedCommitError
			require.ErrorAs(t, outcome.Err, &accepted)
			require.True(t, errors.Is(accepted.Cause(), storage.ErrConflict))
			code, message := nornicerrors.Neo4jStatus(outcome.Err)
			require.Equal(t, nornicerrors.TransactionOutcomeUnknown, code)
			wireError := &neo4j.Neo4jError{Code: code, Msg: message}
			calls := 0
			err := runWithTransientRetry(t.Context(), func() error { calls++; return wireError })
			require.Same(t, wireError, err)
			require.Equal(t, 1, calls, "the unchanged serialized-error consumer must not resubmit accepted work")
		})
	}
}
