package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitReceiptDoneClosesWhenSettlementPanics(t *testing.T) {
	const failure = "settlement callback panicked"
	receipt, owner := NewCommitReceipt(func(CommitOutcome) { panic(failure) })
	outcome := CommitOutcome{State: CommitApplied}
	require.PanicsWithValue(t, failure, func() { _ = owner.Resolve(outcome) })

	select {
	case <-receipt.Done():
	default:
		t.Fatal("terminal receipt left Done open after settlement panicked")
	}
	require.Equal(t, outcome, receipt.WaitOutcome(context.Background()))
}
