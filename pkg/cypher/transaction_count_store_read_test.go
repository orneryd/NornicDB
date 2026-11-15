package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// The transaction's count-store read through a caller's reader chain. These
// cases pin the two answers a custom reader owns for itself: its failure is the
// count's failure, and its explicit decline is the answer - neither is retried
// and neither is overridden by the committed-counter path the wrapper keeps for
// reads storage cannot plan at the transaction's snapshot.

// errTransactionScopedCountRead is the failure a custom reader reports.
var errTransactionScopedCountRead = errors.New("transaction count-store read failed")

// TestTransactionCountReaderErrorIsTheCallers keeps the error contract: a custom
// reader that fails a count-store read fails the count, once, with its own
// error.
func TestTransactionCountReaderErrorIsTheCallers(t *testing.T) {
	fixture := newTransactionScopedCountFixture(t)
	fixture.probe.countErr = errTransactionScopedCountRead
	ctx := context.Background()
	_ = fixture.beginInTransaction(t, ctx)

	fixture.probe.countReads = 0
	_, err := fixture.writer.Execute(ctx, `MATCH ()-[r:T]->() RETURN count(r)`, nil)
	require.ErrorIs(t, err, errTransactionScopedCountRead, "a custom reader's failure is the count's failure")
	require.Equal(t, 1, fixture.probe.countReads, "the failed read is not retried")
}

// TestTransactionCountReaderDeclineIsTheAnswer keeps the decline contract: a
// reader that answers a count-store read with no described answer has answered,
// so the wrapper neither asks it again nor certifies anything, and the count
// stays with the rows the transaction reads.
func TestTransactionCountReaderDeclineIsTheAnswer(t *testing.T) {
	fixture := newTransactionScopedCountFixture(t)
	fixture.probe.decline = true
	ctx := context.Background()
	commit := fixture.beginInTransaction(t, ctx)
	atBegin := transactionSnapshotCountRows(t, fixture.writer, ctx, `MATCH (n:A) RETURN n`)

	fixture.probe.countReads = 0
	count := transactionSnapshotCountScalar(t, fixture.writer, ctx, `MATCH (n:A) RETURN count(n)`)
	rows := transactionSnapshotCountRows(t, fixture.writer, ctx, `MATCH (n:A) RETURN n`)
	require.Equal(t, 1, fixture.probe.countReads, "an explicit decline is the answer, not a step to re-ask")
	require.Equal(t, atBegin, rows)
	require.Equal(t, rows, count, "the declined read is answered from the rows the transaction reads")
	commit()
}
