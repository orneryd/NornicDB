package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Transaction counts at the transaction's own snapshot (B10).
//
// A count inside an explicit transaction counts the rows that transaction
// reads, and those rows are its begin boundary's. The committed counters hold
// every commit as of the moment they are read, so after a peer commits into the
// transaction's namespace they no longer describe those rows: the count was
// answered from the rows themselves instead, which materializes the counted
// population.
//
// The counters are persisted per snapshot, so the transaction reads its own:
// storage plans the read at its snapshot (storage.BadgerTransaction.
// ScopeCountStoreRead), the native counter kernel answers at that snapshot and
// adds the transaction's own staged contribution under the same lock, and the
// answer is both halves together. A peer commit no longer makes this count
// materialize the counted entity population.
//
// The answer is only the transaction's when the native kernel consumed the
// capability it carried. A reader or decorator that answers the request itself,
// refuses it, or rewrites it leaves the capability unconsumed, and its answer
// stays unproven: ReadCountStore then falls back to the committed counter's own
// certification, exactly as it did before this path existed.

// snapshotCountStoreRead asks the reader chain for one count-store read at the
// transaction's own snapshot and reports whether that read is answered.
//
// planned is false when the transaction cannot prove such a read
// (ScopeCountStoreRead's gates): the caller then keeps its committed-counter
// path. When planned, the returned answer is final - the transaction's own
// counter, which storage composed with the transaction's staged contribution
// under its lock - or, if the reader did not consume the capability, an
// unproven answer the caller counts from the rows instead.
//
// A reader's error is the caller's: a custom reader stays the authority for its
// own failure, exactly as the committed-counter path treats it. A reader that
// declines (DescribesRows false) is not asked again: its refusal is the answer,
// not a step to retry past.
func (w *transactionStorageWrapper) snapshotCountStoreRead(ctx context.Context, reader storage.CountStoreReadEngine, request storage.CountStoreReadRequest) (storage.CountStoreRead, bool, error) {
	scoped, planned, err := w.tx.ScopeCountStoreRead(ctx, request)
	if err != nil || !planned {
		return storage.CountStoreRead{}, false, err
	}
	answer, err := reader.ReadCountStore(ctx, scoped)
	if err != nil {
		return storage.CountStoreRead{}, true, err
	}
	if !answer.DescribesRows || !scoped.Scope.DescribesSnapshot() {
		// The reader answered without consuming the transaction's snapshot
		// capability, so its count is not proof of what this transaction
		// reads.
		return storage.CountStoreRead{}, true, nil
	}
	return answer, true, nil
}
