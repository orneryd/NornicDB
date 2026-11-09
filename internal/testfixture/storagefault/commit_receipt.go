package storagefault

import "github.com/orneryd/nornicdb/pkg/storage"

// FinishPreparedReceipt completes the physical decision before publishing its
// caller-supplied outcome. A failed physical commit leaves the receipt untouched.
// The outcome, including an applied acknowledgement error, is preserved exactly.
func FinishPreparedReceipt(prepared *storage.PreparedEntityCommit, owner storage.CommitReceiptOwner, outcome storage.CommitOutcome) error {
	if outcome.State == storage.CommitApplied {
		if err := prepared.Commit(nil); err != nil {
			return err
		}
	} else {
		prepared.Abort()
	}
	owner.PublishPhysical(outcome)
	return owner.Resolve(outcome)
}
