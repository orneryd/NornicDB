package storage

import "context"

// snapshotForNotification takes ownership of a bound embedding outcome. Its
// writer waits for delivery before returning, so the accepted private snapshot
// remains immutable for the queue, listeners and worker callback. Other public
// writes may return while delivery is queued and must detach their payloads.
func (op *Operation) snapshotForNotification() {
	_ = op.snapshotForNotificationWithContext(context.Background())
}

// snapshotForNotificationWithContext freezes the same owned images during
// preparation. Accepted delivery uses the context-free entrance above.
func (op *Operation) snapshotForNotificationWithContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if op.Node != nil && !(op.embeddingOutcome && op.Node.embeddingAttempt != nil) {
		image, err := copyNodeWithContext(ctx, op.Node)
		if err != nil {
			return err
		}
		op.Node = image
	}
	if op.Edge != nil {
		image, err := copyEdgeWithContext(ctx, op.Edge)
		if err != nil {
			return err
		}
		op.Edge = image
	}
	return ctx.Err()
}
