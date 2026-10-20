// Package txphase owns HTTP transaction execution and COMMIT admission.
// Statements keep the request deadline; an admitted terminal commit owns its
// real outcome independently of that deadline or a subsequent disconnect.
package txphase

import (
	"context"
	"sync"
	"time"
)

type ownerKey struct{}
type owner struct {
	mu       sync.Mutex
	admitted bool
}

func WithExecutionDeadline(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	execution, cancel := context.WithTimeout(ctx, timeout)
	return context.WithValue(execution, ownerKey{}, &owner{}), cancel
}

// Statements keeps execution cancellation while preventing internal statements
// and independently committed batches from admitting their enclosing request.
func Statements(ctx context.Context) context.Context {
	return context.WithValue(ctx, ownerKey{}, (*owner)(nil))
}

// CommitContext is the only phase transition. Terminal owners call it after
// statement execution and arbitration, immediately before irreversible commit.
// Other transports retain their existing cancellation contract.
func CommitContext(ctx context.Context) (context.Context, error) {
	phase, _ := ctx.Value(ownerKey{}).(*owner)
	if phase == nil {
		return ctx, nil
	}
	phase.mu.Lock()
	defer phase.mu.Unlock()
	if phase.admitted {
		return context.WithoutCancel(ctx), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return nil, context.DeadlineExceeded
	}
	phase.admitted = true
	return context.WithoutCancel(ctx), nil
}
