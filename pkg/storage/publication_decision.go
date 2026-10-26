package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// PublicationIdentity identifies one semantic publication attempt. It is not a
// physical timestamp, replay identity or persistent idempotency record.
type PublicationIdentity struct{ sequence uint64 }

// PublicationDecision is the semantic owner's last-abortable call seam. The
// callback invokes action synchronously once or rejects without invoking it.
// #1615 owns arbitration; storage owns and returns the actual action outcome.
type PublicationDecision func(context.Context, PublicationIdentity, func() error) error

type publicationDecisionKey struct{}

var publicationSequence atomic.Uint64

// WithPublicationDecision carries a semantic decision on the exact commit
// context. A nil decision clears an outer owner for preparation or replay.
func WithPublicationDecision(ctx context.Context, decision PublicationDecision) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, publicationDecisionKey{}, decision)
}

// DecidePublication encloses the authoritative storage action. A successful
// action cannot subsequently become a rollback because cancellation won later.
func DecidePublication(ctx context.Context, action func() error) error {
	var decision PublicationDecision
	if ctx != nil {
		decision, _ = ctx.Value(publicationDecisionKey{}).(PublicationDecision)
	}
	if decision == nil {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		return action()
	}
	var mu sync.Mutex
	called, sealed := false, false
	var outcome error
	guarded := func() error {
		mu.Lock()
		defer mu.Unlock()
		if sealed || called {
			return fmt.Errorf("%w: publication action is closed", ErrInvalidData)
		}
		called = true
		outcome = action()
		return outcome
	}
	defer func() {
		mu.Lock()
		sealed = true
		mu.Unlock()
	}()
	err := decision(ctx, PublicationIdentity{sequence: publicationSequence.Add(1)}, guarded)
	mu.Lock()
	defer mu.Unlock()
	sealed = true
	if called {
		return outcome
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: publication decision omitted its action", ErrInvalidData)
}
