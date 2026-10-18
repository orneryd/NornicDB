package util

import "context"

// LockContext retains the lock's normal admission/fairness policy while letting
// the caller cancel its wait. A cancelled handoff unlocks as soon as the queued
// lock acquisition completes. On success the caller must invoke the release.
func LockContext(ctx context.Context, lock, unlock func()) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctx.Done() == nil {
		lock()
		return unlock, nil
	}
	acquired := make(chan struct{})
	go func() {
		lock()
		select {
		case acquired <- struct{}{}:
		case <-ctx.Done():
			unlock()
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-acquired:
		if err := ctx.Err(); err != nil {
			unlock()
			return nil, err
		}
		return unlock, nil
	}
}
