package storage

import (
	"context"
	"sync/atomic"
)

// The observer records an actual blocked admission boundary in deterministic
// regression tests. Production has no observer and creates no extra worker.
var admissionWaitObserver atomic.Pointer[func(any, context.Context)]

func observeWriteAdmissionWait(owner any, ctx context.Context) {
	if observer := admissionWaitObserver.Load(); observer != nil {
		(*observer)(owner, ctx)
	}
}

func setAdmissionWaitObserver(observer func(any, context.Context)) func() {
	previous := admissionWaitObserver.Swap(&observer)
	return func() { admissionWaitObserver.Store(previous) }
}
