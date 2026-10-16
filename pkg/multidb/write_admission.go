package multidb

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
	"golang.org/x/sync/semaphore"
)

// A shared lease lets unlimited transactions publish independently while an
// exclusive lease orders quota checks and lazy byte-cache initialization.
const writeAdmissionCapacity int64 = 1 << 60

type writeAdmission struct {
	once sync.Once
	gate *semaphore.Weighted
}

func (a *writeAdmission) init() {
	a.once.Do(func() { a.gate = semaphore.NewWeighted(writeAdmissionCapacity) })
}
func (a *writeAdmission) Lock() {
	a.init()
	_ = a.gate.Acquire(context.Background(), writeAdmissionCapacity)
}
func (a *writeAdmission) Unlock() { a.gate.Release(writeAdmissionCapacity) }

func (a *writeAdmission) acquire(ctx context.Context, exclusive, holdsMergeKey bool) (func(), error) {
	a.init()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	weight := int64(1)
	if exclusive {
		weight = writeAdmissionCapacity
	}
	// A writer may hold admission while waiting for this transaction's MERGE
	// key. Never wait in the opposite order, including a pending exclusive lease.
	if holdsMergeKey {
		if !a.gate.TryAcquire(weight) {
			return nil, storage.ErrConflict
		}
	} else if err := a.gate.Acquire(ctx, weight); err != nil {
		return nil, err
	}
	return func() { a.gate.Release(weight) }, nil
}
