package storage

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

// vectorWorkFallbacks counts, per owner site, writes whose storage owner had to
// admit vector work itself because the caller carried no admission. Request
// paths must never reach these sites: the request's own admission is bound to
// its transaction (BeginAdmittedTransaction) or its write handle
// (EngineWithVectorWorkOrigin). Process-internal writes (startup, recovery,
// background jobs, offline tools) legitimately admit here.
var vectorWorkFallbacks sync.Map // site -> *atomic.Int64

// fallbackVectorWorkOrigin admits work at an owner site that received none.
func fallbackVectorWorkOrigin(site string) uint64 {
	counter, ok := vectorWorkFallbacks.Load(site)
	if !ok {
		counter, _ = vectorWorkFallbacks.LoadOrStore(site, new(atomic.Int64))
	}
	counter.(*atomic.Int64).Add(1)
	return CaptureVectorWorkOrigin()
}

// AdmitUnrequestedVectorWork is fallbackVectorWorkOrigin for an owner site
// outside this package (a write entry that received no admission).
func AdmitUnrequestedVectorWork(site string) uint64 { return fallbackVectorWorkOrigin(site) }

// VectorWorkFallback is one owner site's fallback admission count.
type VectorWorkFallback struct {
	Site  string
	Count int64
}

// VectorWorkFallbacks reports how often each owner site admitted vector work
// that arrived without an admission, sorted by site. Request-path tests assert
// that driving a request adds nothing here.
func VectorWorkFallbacks() []VectorWorkFallback {
	var out []VectorWorkFallback
	vectorWorkFallbacks.Range(func(site, counter any) bool {
		out = append(out, VectorWorkFallback{Site: site.(string), Count: counter.(*atomic.Int64).Load()})
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Site < out[j].Site })
	return out
}

// TransactionBeginner opens a native transaction.
type TransactionBeginner interface {
	BeginTransaction() (*BadgerTransaction, error)
}

// BeginAdmittedTransaction is the one owner that opens a transaction for a
// request: the transaction carries the request's admitted origin (read from
// ctx, never captured here), so every write staged in it enqueues vector work
// under that admission.
func BeginAdmittedTransaction(ctx context.Context, engine TransactionBeginner) (*BadgerTransaction, error) {
	origin, admitted := VectorWorkOrigin(ctx)
	if !admitted {
		return nil, ErrVectorWorkNotAdmitted
	}
	if engine == nil {
		return nil, errors.New("admitted transaction needs a transactional engine")
	}
	tx, err := engine.BeginTransaction()
	if err != nil {
		return nil, err
	}
	tx.mu.Lock()
	tx.bindVectorWorkOriginLocked(origin)
	tx.mu.Unlock()
	return tx, nil
}

// VectorWorkOriginDeleter deletes under an admitted vector-work origin. A
// deletion has no entity argument to carry the admission, so every engine
// layer on a request path forwards it explicitly.
type VectorWorkOriginDeleter interface {
	DeleteNodeWithVectorWorkOrigin(id NodeID, origin uint64) error
	DeleteEdgeWithVectorWorkOrigin(id EdgeID, origin uint64) error
}

// deleteNodeAdmitted forwards an admitted deletion to engine, or reports that
// engine cannot carry it (handled false).
func deleteNodeAdmitted(engine Engine, id NodeID, origin uint64) (bool, error) {
	if deleter, ok := engine.(VectorWorkOriginDeleter); ok {
		return true, deleter.DeleteNodeWithVectorWorkOrigin(id, origin)
	}
	return false, nil
}

func deleteEdgeAdmitted(engine Engine, id EdgeID, origin uint64) (bool, error) {
	if deleter, ok := engine.(VectorWorkOriginDeleter); ok {
		return true, deleter.DeleteEdgeWithVectorWorkOrigin(id, origin)
	}
	return false, nil
}
