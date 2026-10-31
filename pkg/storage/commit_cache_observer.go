package storage

import "sync/atomic"

// This observer exposes deterministic operation boundaries to the public-path
// regression tests. It has no storage or publication effect.
var commitCacheObserver atomic.Pointer[func(Operation)]

func observeCommitCacheOperation(op Operation) {
	if observer := commitCacheObserver.Load(); observer != nil {
		(*observer)(op)
	}
}

func setCommitCacheObserver(observer func(Operation)) func() {
	previous := commitCacheObserver.Swap(&observer)
	return func() { commitCacheObserver.Store(previous) }
}
