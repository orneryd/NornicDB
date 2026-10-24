package storage

// Helpers in this file are exported for use by external test packages
// (pkg/cypher/..., integration tests). They are compiled into the
// production binary but intentionally limited to construction helpers
// that would otherwise be pasted across _test.go files, and hooks that let
// those tests order concurrent work deterministically.

import (
	"fmt"
	"sync/atomic"
)

// NewMemoryEngineWithMVCCHistory creates an in-memory engine that retains
// historical MVCC versions. Use this for tests and callers that exercise
// the multi-version surface (temporal.asOf, time-travel reads, prior-version
// snapshots). The default NewMemoryEngine runs head-only, matching the
// production default.
func NewMemoryEngineWithMVCCHistory() *MemoryEngine {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
		EngineOptions: EngineOptions{
			RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100},
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create in-memory BadgerEngine: %v", err))
	}
	return &MemoryEngine{BadgerEngine: engine}
}

// uniqueKeyLockWaitHook, when set, runs each time a transaction starts
// waiting for a UNIQUE / NODE KEY lock another transaction holds, after the
// wait is registered and before it blocks.
var uniqueKeyLockWaitHook atomic.Pointer[func()]

// SetUniqueKeyLockWaitHookForTesting makes hook run whenever a transaction
// starts waiting for a constraint key lock another transaction holds (a
// MERGE of a key a concurrent MERGE holds), so tests outside this package
// can order concurrent statements by the wait itself instead of by timing.
// The returned function restores the previous hook.
func SetUniqueKeyLockWaitHookForTesting(hook func()) (restore func()) {
	previous := uniqueKeyLockWaitHook.Swap(&hook)
	return func() { uniqueKeyLockWaitHook.Store(previous) }
}
