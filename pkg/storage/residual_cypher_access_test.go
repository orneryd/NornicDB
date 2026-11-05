package storage

import (
	"context"
	"sync"
)

// Test-only adapters expose the existing deterministic publication boundaries
// to external-package tests that drive public Cypher parsing and execution.
func ResidualPublicationStageForTest(run func()) func() {
	return setPublicationStageHook(func(uint64) { run() })
}

func ResidualMiddleOperationForTest(run func()) func() {
	return setCommitCacheObserver(func(op Operation) {
		if op.Type == OpUpdateNode && op.Node != nil && op.Node.Properties["middle_marker"] == true {
			run()
		}
	})
}

func (b *BadgerEngine) ResidualHoldWriteBarrierForTest() (func(), <-chan struct{}) {
	b.writeBarrier.Lock()
	waiting := make(chan struct{}, 1)
	restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &b.writeBarrier {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	})
	var once sync.Once
	return func() { once.Do(func() { restore(); b.writeBarrier.Unlock() }) }, waiting
}

func (b *BadgerEngine) ResidualPrefixTailForTest(nodeID NodeID, edgeID EdgeID, run func(nodeCached, edgeCached bool)) func() {
	previous := b.dropPrefixTailHook
	b.dropPrefixTailHook = func(prefix string) error {
		b.nodeCacheMu.RLock()
		nodeCached := b.nodeCache[nodeID] != nil || b.queryNodeCache[nodeID] != nil
		b.nodeCacheMu.RUnlock()
		b.nodeBodyCacheMu.RLock()
		nodeCached = nodeCached || b.nodeBodyCache[nodeID] != nil
		b.nodeBodyCacheMu.RUnlock()
		b.edgeCacheMu.RLock()
		edgeCached := b.edgeCache[edgeID] != nil
		b.edgeCacheMu.RUnlock()
		run(nodeCached, edgeCached)
		if previous != nil {
			return previous(prefix)
		}
		return nil
	}
	return func() { b.dropPrefixTailHook = previous }
}
