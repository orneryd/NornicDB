// Plan 04-04-07: cmd/nornicdb wiring for StorageMetrics + MVCCMetrics.
//
// unwrapBadgerEngine walks the storage engine wrapper chain
// (NamespacedEngine → AsyncEngine → WALEngine → BadgerEngine) until it
// hits the underlying *storage.BadgerEngine. The walk is type-assertion
// based and tolerates future wrapper additions: each layer exposes either
// GetEngine() or GetInnerEngine().
//
// badgerStorageProbe + badgerMVCCProbe are the thin adapters that satisfy
// observability.StorageProbe / observability.MVCCProbe by routing into
// the *BadgerEngine accessors. The error returns from the engine's
// NodeCount/EdgeCount are dropped — the GaugeFunc surface only cares
// about a numeric value, and the engine's defer-recover already protects
// against panic on closed-engine paths.
package main

import (
	"github.com/orneryd/nornicdb/pkg/storage"
)

// unwrapBadgerEngine walks the storage.Engine wrapper chain until it hits
// the underlying *storage.BadgerEngine. Returns nil when the chain does
// not terminate at a BadgerEngine (e.g. MemoryEngine in tests, or a
// future engine implementation).
func unwrapBadgerEngine(engine storage.Engine) *storage.BadgerEngine {
	for i := 0; i < 8; i++ { // safety bound; current depth is 3
		if be, ok := engine.(*storage.BadgerEngine); ok {
			return be
		}
		// NamespacedEngine
		if ns, ok := engine.(*storage.NamespacedEngine); ok {
			engine = ns.GetInnerEngine()
			continue
		}
		// AsyncEngine
		if ae, ok := engine.(*storage.AsyncEngine); ok {
			engine = ae.GetEngine()
			continue
		}
		// WALEngine
		if we, ok := engine.(*storage.WALEngine); ok {
			engine = we.GetEngine()
			continue
		}
		// Anything else: bail.
		return nil
	}
	return nil
}

// badgerStorageProbe satisfies observability.StorageProbe.
type badgerStorageProbe struct {
	be *storage.BadgerEngine
}

func (p badgerStorageProbe) NodeCount() int64 {
	n, err := p.be.NodeCount()
	if err != nil {
		return 0
	}
	return n
}

func (p badgerStorageProbe) EdgeCount() int64 {
	n, err := p.be.EdgeCount()
	if err != nil {
		return 0
	}
	return n
}

// badgerMVCCProbe satisfies observability.MVCCProbe by routing to the
// RISK-2 accessors shipped in Plan 04-04-01.
type badgerMVCCProbe struct {
	be *storage.BadgerEngine
}

func (p badgerMVCCProbe) PinnedBytes() int64              { return p.be.PinnedBytes() }
func (p badgerMVCCProbe) OldestReaderAgeSeconds() float64 { return p.be.OldestReaderAgeSeconds() }
func (p badgerMVCCProbe) ActiveReaders() int64            { return p.be.ActiveReaders() }
