package storage

import "sync"

// Ordered post-commit publication (F-PUBLISH).
//
// After a commit, the structures derived from committed entities are updated
// in memory: the node, query-node and edge caches, the label, edge-type and
// adjacency caches, the property and composite indexes, the unique-value
// registries, the relationship-uniqueness index, the cached counts and the
// pending-embedding candidates. The contract: at every moment that state is
// the result of the committed operations applied in commit-timestamp order up
// to some timestamp, and a write returns only once that timestamp covers its
// own commit.
//
// Each commit's update is its own delta (old to new, from the commit's own
// operations), which is correct only when it runs right after the update of
// the commit before it. So every commit hands its delta to the commit writer
// (commitWriter.publish) instead of applying it, and the commit oracle runs
// them in one ordered stage (commitOracle.publishAt, applyThrough): each
// once, in timestamp order, after Badger has published it, and the
// committer waits for the stage to pass its own timestamp before it returns.
// Engine writes, transaction commits, write-behind replay, the decay
// suppression rewrites and the embedding sidecar all commit through the
// oracle, so they all publish in the one order.
//
// A publication only updates in-memory structures: it takes their own locks
// and never commits, so the stage never waits on itself. Listeners (commit
// notifications) and Badger-side follow-ups (positional counts, ID counters,
// MVCC sequence) stay outside it.

var (
	publicationStageHookMu sync.RWMutex
	publicationStageHook   func(ts uint64)
)

// setPublicationStageHook installs a test seam the stage calls before each
// publication it runs, with the commit's timestamp, and returns the restore.
func setPublicationStageHook(hook func(ts uint64)) func() {
	publicationStageHookMu.Lock()
	previous := publicationStageHook
	publicationStageHook = hook
	publicationStageHookMu.Unlock()
	return func() {
		publicationStageHookMu.Lock()
		publicationStageHook = previous
		publicationStageHookMu.Unlock()
	}
}

func runPublicationStageHook(ts uint64) {
	publicationStageHookMu.RLock()
	hook := publicationStageHook
	publicationStageHookMu.RUnlock()
	if hook != nil {
		hook(ts)
	}
}
