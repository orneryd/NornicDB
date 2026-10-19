package storage

import (
	"strings"
	"sync/atomic"
)

type overlayScopeEpoch struct {
	epoch       atomic.Uint64
	generations int // guarded by WriteBehindBuffer.mu
}

// idleReadEpoch returns a publication token, not merely a pending-op count:
// rotation and replay cannot make an old snapshot certify an empty later buffer.
func (w *WriteBehindBuffer) idleReadEpoch(scope string) (uint64, bool) {
	if w == nil {
		return 0, true
	}
	if !w.readTracking {
		return 0, false
	}
	global := w.readEpoch.Load()
	if global&1 == 0 {
		return global << 1, true
	}
	if scope == "" {
		return global, false
	}
	if wildcard, ok := w.readScopes.Load(""); ok && wildcard.(*overlayScopeEpoch).epoch.Load()&1 != 0 {
		return global, false
	}
	if entry, ok := w.readScopes.Load(scope); ok {
		epoch := entry.(*overlayScopeEpoch).epoch.Load()
		return epoch<<1 | 1, epoch&1 == 0
	}
	return global << 1, true
}

// Each generation owns one scope reference, independent of how many operations
// or updates to the same key it contains. Empty scope is the legacy wildcard.
func (w *WriteBehindBuffer) markReadScopeLocked(id string) {
	scope, _, ok := strings.Cut(id, ":")
	if !ok {
		scope = ""
	}
	if w.active.readScopes == nil {
		w.active.readScopes = make(map[string]struct{})
	}
	if _, seen := w.active.readScopes[scope]; seen {
		return
	}
	w.active.readScopes[scope] = struct{}{}
	value, _ := w.readScopes.LoadOrStore(scope, &overlayScopeEpoch{})
	entry := value.(*overlayScopeEpoch)
	entry.generations++
	entry.epoch.Store((entry.epoch.Load() + 2) | 1)
}

func (w *WriteBehindBuffer) markCommitReadScopesLocked(c bufferedCommit) {
	for _, op := range c.ops {
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node != nil {
				w.markReadScopeLocked(string(op.Node.ID))
			}
		case OpDeleteNode:
			w.markReadScopeLocked(string(op.NodeID))
			for _, id := range op.DeletedEdgeIDs {
				w.markReadScopeLocked(string(id))
			}
		case OpCreateEdge, OpUpdateEdge:
			if op.Edge != nil {
				w.markReadScopeLocked(string(op.Edge.ID))
			}
		case OpDeleteEdge:
			w.markReadScopeLocked(string(op.EdgeID))
		}
	}
}

func (w *WriteBehindBuffer) retireReadScopesLocked(buf *CommitBuffer) {
	for scope := range buf.readScopes {
		value, _ := w.readScopes.Load(scope)
		entry := value.(*overlayScopeEpoch)
		entry.generations--
		epoch := (entry.epoch.Load() + 2) | 1
		if entry.generations == 0 {
			epoch &^= 1
		}
		entry.epoch.Store(epoch)
	}
}
