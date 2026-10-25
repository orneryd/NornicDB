package storage

import "sync/atomic"

// WaitForListeners consumes a finite event frontier on each distinct storage
// owner under engine's existing wrappers. It must be called outside locks a
// callback may need. It does not land buffered data; FlushWriteBehind does both.
func WaitForListeners(engine Engine) {
	_ = visitStorageOwners(engine, func(e Engine) error {
		if owner, ok := e.(interface{ WaitForListeners() }); ok {
			owner.WaitForListeners()
		}
		return nil
	})
}

// WaitForListenersThen runs after once its captured events are consumed. A
// dispatcher callback cannot wait for itself: its action stays on the real
// frontier for that dispatcher to finish after the callback returns. No extra
// goroutine or placeholder event is needed. Call outside callback-needed locks.
func WaitForListenersThen(engine Engine, after func()) {
	done := make(chan struct{})
	var pending atomic.Int32
	pending.Store(1) // registration cannot complete before all owners are captured
	complete := func() {
		if pending.Add(-1) == 0 {
			defer close(done)
			if after != nil {
				after()
			}
		}
	}
	reentrant := false
	_ = visitStorageOwners(engine, func(e Engine) error {
		if owner, ok := e.(interface {
			listenerFrontier() (*commitNotification, bool)
			afterCommitNotification(*commitNotification, func())
		}); ok {
			last, self := owner.listenerFrontier()
			reentrant = reentrant || self
			pending.Add(1)
			owner.afterCommitNotification(last, complete)
		} else if owner, ok := e.(interface{ WaitForListeners() }); ok {
			owner.WaitForListeners()
		}
		return nil
	})
	complete()
	if !reentrant {
		<-done
	}
}

// LandWriteBehind applies acknowledged data through wrappers without waiting
// for callbacks. Shared owners in a composite are visited once.
func LandWriteBehind(engine Engine) error {
	return visitStorageOwners(engine, func(e Engine) error {
		if owner, ok := e.(interface{ LandWriteBehind() error }); ok {
			return owner.LandWriteBehind()
		}
		return nil
	})
}

func visitStorageOwners(engine Engine, visit func(Engine) error) error {
	seen := make(map[Engine]bool)
	var walk func(Engine) error
	walk = func(e Engine) error {
		if e == nil || seen[e] {
			return nil
		}
		seen[e] = true
		if c, ok := e.(*CompositeEngine); ok {
			for _, target := range c.eventTargets() {
				if err := walk(target.engine); err != nil {
					return err
				}
			}
			return nil
		}
		if wrapper, ok := e.(EngineUnwrapper); ok {
			return walk(wrapper.GetInnerEngine())
		}
		return visit(e)
	}
	return walk(engine)
}

func (w *WriteBehindBuffer) pruneConsumedFrontier() {
	w.mu.Lock()
	kept := w.frontier[:0]
	for _, g := range w.frontier {
		if !g.consumed() {
			kept = append(kept, g)
		}
	}
	clear(w.frontier[len(kept):])
	w.frontier = kept
	w.mu.Unlock()
}
