package storage

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// engineWorker is one engine background loop. It is accepted work of the
// engine's one owner, so retirement never overtakes it. Its stop signal is its
// own context, a child of its parent and linked to the owner's, so stopping
// the engine also stops it, and its one completion is what a stop joins.
type engineWorker struct {
	stop context.CancelFunc
	done lifecycle.OnceCompletion
}

// startEngineWorker launches run as accepted engine work and returns its
// handle, or nil once the engine sealed admission. run returns when ctx ends:
// at its stop, when parent ends, or when the engine owner stops.
func (b *BadgerEngine) startEngineWorker(parent context.Context, run func(ctx context.Context)) *engineWorker {
	return startOwnedWorker(&b.work, parent, run)
}

// startOwnedWorker launches run as accepted work of owner: a storage
// component's background loop. It returns nil once owner sealed admission.
func startOwnedWorker(owner *lifecycle.WorkLifetime, parent context.Context, run func(ctx context.Context)) *engineWorker {
	if !owner.Enter() {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	unlink := context.AfterFunc(owner.Context(), cancel)
	w := &engineWorker{stop: cancel}
	go func() {
		defer owner.Leave()
		defer unlink()
		defer cancel()
		w.done.Run(func() { run(ctx) })
	}()
	return w
}

// stopAndJoin signals the worker and waits for its actual return. Repeated and
// concurrent calls join the same completion.
func (w *engineWorker) stopAndJoin() {
	if w == nil {
		return
	}
	w.stop()
	<-w.done.Done()
}
