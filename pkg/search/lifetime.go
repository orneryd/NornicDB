package search

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/util"
)

// BeginShutdown closes service admission and signals accepted work. Close owns
// the join and disposal. DB owners signal every service before joining workers
// that may themselves be waiting for a service's cancellation.
func (s *Service) BeginShutdown() {
	if s == nil {
		return
	}
	s.lifetimeWork.BeginStop(s.closeResources)
}

// LifetimeContext is immutable and is canceled by BeginShutdown/Close.
func (s *Service) LifetimeContext() context.Context { return s.lifecycleCtx }

// beginRead keeps a synchronous native/provider operation in the same service
// owner as its producers. State locks protect publication only; Close signals
// this context and joins the ticket before disposing native resources.
func (s *Service) beginRead(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if s == nil {
		return nil, nil, context.Canceled
	}
	ctx, release, accepted := s.lifetimeWork.Acquire(ctx)
	if !accepted {
		return nil, nil, context.Canceled
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifecycleCtx, cancel)
	return ctx, func() { stop(); cancel(); release() }, nil
}

func (s *Service) retainBackground() (func(), bool) {
	if s == nil || s.Closed() {
		return func() {}, false
	}
	_, release, accepted := s.lifetimeWork.Acquire(s.lifecycleCtx)
	return release, accepted
}

// RunBackgroundTask enrolls before dispatch. A DB build trampoline uses the
// same service owner even when its caller already holds the DB state lock.
func (s *Service) RunBackgroundTask(fn func()) bool {
	release, accepted := s.retainBackground()
	if !accepted {
		return false
	}
	go func() { defer release(); fn() }()
	return true
}

// AfterFunc retains the real callback, including a callback already running
// when Stop returns false. Origin/provenance decisions remain with its caller.
func (s *Service) AfterFunc(delay time.Duration, fn func()) *lifecycle.OwnedTimer {
	return lifecycle.AfterFuncOwned(s.lifecycleCtx, time.AfterFunc, delay, s.retainBackground, fn)
}

func (s *Service) recordLifetimeOutcome(operation string, err error) {
	// Cancellation caused by this owner's stop is an expected terminal status;
	// a joined real sibling must survive that disposition.
	if s.Closed() && util.ErrorIsOnly(err, context.Canceled) {
		return
	}
	s.lifetimeWork.RecordError(operation, err)
}
