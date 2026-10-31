package search

import "context"

// HoldWarmDispatchForTest intercepts the first dispatched callback without
// replacing the production DB callback or adding a product timing hook.
func HoldWarmDispatchForTest(s *Service, entered chan<- struct{}, release <-chan struct{}) {
	original := s.warmFunc.Load().(WarmFunc)
	s.warmFunc.Store(WarmFunc(func(ctx context.Context) { close(entered); <-release; original(ctx) }))
}
