package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/resultstream"
)

type fineGrainedSearchStream struct {
	resultstream.Stream
	scope *indexedSearchScope
}

func (s *fineGrainedSearchStream) Pull(ctx context.Context, position uint64, n int) (*resultstream.Page, error) {
	principal := auth.FineGrainedRequestFromContext(ctx)
	if principal == nil || principal.Fingerprint == "" || principal.Fingerprint != s.scope.principal.Fingerprint {
		return nil, resultstream.ErrInvalidated
	}
	if err := s.scope.check(ctx); err != nil {
		return nil, err
	}
	ctx = auth.WithFineGrainedRequest(ctx, s.scope.principal)
	ctx = context.WithValue(ctx, indexedSearchScopeKey{}, s.scope)
	page, err := s.Stream.Pull(ctx, position, n)
	if err != nil {
		return nil, err
	}
	if err := s.scope.check(ctx); err != nil {
		return nil, err
	}
	return page, nil
}

func (s *fineGrainedSearchStream) CheckValid() error {
	if s.scope.versions != nil {
		if err := checkContinuationGraphVersion(s.scope.versions, s.scope.version); err != nil {
			return err
		}
	}
	if checker, ok := s.Stream.(resultstream.ValidityChecker); ok {
		return checker.CheckValid()
	}
	return nil
}

func (s *fineGrainedSearchStream) RetainedBytes() int64 {
	if reporter, ok := s.Stream.(resultstream.RetainedBytesReporter); ok {
		return reporter.RetainedBytes()
	}
	return 0
}

func (s *fineGrainedSearchStream) SetRetainedBytesGrowthGuard(guard func() bool) {
	resultstream.BindRetainedBytesGrowthGuard(s.Stream, guard)
}
