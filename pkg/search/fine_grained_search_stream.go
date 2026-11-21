package search

import (
	"context"
	"errors"
	"sync"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/resultstream"
)

type fineGrainedSearchStream struct {
	resultstream.Stream
	policy   string
	version  uint64
	versions graphMutationVersionProvider
	release  func() error
	close    sync.Once
	closeErr error
}

func newFineGrainedSearchStream(stream resultstream.Stream, view *Service, release func() error) *fineGrainedSearchStream {
	return &fineGrainedSearchStream{
		Stream: stream, policy: view.fineGrainedOwner.Fingerprint, release: release,
		version: view.fineGrainedVersion, versions: view.fineGrainedVersions,
	}
}

func (s *fineGrainedSearchStream) Pull(ctx context.Context, position uint64, n int) (*resultstream.Page, error) {
	principal := auth.FineGrainedRequestFromContext(ctx)
	if principal == nil || principal.Fingerprint == "" || principal.Fingerprint != s.policy {
		return nil, resultstream.ErrInvalidated
	}
	if err := s.CheckValid(); err != nil {
		return nil, err
	}
	return s.Stream.Pull(ctx, position, n)
}

func (s *fineGrainedSearchStream) CheckValid() error {
	if s.versions != nil {
		if err := checkContinuationGraphVersion(s.versions, s.version); err != nil {
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

func (s *fineGrainedSearchStream) Close() error {
	s.close.Do(func() {
		s.closeErr = errors.Join(s.Stream.Close(), s.release())
	})
	return s.closeErr
}
