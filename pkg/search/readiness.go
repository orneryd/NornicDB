package search

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// prepareSearchRead is the common full-search read boundary. Live-only scalar
// vector reads retain their established contract and do not enter this boundary.
func (s *Service) prepareSearchRead(ctx context.Context) error {
	if err := s.EnsureWarm(ctx); err != nil {
		return err
	}
	if s.buildAttempted.Load() && !s.IsReady() {
		return localizedError(localization.SearchIndexBuilding(), ErrSearchIndexBuilding)
	}
	return nil
}
