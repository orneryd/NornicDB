package storage

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNor279FilteredProvenanceReadsDetachMetadata(t *testing.T) {
	defer config.WithEdgeProvenanceEnabled()()
	for _, subject := range []struct {
		name    string
		indexes []int
		read    func(context.Context, *EdgeMetaStore, int) ([]*EdgeMeta, error)
	}{
		{"signal", []int{2, 1, 0}, func(ctx context.Context, s *EdgeMetaStore, limit int) ([]*EdgeMeta, error) {
			return s.GetBySignalType(ctx, string(SignalSimilarity), limit)
		}},
		{"materialized", []int{2, 0}, func(ctx context.Context, s *EdgeMetaStore, limit int) ([]*EdgeMeta, error) {
			return s.GetMaterialized(ctx, limit)
		}},
		{"time", []int{2, 1, 0}, func(ctx context.Context, s *EdgeMetaStore, limit int) ([]*EdgeMeta, error) {
			return s.GetByTimeRange(ctx, nor279EdgeBaseTime, nor279EdgeBaseTime.Add(2*time.Minute), limit)
		}},
		{"origin", []int{1}, func(ctx context.Context, s *EdgeMetaStore, limit int) ([]*EdgeMeta, error) {
			return s.GetByOrigin(ctx, "origin-1", limit)
		}},
		{"session", []int{2, 1, 0}, func(ctx context.Context, s *EdgeMetaStore, limit int) ([]*EdgeMeta, error) {
			return s.GetBySession(ctx, "session-1", limit)
		}},
	} {
		for _, limit := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/limit=%d", subject.name, limit), func(t *testing.T) {
				store := nor279SeedEdgeMetaStore(t, t.Context())
				want := nor279EdgeMetaRecords(3)
				got, err := subject.read(t.Context(), store, limit)
				require.NoError(t, err)
				indexes := subject.indexes
				if limit > 0 {
					indexes = indexes[:limit]
				}
				require.Len(t, got, len(indexes))
				for i, index := range indexes {
					nor279RequireEdgeMeta(t, got[i], &want[index], subject.name)
				}
				nor279MutateNestedMetadata(t, got[0].Metadata)
				nor279RequireEdgeMetaStoreState(t, store, t.Context(), want, subject.name)
			})
		}
	}
}
