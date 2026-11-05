package qdrantgrpc

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type pointDeleteBoundary struct {
	storage.Engine
	suffix   string
	failure  error
	attempts atomic.Int64
}

func (e *pointDeleteBoundary) GetInnerEngine() storage.Engine { return e.Engine }
func (e *pointDeleteBoundary) WithEntityChange(op storage.Operation, apply func() error) error {
	if op.Type == storage.OpDeleteNode && strings.HasSuffix(string(op.NodeID), e.suffix) {
		e.attempts.Add(1)
		if e.failure != nil {
			return e.failure
		}
	}
	return apply()
}

func TestPointsDeleteMutationOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		target     uint64
		rejected   bool
		wantPoints int
	}{
		{"rejected", 1, true, 1}, {"successful", 1, false, 0}, {"absent", 2, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
			t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			target := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: tc.target}}
			original := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 1}}
			memory := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { require.NoError(t, memory.Close()) })
			base := &pointDeleteBoundary{Engine: memory, suffix: string(pointIDToNodeID(target))}
			const privateCause = "synthetic-private-storage-credential-903718"
			if tc.rejected {
				base.failure = errors.New(privateCause)
			}
			manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			srv := startSharedIndexServer(t, manager, base, nil)
			createCosineCollection(t, ctx, srv.collections, "outcomes", 4)
			upsertPoint(t, ctx, srv.points, "outcomes", 1, []float32{1, 0, 0, 0}, nil)
			response, err := srv.points.Delete(ctx, &qpb.DeletePoints{CollectionName: "outcomes", Points: &qpb.PointsSelector{PointsSelectorOneOf: &qpb.PointsSelector_Points{Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{target}}}}})
			if tc.rejected {
				if assert.Error(t, err) {
					assert.Equal(t, codes.Internal, status.Code(err))
					assert.NotContains(t, err.Error(), privateCause)
				}
				assert.Nil(t, response)
				assert.Equal(t, int64(1), base.attempts.Load(), "rejection must reach the actual managed mutation boundary")
			} else {
				require.NoError(t, err)
				require.NotNil(t, response)
				require.NotNil(t, response.Result)
				assert.Equal(t, qpb.UpdateStatus_Completed, response.Result.Status)
				if tc.target == 1 {
					assert.Equal(t, int64(1), base.attempts.Load())
				}
			}
			got, getErr := srv.points.Get(ctx, &qpb.GetPoints{CollectionName: "outcomes", Ids: []*qpb.PointId{original}})
			require.NoError(t, getErr)
			require.Len(t, got.Result, tc.wantPoints)
			if tc.wantPoints == 1 {
				assert.Equal(t, uint64(1), got.Result[0].GetId().GetNum())
			}
		})
	}
}
