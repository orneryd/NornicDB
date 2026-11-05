package qdrantgrpc

import (
	"context"
	"errors"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

type collectionAdmissionProbe struct {
	calls int
	name  string
	write bool
	err   error
}

func (p *collectionAdmissionProbe) AllowDatabaseAccess(_ context.Context, name string, write bool) error {
	p.calls++
	p.name = name
	p.write = write
	return p.err
}
func (*collectionAdmissionProbe) VisibleDatabases(_ context.Context, names []string) ([]string, error) {
	return names, nil
}

// Exercise every public Points admission boundary, including the intentionally
// different empty QueryBatch contract. Nil collection storage proves denial
// occurs before opening storage or validating operation-specific payloads.
func TestPR1655PointCollectionAdmissionPreservesPublicOrder(t *testing.T) {
	ctx := context.Background()
	denied := errors.New("admission denied")
	cases := []struct {
		name  string
		write bool
		batch bool
		call  func(*PointsService, string) error
	}{
		{"Upsert", true, false, func(s *PointsService, name string) error {
			_, err := s.Upsert(ctx, &qpb.UpsertPoints{CollectionName: name})
			return err
		}},
		{"Get", false, false, func(s *PointsService, name string) error {
			_, err := s.Get(ctx, &qpb.GetPoints{CollectionName: name})
			return err
		}},
		{"Delete", true, false, func(s *PointsService, name string) error {
			_, err := s.Delete(ctx, &qpb.DeletePoints{CollectionName: name})
			return err
		}},
		{"Count", false, false, func(s *PointsService, name string) error {
			_, err := s.Count(ctx, &qpb.CountPoints{CollectionName: name})
			return err
		}},
		{"Search", false, false, func(s *PointsService, name string) error {
			_, err := s.Search(ctx, &qpb.SearchPoints{CollectionName: name})
			return err
		}},
		{"Scroll", false, false, func(s *PointsService, name string) error {
			_, err := s.Scroll(ctx, &qpb.ScrollPoints{CollectionName: name})
			return err
		}},
		{"SetPayload", true, false, func(s *PointsService, name string) error {
			_, err := s.SetPayload(ctx, &qpb.SetPayloadPoints{CollectionName: name})
			return err
		}},
		{"OverwritePayload", true, false, func(s *PointsService, name string) error {
			_, err := s.OverwritePayload(ctx, &qpb.SetPayloadPoints{CollectionName: name})
			return err
		}},
		{"DeletePayload", true, false, func(s *PointsService, name string) error {
			_, err := s.DeletePayload(ctx, &qpb.DeletePayloadPoints{CollectionName: name})
			return err
		}},
		{"ClearPayload", true, false, func(s *PointsService, name string) error {
			_, err := s.ClearPayload(ctx, &qpb.ClearPayloadPoints{CollectionName: name})
			return err
		}},
		{"UpdateVectors", true, false, func(s *PointsService, name string) error {
			_, err := s.UpdateVectors(ctx, &qpb.UpdatePointVectors{CollectionName: name})
			return err
		}},
		{"DeleteVectors", true, false, func(s *PointsService, name string) error {
			_, err := s.DeleteVectors(ctx, &qpb.DeletePointVectors{CollectionName: name})
			return err
		}},
		{"SearchBatch", false, false, func(s *PointsService, name string) error {
			_, err := s.SearchBatch(ctx, &qpb.SearchBatchPoints{CollectionName: name})
			return err
		}},
		{"Recommend", false, false, func(s *PointsService, name string) error {
			_, err := s.Recommend(ctx, &qpb.RecommendPoints{CollectionName: name})
			return err
		}},
		{"RecommendBatch", false, false, func(s *PointsService, name string) error {
			_, err := s.RecommendBatch(ctx, &qpb.RecommendBatchPoints{CollectionName: name})
			return err
		}},
		{"SearchGroups", false, false, func(s *PointsService, name string) error {
			_, err := s.SearchGroups(ctx, &qpb.SearchPointGroups{CollectionName: name})
			return err
		}},
		{"CreateFieldIndex", true, false, func(s *PointsService, name string) error {
			_, err := s.CreateFieldIndex(ctx, &qpb.CreateFieldIndexCollection{CollectionName: name})
			return err
		}},
		{"DeleteFieldIndex", true, false, func(s *PointsService, name string) error {
			_, err := s.DeleteFieldIndex(ctx, &qpb.DeleteFieldIndexCollection{CollectionName: name})
			return err
		}},
		{"Query", false, false, func(s *PointsService, name string) error {
			_, err := s.Query(ctx, &qpb.QueryPoints{CollectionName: name})
			return err
		}},
		{"QueryBatch", false, true, func(s *PointsService, name string) error {
			_, err := s.QueryBatch(ctx, &qpb.QueryBatchPoints{CollectionName: name})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range []string{"", "tenant"} {
				probe := &collectionAdmissionProbe{err: denied}
				service := NewPointsService(DefaultConfig(), nil, nil, probe)
				err := tc.call(service, name)
				if name == "" && !tc.batch {
					require.Equal(t, codes.InvalidArgument, status.Code(err))
					require.Zero(t, probe.calls)
				} else {
					require.ErrorIs(t, err, denied)
					require.Equal(t, 1, probe.calls)
					require.Equal(t, name, probe.name)
					require.Equal(t, tc.write, probe.write)
				}
			}
		})
	}
	service := NewPointsService(DefaultConfig(), nil, nil, nil)
	response, err := service.QueryBatch(ctx, &qpb.QueryBatchPoints{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Empty(t, response.Result)
}
