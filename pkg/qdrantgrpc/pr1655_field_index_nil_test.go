package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPR1655FieldIndexNilRequestRetainsAdmissionOrder(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func(*PointsService) error
	}{
		{"create", func(s *PointsService) error { _, err := s.CreateFieldIndex(ctx, nil); return err }},
		{"delete", func(s *PointsService) error { _, err := s.DeleteFieldIndex(ctx, nil); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &collectionAdmissionProbe{}
			service := NewPointsService(DefaultConfig(), nil, nil, probe)
			var err error
			require.NotPanics(t, func() { err = tc.call(service) }, "collection admission precedes request-specific fields")
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Zero(t, probe.calls, "the empty collection fails before authorization or storage")
		})
	}
}
