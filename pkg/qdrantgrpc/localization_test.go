package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestCollectionsCreateLocalizesRequiredNameAndPreservesCode(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	service := NewCollectionsService(nil, nil, nil)
	service.localizer = manager
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES"))

	_, err = service.Create(ctx, &qpb.CreateCollection{})

	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "se requiere collection_name", status.Convert(err).Message())
}
