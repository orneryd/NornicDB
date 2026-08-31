package nornicgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestSearchTextLocalizesValidationErrorFromMetadata(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	service, err := NewService(Config{Localizer: manager}, nil, nil, &stubSearcher{})
	require.NoError(t, err)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES, en;q=0.5"))
	_, err = service.SearchText(ctx, &gen.SearchTextRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "se requiere una consulta", status.Convert(err).Message())

	_, err = service.SearchText(context.Background(), &gen.SearchTextRequest{})
	require.Error(t, err)
	require.Equal(t, "query is required", status.Convert(err).Message())
}
