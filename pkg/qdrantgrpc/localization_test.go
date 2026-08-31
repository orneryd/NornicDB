package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
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

func TestRequiredFieldLocalizesAndPreservesIdentifier(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES"))

	err = localizedStatus(ctx, manager, codes.InvalidArgument, localization.QdrantFieldRequired("field_name"))

	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "se requiere field_name", status.Convert(err).Message())
	require.Equal(t, "field_name is required", localization.QdrantFieldRequired("field_name").Fallback)
}

func TestRequiredFieldsLocalizesAndPreservesIdentifier(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES"))

	err = localizedStatus(ctx, manager, codes.InvalidArgument, localization.QdrantFieldsRequired("search_points"))

	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "se requieren search_points", status.Convert(err).Message())
	require.Equal(t, "search_points are required", localization.QdrantFieldsRequired("search_points").Fallback)
}

func TestDatabaseAccessLocalizesDenialAndPreservesCode(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	config := DefaultConfig()
	config.Localizer = manager
	config.DatabaseAccessModeResolver = func([]string) auth.DatabaseAccessMode {
		return auth.DenyAllDatabaseAccessMode
	}
	store, _ := newTestCollectionStore(t)
	server, err := NewServer(config, store, nil, nil, nil)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES"))

	err = server.AllowDatabaseAccess(ctx, "tenant-a", false)

	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, `no se permite el acceso a la base de datos "tenant-a"`, status.Convert(err).Message())
	require.Equal(t, `access to database "tenant-a" is not allowed`, localization.QdrantDatabaseAccessDenied("tenant-a").Fallback)
}

func TestAuthenticationMessagesPreserveCodesAndDiagnosticData(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES"))

	authRequired := localizedStatus(ctx, manager, codes.Unauthenticated, localization.QdrantAuthenticationRequired("missing authorization metadata"))
	require.Equal(t, codes.Unauthenticated, status.Code(authRequired))
	require.Equal(t, "se requiere autenticación: missing authorization metadata", status.Convert(authRequired).Message())
	require.Equal(t, "authentication required: missing authorization metadata", localization.QdrantAuthenticationRequired("missing authorization metadata").Fallback)

	permissionDenied := localizedStatus(ctx, manager, codes.PermissionDenied, localization.QdrantPermissionDenied())
	require.Equal(t, codes.PermissionDenied, status.Code(permissionDenied))
	require.Equal(t, "permiso denegado", status.Convert(permissionDenied).Message())
}

func TestNotFoundMessagesPreserveCodesAndIdentifiers(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES"))

	collectionErr := localizedStatus(ctx, manager, codes.NotFound, localization.QdrantCollectionNotFound("tenant-a"))
	require.Equal(t, codes.NotFound, status.Code(collectionErr))
	require.Equal(t, `no se encontró la colección "tenant-a"`, status.Convert(collectionErr).Message())
	require.Equal(t, `collection "tenant-a" not found`, localization.QdrantCollectionNotFound("tenant-a").Fallback)

	snapshotErr := localizedStatus(ctx, manager, codes.NotFound, localization.QdrantSnapshotNotFound("daily"))
	require.Equal(t, codes.NotFound, status.Code(snapshotErr))
	require.Equal(t, `no se encontró la instantánea "daily"`, status.Convert(snapshotErr).Message())
}
