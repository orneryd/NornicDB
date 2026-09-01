package qdrantgrpc

import (
	"context"
	"errors"
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

func TestVectorValidationMessagesPreserveCodesAndArguments(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES"))

	dimensionErr := localizedStatus(ctx, manager, codes.InvalidArgument, localization.QdrantVectorDimensionMismatch(3, 4))
	require.Equal(t, codes.InvalidArgument, status.Code(dimensionErr))
	require.Equal(t, "la dimensión del vector no coincide: se obtuvo 3, se esperaba 4", status.Convert(dimensionErr).Message())
	require.Equal(t, "vector dimension mismatch: got 3, expected 4", localization.QdrantVectorDimensionMismatch(3, 4).Fallback)

	mutationErr := localizedStatus(ctx, manager, codes.FailedPrecondition, localization.QdrantVectorMutationsDisabled())
	require.Equal(t, codes.FailedPrecondition, status.Code(mutationErr))
	require.Contains(t, status.Convert(mutationErr).Message(), "NORNICDB_EMBEDDING_ENABLED=false")
	require.Equal(t, "vector mutations are disabled because NornicDB-managed embeddings are enabled; set NORNICDB_EMBEDDING_ENABLED=false to allow managing vectors via Qdrant gRPC", localization.QdrantVectorMutationsDisabled().Fallback)
}

func TestRepeatedQueryMessagesPreserveCodesAndDiagnostics(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES"))

	limitErr := localizedStatus(ctx, manager, codes.InvalidArgument, localization.QdrantLimitTooLarge(101, 100))
	require.Equal(t, codes.InvalidArgument, status.Code(limitErr))
	require.Equal(t, "el límite es demasiado grande: 101 > 100", status.Convert(limitErr).Message())
	require.Equal(t, "limit too large: 101 > 100", localization.QdrantLimitTooLarge(101, 100).Fallback)

	getErr := localizedStatus(ctx, manager, codes.Internal, localization.QdrantGetPointsFailed(errors.New("disk offline")))
	require.Equal(t, codes.Internal, status.Code(getErr))
	require.Equal(t, "no se pudieron obtener los puntos: disk offline", status.Convert(getErr).Message())
	require.Equal(t, "failed to get points: disk offline", localization.QdrantGetPointsFailed(errors.New("disk offline")).Fallback)

	embedErr := localizedStatus(ctx, manager, codes.Internal, localization.QdrantEmbedQueryFailed(errors.New("model unavailable")))
	require.Equal(t, "no se pudo generar el embedding de la consulta: model unavailable", status.Convert(embedErr).Message())
	require.Equal(t, "failed to embed query: model unavailable", localization.QdrantEmbedQueryFailed(errors.New("model unavailable")).Fallback)

	requiredErr := localizedStatus(ctx, manager, codes.FailedPrecondition, localization.QdrantTextQueryEmbeddingsRequired())
	require.Equal(t, codes.FailedPrecondition, status.Code(requiredErr))
	require.Contains(t, status.Convert(requiredErr).Message(), "EmbedQuery")
	require.Equal(t, "text query requires embeddings; enable NornicDB embeddings and configure EmbedQuery", localization.QdrantTextQueryEmbeddingsRequired().Fallback)
}

func TestRepeatedSnapshotMessagesPreserveCodesAndDiagnostics(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", "es-ES"))
	cause := errors.New("disk offline")
	tests := []struct {
		name     string
		code     codes.Code
		message  localization.Message
		want     string
		fallback string
	}{
		{name: "collection", code: codes.NotFound, message: localization.QdrantCollectionNotFoundWithCause(cause), want: "no se encontró la colección: disk offline", fallback: "collection not found: disk offline"},
		{name: "directory", code: codes.Internal, message: localization.QdrantSnapshotDirectoryCreateFailed(cause), want: "no se pudo crear el directorio de instantáneas: disk offline", fallback: "failed to create snapshot directory: disk offline"},
		{name: "save", code: codes.Internal, message: localization.QdrantSnapshotSaveFailed(cause), want: "no se pudo guardar la instantánea: disk offline", fallback: "failed to save snapshot: disk offline"},
		{name: "list", code: codes.Internal, message: localization.QdrantSnapshotListFailed(cause), want: "no se pudieron enumerar las instantáneas: disk offline", fallback: "failed to list snapshots: disk offline"},
		{name: "delete", code: codes.Internal, message: localization.QdrantSnapshotDeleteFailed(cause), want: "no se pudo eliminar la instantánea: disk offline", fallback: "failed to delete snapshot: disk offline"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := localizedStatus(ctx, manager, test.code, test.message)
			require.Equal(t, test.code, status.Code(err))
			require.Equal(t, test.want, status.Convert(err).Message())
			require.Equal(t, test.fallback, test.message.Fallback)
		})
	}
}
