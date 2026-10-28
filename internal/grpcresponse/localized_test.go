package grpcresponse

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Independently derived expectations for rows 173/174. The wanted text for each
// row comes from reading the frozen original owners, not from this package:
//   - Qdrant (pkg/qdrantgrpc/localization.go) returned
//     status.Error(code, message.Fallback) before any metadata work when its
//     manager was nil, used preferences only after a clean accept-language parse,
//     and used manager.MustRenderEnglish(message) on a render error.
//   - Native search (pkg/nornicgrpc/search_service.go) rendered through the
//     nil-safe manager path, applied parsed preferences whenever they were
//     non-empty while ignoring the parse error, and used message.Fallback on a
//     render error.
//
// Both returned status.Error(code, text) with no details, headers or trailers.
func TestLocalizedStatusPurposePolicyMatrix(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	missing := localization.Message{ID: "grpc.owner.matrix.missing", Fallback: "literal fallback"}
	empty := localization.Message{ID: "grpc.owner.matrix.empty"}
	for _, test := range []struct {
		name    string
		purpose StatusLocalizationPurpose
		manager *localization.Manager
		header  []string
		context language.Tag
		code    codes.Code
		message localization.Message
		want    string
	}{
		{
			name: "qdrant nil manager returns fallback before metadata", purpose: QdrantSourceFallback, manager: nil,
			header: []string{"es-ES"}, code: codes.InvalidArgument, message: localization.QdrantFieldRequired("field_name"),
			want: "field_name is required",
		},
		{
			name: "qdrant nil manager keeps empty fallback", purpose: QdrantSourceFallback, manager: nil,
			code: codes.Internal, message: empty, want: "",
		},
		{
			name: "qdrant Spanish metadata renders catalog text", purpose: QdrantSourceFallback, manager: manager,
			header: []string{"es-ES"}, code: codes.InvalidArgument, message: localization.QdrantFieldRequired("field_name"),
			want: "se requiere field_name",
		},
		{
			name: "qdrant repeated metadata joined with comma keeps first preference", purpose: QdrantSourceFallback, manager: manager,
			header: []string{"en-US", "es-ES"}, code: codes.InvalidArgument, message: localization.QdrantFieldRequired("field_name"),
			want: "field_name is required",
		},
		{
			name: "qdrant invalid metadata retains context preference", purpose: QdrantSourceFallback, manager: manager,
			header: []string{"?"}, context: language.EuropeanSpanish,
			code: codes.InvalidArgument, message: localization.QdrantFieldRequired("field_name"), want: "se requiere field_name",
		},
		{
			name: "qdrant render error uses source English", purpose: QdrantSourceFallback, manager: manager,
			header: []string{"es-ES"}, code: codes.Internal, message: missing, want: "grpc.owner.matrix.missing",
		},
		{
			name: "native render error uses literal fallback", purpose: NativeLiteralFallback, manager: manager,
			header: []string{"es-ES"}, code: codes.Internal, message: missing, want: "literal fallback",
		},
		{
			name: "native nil manager literal fallback", purpose: NativeLiteralFallback, manager: nil,
			header: []string{"es-ES"}, code: codes.InvalidArgument, message: localization.QdrantFieldRequired("field_name"),
			want: "field_name is required",
		},
		{
			// The original native code passed a nil manager to
			// localizationboundary.Render, whose FallbackOnly policy returns the raw
			// (empty) fallback without substituting the message ID.
			name: "native nil manager empty fallback stays empty", purpose: NativeLiteralFallback, manager: nil,
			code: codes.InvalidArgument, message: empty, want: "",
		},
		{
			name: "native Spanish metadata renders catalog text", purpose: NativeLiteralFallback, manager: manager,
			header: []string{"es-ES", "en;q=0.5"}, code: codes.InvalidArgument, message: localization.QueryRequired(),
			want: "se requiere una consulta",
		},
		{
			name: "native absent metadata uses process default", purpose: NativeLiteralFallback, manager: manager,
			code: codes.InvalidArgument, message: localization.QueryRequired(), want: "query is required",
		},
		{
			name: "native pseudo locale keeps accent markers", purpose: NativeLiteralFallback, manager: manager,
			header: []string{"en-XA"}, code: codes.InvalidArgument, message: localization.QueryRequired(),
			want: "[!! query is required !!]",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.context != language.Und {
				ctx = localization.WithPreferences(ctx, test.context)
			}
			if len(test.header) > 0 {
				pairs := make([]string, 0, 2*len(test.header))
				for _, value := range test.header {
					pairs = append(pairs, "accept-language", value)
				}
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(pairs...))
			}
			err := LocalizedStatus(ctx, test.manager, test.code, test.message, test.purpose)
			require.Equal(t, test.code, status.Code(err))
			require.Equal(t, test.want, status.Convert(err).Message())
			require.Empty(t, status.Convert(err).Details())
		})
	}
}

// status.Error(codes.OK, text) goes through status.Err, which returns nil when
// the code is OK; both original owners inherited that identity. The shared owner
// must not invent an error for codes.OK, remap the code, or drop the text for any
// other code, so this pins the actual grpc-go contract the two owners relied on.
func TestCodesOKReturnsNilWithoutRemapping(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	err = LocalizedStatus(context.Background(), manager, codes.OK, localization.QdrantPermissionDenied(), QdrantSourceFallback)
	require.NoError(t, err)
	err = LocalizedStatus(context.Background(), nil, codes.OK, localization.QdrantPermissionDenied(), QdrantSourceFallback)
	require.NoError(t, err)

	notFound := LocalizedStatus(context.Background(), manager, codes.NotFound, localization.QdrantCollectionNotFound("tenant-a"), QdrantSourceFallback)
	require.Equal(t, codes.NotFound, status.Code(notFound))
	require.Equal(t, `collection "tenant-a" not found`, status.Convert(notFound).Message())
}

// The native branch applied whatever ParseAcceptLanguage returned whenever that
// slice was non-empty and ignored the error entirely. The x/text contract allows
// a non-nil partial slice alongside an error ("more permissive than RFC 2616 and
// may return non-nil slices even if the input is not valid"), so the owner must
// stay total on an erroring header; with German uninstalled in this matrix only
// the status contract is asserted, not a German rendering. The Qdrant branch
// rejects the whole header on error and keeps its context preference.
func TestNativePurposeAcceptsPartiallyParsedHeader(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	_, _, parseErr := language.ParseAcceptLanguage("de, german")
	require.Error(t, parseErr, "matrix precondition: header must error")
	for _, header := range []string{"de, german", "es-ES, ???", "?"} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", header))
		nativeErr := LocalizedStatus(ctx, manager, codes.InvalidArgument, localization.QueryRequired(), NativeLiteralFallback)
		require.Equal(t, codes.InvalidArgument, status.Code(nativeErr))
		require.Equal(t, "query is required", status.Convert(nativeErr).Message())

		qdrantCtx := localization.WithPreferences(
			metadata.NewIncomingContext(context.Background(), metadata.Pairs("accept-language", header)),
			language.EuropeanSpanish,
		)
		qdrantErr := LocalizedStatus(qdrantCtx, manager, codes.InvalidArgument, localization.QueryRequired(), QdrantSourceFallback)
		require.Equal(t, codes.InvalidArgument, status.Code(qdrantErr))
		require.Equal(t, "se requiere una consulta", status.Convert(qdrantErr).Message())
	}
}

func TestQdrantPurposeWithoutMetadataUsesProcessDefault(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	err = LocalizedStatus(context.Background(), manager, codes.PermissionDenied, localization.QdrantPermissionDenied(), QdrantSourceFallback)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, "permission denied", status.Convert(err).Message())
}
