// Package grpcresponse owns localized status framing for gRPC boundaries.
package grpcresponse

import (
	"context"
	"github.com/orneryd/nornicdb/internal/localizationboundary"
	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"strings"
)

// StatusLocalizationPurpose preserves each boundary's metadata/error policy.
type StatusLocalizationPurpose uint8

const (
	QdrantSourceFallback StatusLocalizationPurpose = iota
	NativeLiteralFallback
)

// LocalizedStatus renders an already-decided status without changing its code,
// details or admission. Qdrant requires clean metadata and source-English error
// fallback; native search retains parsed preferences and literal error fallback.
func LocalizedStatus(ctx context.Context, manager *localization.Manager, code codes.Code, message localization.Message, purpose StatusLocalizationPurpose) error {
	failure := localizationboundary.LiteralFallback
	if purpose == QdrantSourceFallback {
		failure = localizationboundary.SourceEnglish
	}
	// Qdrant's nil manager bypasses metadata; native's nil manager is nil-safe.
	if manager != nil || purpose == NativeLiteralFallback {
		if incoming, ok := metadata.FromIncomingContext(ctx); ok {
			values := incoming.Get("accept-language")
			if purpose == QdrantSourceFallback || len(values) > 0 {
				preferences, _, err := language.ParseAcceptLanguage(strings.Join(values, ","))
				if len(preferences) > 0 && (purpose == NativeLiteralFallback || err == nil) {
					ctx = localization.WithPreferences(ctx, manager.Resolve("grpc", preferences...).Tag)
				}
			}
		}
	}
	text, _ := localizationboundary.Render(ctx, manager, message, localizationboundary.FallbackOnly, failure)
	return status.Error(code, text)
}
