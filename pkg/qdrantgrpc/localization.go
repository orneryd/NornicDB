package qdrantgrpc

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func localizedStatus(ctx context.Context, manager *localization.Manager, code codes.Code, message localization.Message) error {
	if manager == nil {
		return status.Error(code, message.Fallback)
	}
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		preferences, _, err := language.ParseAcceptLanguage(strings.Join(incoming.Get("accept-language"), ","))
		if err == nil && len(preferences) > 0 {
			match := manager.Resolve("grpc", preferences...)
			ctx = localization.WithPreferences(ctx, match.Tag)
		}
	}
	text, _, err := manager.Render(ctx, message)
	if err != nil {
		text = manager.MustRenderEnglish(message)
	}
	return status.Error(code, text)
}
