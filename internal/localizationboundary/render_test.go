package localizationboundary

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestRenderBoundaryPolicies(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	unknown := localization.Message{ID: "unknown.boundary.message", Fallback: "literal fallback"}
	for _, test := range []struct {
		name        string
		manager     *localization.Manager
		message     localization.Message
		absent      NilFallback
		failure     RenderFailure
		preferences language.Tag
		text        string
		tag         language.Tag
	}{
		{"nil literal", nil, unknown, FallbackOnly, SourceEnglish, language.EuropeanSpanish, "literal fallback", language.AmericanEnglish},
		{"nil ID policy literal", nil, unknown, FallbackOrID, SourceEnglish, language.EuropeanSpanish, "literal fallback", language.AmericanEnglish},
		{"nil empty literal", nil, localization.Message{ID: "empty.fallback"}, FallbackOnly, SourceEnglish, language.EuropeanSpanish, "", language.AmericanEnglish},
		{"nil empty ID", nil, localization.Message{ID: "empty.fallback"}, FallbackOrID, SourceEnglish, language.EuropeanSpanish, "empty.fallback", language.AmericanEnglish},
		{"catalog error source", manager, unknown, FallbackOnly, SourceEnglish, language.EuropeanSpanish, "unknown.boundary.message", language.AmericanEnglish},
		{"catalog error literal", manager, unknown, FallbackOnly, LiteralFallback, language.EuropeanSpanish, "literal fallback", language.AmericanEnglish},
		{"catalog English", manager, localization.InvalidRequestBody(), FallbackOrID, SourceEnglish, language.AmericanEnglish, "invalid request body", language.AmericanEnglish},
		{"catalog Spanish", manager, localization.InvalidRequestBody(), FallbackOnly, LiteralFallback, language.EuropeanSpanish, "cuerpo de solicitud no válido", language.EuropeanSpanish},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := localization.WithPreferences(context.Background(), test.preferences)
			text, tag := Render(ctx, test.manager, test.message, test.absent, test.failure)
			require.Equal(t, test.text, text)
			require.Equal(t, test.tag, tag)
		})
	}
}
