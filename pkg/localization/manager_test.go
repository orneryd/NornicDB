package localization

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestManagerRendersCatalogAndPluralForms(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	require.Equal(t, language.AmericanEnglish, manager.DefaultTag())
	require.Equal(t, []language.Tag{
		language.AmericanEnglish,
		language.MustParse("en-XA"),
		language.EuropeanSpanish,
	}, manager.SupportedTags())

	english, tag, err := manager.Render(context.Background(), InvalidRequestBody())
	require.NoError(t, err)
	require.Equal(t, language.AmericanEnglish, tag)
	require.Equal(t, "invalid request body", english)

	spanishContext := WithPreferences(context.Background(), language.EuropeanSpanish)
	spanish, tag, err := manager.Render(spanishContext, InvalidRequestBody())
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "cuerpo de solicitud no válido", spanish)
	pseudo, tag, err := manager.Render(WithPreferences(context.Background(), language.MustParse("en-XA")), InvalidRequestBody())
	require.NoError(t, err)
	require.Equal(t, language.MustParse("en-XA"), tag)
	require.Equal(t, "[!! invalid request body !!]", pseudo)

	one, _, err := manager.Render(spanishContext, ItemsProcessed(1))
	require.NoError(t, err)
	require.Equal(t, "1 elemento procesado", one)
	many, _, err := manager.Render(spanishContext, ItemsProcessed(3))
	require.NoError(t, err)
	require.Equal(t, "3 elementos procesados", many)
}

func TestManagerContextOverrideAndFallback(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, manager.DefaultTag())

	englishContext := WithPreferences(context.Background(), language.AmericanEnglish)
	text, tag, err := manager.Render(englishContext, InvalidRequestBody())
	require.NoError(t, err)
	require.Equal(t, "invalid request body", text)
	require.Equal(t, language.AmericanEnglish, tag)

	text, tag, err = manager.Render(context.Background(), Message{ID: "missing.message"})
	require.Error(t, err)
	require.Equal(t, "missing.message", text)
	require.Equal(t, language.AmericanEnglish, tag)
}

func TestManagerWarnsOnceForMissingPack(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, logger)
	require.NoError(t, err)

	manager.Resolve("http", language.French)
	manager.Resolve("http", language.French)

	require.Equal(t, 1, strings.Count(output.String(), "localization.language_pack_missing"))
}

func TestManagerWarnsOnceForMissingCatalogEntry(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	manager, err := NewManager([]language.Tag{language.EuropeanSpanish}, logger)
	require.NoError(t, err)
	spanishContext := WithPreferences(context.Background(), language.EuropeanSpanish)
	message := CatalogEntryMissing("es-ES", MessageInvalidRequestBody)

	text, tag, err := manager.Render(spanishContext, message)
	require.NoError(t, err)
	require.Equal(t, language.AmericanEnglish, tag)
	require.Equal(t, "Message server.invalid_request_body is unavailable in es-ES; using English (United States)", text)
	_, _, err = manager.Render(spanishContext, message)
	require.NoError(t, err)

	require.Equal(t, 1, strings.Count(strings.TrimSpace(output.String()), "\n")+1)
}
