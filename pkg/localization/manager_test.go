package localization

import (
	"bytes"
	"context"
	"encoding/json"
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
	message := Message{ID: "test.missing_message"}

	text, tag, err := manager.Render(spanishContext, message)
	require.Error(t, err)
	require.Equal(t, language.AmericanEnglish, tag)
	require.Equal(t, "test.missing_message", text)
	_, _, err = manager.Render(spanishContext, message)
	require.Error(t, err)

	require.Equal(t, uint64(2), manager.MissingCatalogEntryCount())
	require.Equal(t, 1, strings.Count(strings.TrimSpace(output.String()), "\n")+1)
}

func TestManagerLogPreservesEventIdentityAndFieldsAcrossLocales(t *testing.T) {
	event := LogEvent{
		ID:      "server.request.invalid_body",
		Message: InvalidRequestBody(),
		Attrs: []slog.Attr{
			slog.String("component", "server"),
			slog.Int("status", 400),
		},
	}

	logs := make([]map[string]any, 0, 2)
	for _, tag := range []language.Tag{language.AmericanEnglish, language.EuropeanSpanish} {
		var output bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&output, nil))
		manager, err := NewManager([]language.Tag{tag}, nil)
		require.NoError(t, err)

		manager.Log(WithPreferences(context.Background(), tag), logger, slog.LevelWarn, event)

		var record map[string]any
		require.NoError(t, json.Unmarshal(output.Bytes(), &record))
		logs = append(logs, record)
	}

	require.Equal(t, "invalid request body", logs[0]["msg"])
	require.Equal(t, "cuerpo de solicitud no válido", logs[1]["msg"])
	for _, field := range []string{"level", "event_id", "component", "status"} {
		require.Equal(t, logs[0][field], logs[1][field], "field %s changed with locale", field)
	}
	require.Equal(t, "server.request.invalid_body", logs[0]["event_id"])
}
