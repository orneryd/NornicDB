package search

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestSearchLogEventEnglishFallbackAndAttrs(t *testing.T) {
	var output bytes.Buffer
	service := &Service{logger: slog.New(slog.NewTextHandler(&output, nil))}

	service.logEvent(context.Background(), slog.LevelInfo, localization.SearchQueryEmbeddingFallbackEvent("http", "unavailable", "offline"))

	line := output.String()
	require.Contains(t, line, `msg="query embedding failed; search fallback activated"`)
	require.Contains(t, line, "event_id=search.query_embedding_fallback")
	require.Contains(t, line, "transport=http")
	require.Contains(t, line, "fallback_reason=unavailable")
	require.Contains(t, line, "diagnostic=offline")
}

func TestSearchLogEventUsesInjectedLocalizer(t *testing.T) {
	var output bytes.Buffer
	manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)
	service := &Service{logger: slog.New(slog.NewTextHandler(&output, nil))}
	service.SetLocalizer(manager)

	service.logEvent(context.Background(), slog.LevelInfo, localization.SearchQueryEmbeddingFallbackEvent("http", "unavailable", "offline"))

	require.Contains(t, output.String(), "falló el embedding de la consulta; se activó la búsqueda alternativa")
}

func TestSearchOperatorEventStableIdentityAndStructuredArgs(t *testing.T) {
	cause := errors.New("backend unavailable")
	first := localization.SearchOperatorEvent("build failed after %d batches: %v reason=backend_error", 3, cause)
	second := localization.SearchOperatorEvent("build failed after %d batches: %v reason=backend_error", 7, cause)

	require.Equal(t, first.ID, second.ID)
	require.Equal(t, "build failed after 3 batches: backend unavailable reason=backend_error", first.Message.Fallback)
	require.Equal(t, "search", logEventAttr(first, "component").Value.String())
	require.Equal(t, "backend_error", logEventAttr(first, "reason").Value.String())
	// The raw template and positional arguments must not be re-emitted as
	// trailing attrs; they previously surfaced unprocessed at the end of lines.
	require.Equal(t, slog.Attr{}, logEventAttr(first, "message_template"))
	require.Equal(t, slog.Attr{}, logEventAttr(first, "arg_0"))
	require.Equal(t, slog.Attr{}, logEventAttr(first, "arg_1"))
}

func TestSearchLogCatalogsAreComplete(t *testing.T) {
	for _, path := range []string{
		"../localization/catalog/active.search-log.en-US.yaml",
		"../localization/catalog/active.search-log.es-ES.yaml",
		"../localization/catalog/active.search-log.en-XA.yaml",
	} {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.True(t, strings.Contains(string(content), "search-log.log.query_embedding_fallback"))
	}
}

func logEventAttr(event localization.LogEvent, key string) slog.Attr {
	for _, attr := range event.Attrs {
		if attr.Key == key {
			return attr
		}
	}
	return slog.Attr{}
}
