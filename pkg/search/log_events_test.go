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

	service.logEvent(context.Background(), slog.LevelInfo, localization.SearchBM25EngineSelectedEvent("v2"))

	line := output.String()
	require.Contains(t, line, `msg="📇 Search: BM25 engine selected: v2"`)
	require.Contains(t, line, "event_id=search.bm25_engine.selected")
	require.Contains(t, line, "bm25_engine=v2")
}

func TestSearchLogEventUsesInjectedLocalizer(t *testing.T) {
	var output bytes.Buffer
	manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
	require.NoError(t, err)
	service := &Service{logger: slog.New(slog.NewTextHandler(&output, nil))}
	service.SetLocalizer(manager)

	service.logEvent(context.Background(), slog.LevelInfo, localization.SearchBM25EngineSelectedEvent("v2"))

	require.Contains(t, output.String(), "Motor BM25 de búsqueda seleccionado: v2")
}

func TestSearchOperatorEventStableIdentityAndStructuredArgs(t *testing.T) {
	cause := errors.New("backend unavailable")
	first := localization.SearchOperatorEvent("build failed after %d batches: %v reason=backend_error", 3, cause)
	second := localization.SearchOperatorEvent("build failed after %d batches: %v reason=backend_error", 7, cause)

	require.Equal(t, first.ID, second.ID)
	require.Equal(t, "build failed after 3 batches: backend unavailable reason=backend_error", first.Message.Fallback)
	require.Equal(t, "backend_error", logEventAttr(first, "reason").Value.String())
	require.Equal(t, int64(3), logEventAttr(first, "arg_0").Value.Int64())
	require.Equal(t, cause, logEventAttr(first, "arg_1").Value.Any())
}

func TestSearchLogCatalogsAreComplete(t *testing.T) {
	for _, path := range []string{
		"../localization/catalog/active.search-log.en-US.yaml",
		"../localization/catalog/active.search-log.es-ES.yaml",
		"../localization/catalog/active.search-log.en-XA.yaml",
	} {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.True(t, strings.Contains(string(content), "search-log.log.bm25_engine_selected"))
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
