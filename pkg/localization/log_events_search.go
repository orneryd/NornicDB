package localization

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
)

const (
	// EventSearchBM25EngineSelected identifies selection of the active BM25 implementation.
	EventSearchBM25EngineSelected EventID = "search.bm25_engine.selected"
	// EventSearchQueryEmbeddingFallback identifies fail-open BM25 fallback after an embedding error.
	EventSearchQueryEmbeddingFallback EventID = "search.query_embedding_fallback"

	MessageSearchLogBM25EngineSelected     MessageID = "search-log.log.bm25_engine_selected"
	MessageSearchLogQueryEmbeddingFallback MessageID = "search-log.log.query_embedding_fallback"
	MessageSearchLogOperator               MessageID = "search-log.log.operator"
)

var searchReasonToken = regexp.MustCompile(`\breason=([a-z][a-z0-9_]*)`)

// SearchLogBM25EngineSelected describes BM25 engine selection.
func SearchLogBM25EngineSelected(engine string) Message {
	return Message{
		ID:       MessageSearchLogBM25EngineSelected,
		Fallback: "📇 Search: BM25 engine selected: " + engine,
		Data:     map[string]any{"Engine": engine},
	}
}

// SearchLogQueryEmbeddingFallback describes fail-open search after query embedding fails.
func SearchLogQueryEmbeddingFallback() Message {
	return Message{ID: MessageSearchLogQueryEmbeddingFallback, Fallback: "query embedding failed; search fallback activated"}
}

// SearchLogOperator preserves dynamically formatted operator prose.
func SearchLogOperator(message string) Message {
	return Message{
		ID:       MessageSearchLogOperator,
		Fallback: message,
		Data:     map[string]any{"Message": message},
	}
}

// SearchQueryEmbeddingFallbackEvent describes fail-open search after query embedding fails.
func SearchQueryEmbeddingFallbackEvent(transport, reason, diagnostic string) LogEvent {
	return LogEvent{
		ID:      EventSearchQueryEmbeddingFallback,
		Message: SearchLogQueryEmbeddingFallback(),
		Attrs: []slog.Attr{
			slog.String("component", "search"),
			slog.String("transport", transport),
			slog.String("fallback_reason", reason),
			slog.String("diagnostic", diagnostic),
		},
	}
}

// SearchBM25EngineSelectedEvent describes selection of the active BM25 implementation.
func SearchBM25EngineSelectedEvent(engine string) LogEvent {
	return LogEvent{
		ID:      EventSearchBM25EngineSelected,
		Message: SearchLogBM25EngineSelected(engine),
		Attrs: []slog.Attr{
			slog.String("component", "search"),
			slog.String("bm25_engine", engine),
		},
	}
}

// SearchOperatorEvent converts a production search format string into a stable,
// structured event while retaining its exact historical English rendering.
func SearchOperatorEvent(format string, args ...any) LogEvent {
	message := fmt.Sprintf(format, args...)
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(format))
	attrs := make([]slog.Attr, 0, len(args)+3)
	attrs = append(attrs,
		slog.String("component", "search"),
		slog.String("message_template", format),
	)
	for index, arg := range args {
		attrs = append(attrs, slog.Any(fmt.Sprintf("arg_%d", index), arg))
	}
	if match := searchReasonToken.FindStringSubmatch(message); len(match) == 2 {
		attrs = append(attrs, slog.String("reason", match[1]))
	}
	return LogEvent{
		ID:      EventID(fmt.Sprintf("search.operator.%016x", hash.Sum64())),
		Message: SearchLogOperator(message),
		Attrs:   attrs,
	}
}
