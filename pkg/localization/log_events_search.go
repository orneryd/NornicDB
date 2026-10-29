package localization

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
)

const (
	// EventSearchQueryEmbeddingFallback identifies fail-open BM25 fallback after an embedding error.
	EventSearchQueryEmbeddingFallback EventID = "search.query_embedding_fallback"

	MessageSearchLogQueryEmbeddingFallback MessageID = "search-log.log.query_embedding_fallback"
	MessageSearchLogOperator               MessageID = "search-log.log.operator"
)

var searchReasonToken = regexp.MustCompile(`\breason=([a-z][a-z0-9_]*)`)

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

// SearchOperatorEvent converts a production search format string into a stable,
// structured event while retaining its exact historical English rendering.
// The formatted message is the single source of truth; the raw template and
// positional arguments are intentionally NOT re-emitted as trailing attrs,
// which previously surfaced as unprocessed "message_template" / "arg_N"
// key-value pairs at the end of each log line.
func SearchOperatorEvent(format string, args ...any) LogEvent {
	message := fmt.Sprintf(format, args...)
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(format))
	attrs := make([]slog.Attr, 0, 3)
	attrs = append(attrs, slog.String("component", "search"))
	if match := searchReasonToken.FindStringSubmatch(message); len(match) == 2 {
		attrs = append(attrs, slog.String("reason", match[1]))
	}
	return LogEvent{
		ID:      EventID(fmt.Sprintf("search.operator.%016x", hash.Sum64())),
		Message: SearchLogOperator(message),
		Attrs:   attrs,
	}
}
