package cypher

import (
	"context"
	"log/slog"
	"unicode/utf8"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type logEventLocalizer interface {
	Log(context.Context, *slog.Logger, slog.Level, localization.LogEvent)
}

func (e *StorageExecutor) logEvent(level slog.Level, event localization.LogEvent) {
	if e == nil {
		return
	}
	ctx := context.Background()
	if localizer, ok := e.localizationRenderer.(logEventLocalizer); ok {
		localizer.Log(ctx, e.logger(), level, event)
		return
	}

	attrs := make([]slog.Attr, 0, len(event.Attrs)+1)
	attrs = append(attrs, slog.String("event_id", string(event.ID)))
	for _, attr := range event.Attrs {
		if attr.Key != "event_id" {
			attrs = append(attrs, attr)
		}
	}
	e.logger().LogAttrs(ctx, level, event.Message.Fallback, attrs...)
}

func (e *StorageExecutor) emitRejectionReport(query string, err error) {
	if e == nil || err == nil {
		return
	}
	logger := e.log.Load()
	if logger == nil || !logger.Enabled(context.Background(), slog.LevelInfo) || !nornicerrors.HasNeo4jStatus(err) {
		return
	}
	code, _ := nornicerrors.Neo4jStatus(err)
	if code != "Neo.ClientError.Statement.SyntaxError" {
		return
	}
	redacted := RedactLiterals(StripComments(query))
	hash := StatementShapeHash(redacted)
	statementClass := "OTHER"
	for _, start := range validSyntaxStarts {
		if startsWithKeywordFold(query, start) {
			statementClass = start
			break
		}
	}
	if len(redacted) > 500 {
		redacted = truncateRuneSafe(redacted, 500)
	}
	logger.Info("query rejected", "event_id", "cypher.query_rejected", "event", "query_rejected", "reason", "syntax_error", "statement_class", statementClass, "shape_hash", hash, "query", redacted)
}

// truncateRuneSafe returns s truncated to at most max bytes without splitting
// a UTF-8 rune. The log truncation seams must never emit invalid UTF-8.
func truncateRuneSafe(s string, max int) string {
	if len(s) <= max {
		return s
	}
	end := max
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end]
}
