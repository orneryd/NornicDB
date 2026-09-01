package search

import (
	"context"
	"log/slog"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func (s *Service) loggerOrDefault() *slog.Logger {
	if s != nil && s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

func (s *Service) logEvent(ctx context.Context, level slog.Level, event localization.LogEvent) {
	if s == nil {
		return
	}
	s.localizer.Log(ctx, s.loggerOrDefault(), level, event)
}

func logSearchEvent(ctx context.Context, logger *slog.Logger, localizer *localization.Manager, level slog.Level, event localization.LogEvent) {
	if logger == nil {
		logger = slog.Default()
	}
	localizer.Log(ctx, logger, level, event)
}

func (s *Service) logPrintf(format string, args ...any) {
	if s == nil {
		return
	}
	s.logEvent(context.Background(), slog.LevelInfo, localization.SearchOperatorEvent(format, args...))
}

func logSearchPrintf(format string, args ...any) {
	logSearchEvent(context.Background(), nil, nil, slog.LevelInfo, localization.SearchOperatorEvent(format, args...))
}

// SetLocalizer injects the optional operator-log localizer.
func (s *Service) SetLocalizer(manager *localization.Manager) {
	if s != nil {
		s.localizer = manager
	}
}

// SetLogger injects the optional structured operator logger.
func (s *Service) SetLogger(logger *slog.Logger) {
	if s != nil {
		s.logger = logger
	}
}
