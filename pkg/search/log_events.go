package search

import (
	"context"
	"log/slog"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func (s *Service) loggerOrDefault() *slog.Logger {
	if s != nil {
		if logger := s.logger.Load(); logger != nil {
			return logger
		}
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
	// Manager.Log delegates rendering to Manager.Render, whose nil receiver
	// contract emits the message fallback in American English.
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

// SetLogger injects the optional structured operator logger. It may be
// called while the service runs; later log calls use it.
func (s *Service) SetLogger(logger *slog.Logger) {
	if s != nil {
		s.logger.Store(logger)
	}
}
