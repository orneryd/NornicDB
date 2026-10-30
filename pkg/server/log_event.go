package server

import (
	"context"
	"log/slog"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func (s *Server) logEvent(ctx context.Context, level slog.Level, event localization.LogEvent) {
	if s == nil {
		return
	}
	s.localizer.Log(ctx, s.log, level, event)
}
