package bolt

import (
	"context"
	"log/slog"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func (s *Server) logEvent(ctx context.Context, level slog.Level, event localization.LogEvent) {
	if s == nil {
		return
	}
	var localizer *localization.Manager
	if s.config != nil {
		localizer = s.config.Localizer
	}
	localizer.Log(ctx, s.logger(), level, event)
}
