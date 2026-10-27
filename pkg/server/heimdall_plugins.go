package server

import (
	"context"
	"errors"
	"log/slog"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// initHeimdallPluginsWithoutInference loads configured plugins without starting a
// model, chat handler, or agentic loop. Direct action invocation remains available.
func (s *Server) initHeimdallPluginsWithoutInference(features *nornicConfig.FeatureFlagsConfig, server *mcp.Server) {
	if s.config.PluginsDir == "" && s.config.HeimdallPluginsDir == "" {
		return
	}
	router := newHeimdallDBRouter(s.db, s.dbManager, features)
	metrics := &heimdallMetricsReader{}
	manager := heimdall.GetSubsystemManager()
	bridge := &heimdall.NoOpBifrost{}
	cfg := heimdall.ConfigFromFeatureFlags(features)
	cfg.Enabled = false
	cfg.BifrostEnabled = false
	s.loadHeimdallPlugins(heimdall.SubsystemContext{
		Config: cfg, Database: router, Metrics: metrics, Bifrost: bridge,
		Heimdall: heimdall.NewLiveHeimdallInvoker(manager, nil, bridge, router, metrics),
	}, server)
}

// loadHeimdallPlugins is shared by inference-enabled and plugin-only startup.
func (s *Server) loadHeimdallPlugins(ctx heimdall.SubsystemContext, server *mcp.Server) {
	s.heimdallPluginsMu.Lock()
	defer s.heimdallPluginsMu.Unlock()
	if s.closed.Load() {
		if router, ok := ctx.Database.(*heimdallDBRouter); ok {
			router.Close()
		}
		return
	}
	manager := heimdall.GetSubsystemManager()
	s.heimdallSubsystem = manager
	diagnosticCredentials := observability.ClientCredentials(ctx.Config.APIURL, ctx.Config.APIKey)
	diagnosticCredentials = append(diagnosticCredentials, observability.ClientCredentials(s.config.PluginsDir)...)
	diagnosticCredentials = append(diagnosticCredentials, observability.ClientCredentials(s.config.HeimdallPluginsDir)...)
	pluginContext := ctx
	pluginContext.Config.APIURL = observability.RedactText(ctx.Config.APIURL, diagnosticCredentials...)
	pluginContext.Config.APIKey = ""
	pluginsDir := observability.RedactText(s.config.PluginsDir, diagnosticCredentials...)
	heimdallPluginsDir := observability.RedactText(s.config.HeimdallPluginsDir, diagnosticCredentials...)
	if router, ok := ctx.Database.(*heimdallDBRouter); ok {
		s.heimdallCacheRouter = router
	}
	manager.SetContext(pluginContext)
	if dir := s.config.PluginsDir; dir != "" {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerAPOCPluginsLoadingEvent(pluginsDir))
		if err := nornicdb.LoadPluginsFromDir(dir, &pluginContext); err != nil {
			s.logEvent(context.Background(), slog.LevelWarn, localization.ServerAPOCPluginsLoadFailedEvent(pluginsDir, observability.RedactError(err, diagnosticCredentials...)))
		}
	}
	if dir := s.config.HeimdallPluginsDir; dir != "" && dir != s.config.PluginsDir {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerHeimdallPluginsLoadingEvent(heimdallPluginsDir))
		if err := nornicdb.LoadPluginsFromDir(dir, &pluginContext); err != nil {
			s.logEvent(context.Background(), slog.LevelWarn, localization.ServerHeimdallPluginsLoadFailedEvent(heimdallPluginsDir, observability.RedactError(err, diagnosticCredentials...)))
		}
	}
	if s.config.HeimdallPluginsDir == "" {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerHeimdallPluginsDirectoryEmptyEvent())
	} else if s.config.HeimdallPluginsDir == s.config.PluginsDir {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerHeimdallPluginsDirectoryDuplicateEvent(heimdallPluginsDir, pluginsDir))
	}
	registerHeimdallMCPTools(server, manager, pluginContext.Database, pluginContext.Metrics, pluginContext.Bifrost)
}

// stopHeimdallPlugins serializes cleanup with both plugin-only and async startup.
func (s *Server) stopHeimdallPlugins() error {
	s.heimdallPluginsMu.Lock()
	defer s.heimdallPluginsMu.Unlock()
	var err error
	if manager := s.heimdallSubsystem; manager != nil {
		err = observability.RedactError(errors.Join(manager.StopAll(), manager.ShutdownAll()))
		s.heimdallSubsystem = nil
	}
	if s.heimdallCacheRouter != nil {
		s.heimdallCacheRouter.Close()
	}
	return err
}
