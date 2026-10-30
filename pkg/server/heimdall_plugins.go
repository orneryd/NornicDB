package server

import (
	"context"
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
	// Plugin-only loading runs during construction, under the server's own
	// work lifetime; it has no other caller context.
	s.shutdown.init()
	s.loadHeimdallPlugins(heimdall.SubsystemContext{
		Context: s.shutdown.workContext,
		Config:  cfg, Database: router, Metrics: metrics, Bifrost: bridge,
		Heimdall: heimdall.NewLiveHeimdallInvoker(manager, nil, bridge, router, metrics),
	}, server)
}

// loadHeimdallPlugins is shared by inference-enabled and plugin-only startup.
func (s *Server) loadHeimdallPlugins(ctx heimdall.SubsystemContext, server *mcp.Server) {
	s.shutdown.init()
	constructorCtx, release, accepted := s.shutdown.acquire(ctx.Context)
	if accepted {
		defer release()
	}
	ctx.Context = constructorCtx
	ctx.Lifetime = &s.shutdown.work
	s.heimdallPluginsMu.Lock()
	if s.closed.Load() {
		if router, ok := ctx.Database.(*heimdallDBRouter); ok {
			s.shutdown.mu.Lock()
			s.shutdown.unpublishedRouters = append(s.shutdown.unpublishedRouters, router)
			s.shutdown.mu.Unlock()
		}
		s.heimdallPluginsMu.Unlock()
		return
	}
	manager := heimdall.GetSubsystemManager()
	if s.heimdallSubsystem == nil {
		manager.SetHostContext(ctx)
	} else {
		manager.SetContext(ctx)
	}
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
	s.heimdallPluginsMu.Unlock()
	if dir := s.config.PluginsDir; dir != "" {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerAPOCPluginsLoadingEvent(pluginsDir))
		if err := nornicdb.LoadPluginsFromDir(dir, &pluginContext); err != nil {
			err = observability.RedactError(err, diagnosticCredentials...)
			s.shutdown.work.RecordError("", err)
			s.logEvent(context.Background(), slog.LevelWarn, localization.ServerAPOCPluginsLoadFailedEvent(pluginsDir, err))
		}
	}
	if dir := s.config.HeimdallPluginsDir; dir != "" && dir != s.config.PluginsDir {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerHeimdallPluginsLoadingEvent(heimdallPluginsDir))
		if err := nornicdb.LoadPluginsFromDir(dir, &pluginContext); err != nil {
			err = observability.RedactError(err, diagnosticCredentials...)
			s.shutdown.work.RecordError("", err)
			s.logEvent(context.Background(), slog.LevelWarn, localization.ServerHeimdallPluginsLoadFailedEvent(heimdallPluginsDir, err))
		}
	}
	if s.config.HeimdallPluginsDir == "" {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerHeimdallPluginsDirectoryEmptyEvent())
	} else if s.config.HeimdallPluginsDir == s.config.PluginsDir {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerHeimdallPluginsDirectoryDuplicateEvent(heimdallPluginsDir, pluginsDir))
	}
	registerHeimdallMCPTools(server, manager, pluginContext.Database, pluginContext.Metrics, pluginContext.Bifrost)
}

// stopHeimdallPlugins is the legacy internal entry into the common protocol.
func (s *Server) stopHeimdallPlugins() error {
	return s.Stop(context.Background())
}

func (s *Server) signalHeimdallPlugins() error {
	s.heimdallPluginsMu.Lock()
	manager := s.heimdallSubsystem
	s.heimdallPluginsMu.Unlock()
	if manager == nil {
		return nil
	}
	manager.CloseAdmission()
	return manager.StopAll()
}

func (s *Server) disposeHeimdallPlugins() error {
	s.heimdallPluginsMu.Lock()
	manager, router := s.heimdallSubsystem, s.heimdallCacheRouter
	s.heimdallSubsystem = nil
	s.heimdallPluginsMu.Unlock()
	var err error
	if manager != nil {
		err = manager.ShutdownAll()
	}
	if router != nil {
		router.Close()
	}
	return err
}
