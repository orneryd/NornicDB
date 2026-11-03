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
	s.loadHeimdallPlugins(s.shutdown.work.Context(), heimdall.SubsystemHost{
		Config: cfg, Database: router, Metrics: metrics, Bifrost: bridge,
		Heimdall: heimdall.NewLiveHeimdallInvoker(manager, nil, bridge, router, metrics),
	}, server)
}

// loadHeimdallPlugins is shared by inference-enabled and plugin-only startup.
// callCtx carries the caller's own cancellation into the accepted initialization
// work; host supplies metadata only, and each registration publishes the host
// metadata with the work context that registration was admitted under.
func (s *Server) loadHeimdallPlugins(callCtx context.Context, host heimdall.SubsystemHost, server *mcp.Server) {
	constructorCtx, release, accepted := s.shutdown.work.Acquire(callCtx)
	if accepted {
		defer release()
	}
	host.Lifetime = &s.shutdown.work
	s.heimdallPluginsMu.Lock()
	if s.shutdown.work.Closing() {
		if router, ok := host.Database.(*heimdallDBRouter); ok {
			s.shutdown.mu.Lock()
			s.shutdown.unpublishedRouters = append(s.shutdown.unpublishedRouters, router)
			s.shutdown.mu.Unlock()
		}
		s.heimdallPluginsMu.Unlock()
		return
	}
	manager := heimdall.GetSubsystemManager()
	if s.heimdallSubsystem == nil {
		manager.SetHostContext(host)
	} else {
		manager.SetContext(host)
	}
	s.heimdallSubsystem = manager
	diagnosticCredentials := observability.ClientCredentials(host.Config.APIURL, host.Config.APIKey)
	diagnosticCredentials = append(diagnosticCredentials, observability.ClientCredentials(s.config.PluginsDir)...)
	diagnosticCredentials = append(diagnosticCredentials, observability.ClientCredentials(s.config.HeimdallPluginsDir)...)
	pluginHost := host
	pluginHost.Config.APIURL = observability.RedactText(host.Config.APIURL, diagnosticCredentials...)
	pluginHost.Config.APIKey = ""
	pluginsDir := observability.RedactText(s.config.PluginsDir, diagnosticCredentials...)
	heimdallPluginsDir := observability.RedactText(s.config.HeimdallPluginsDir, diagnosticCredentials...)
	if router, ok := host.Database.(*heimdallDBRouter); ok {
		s.heimdallCacheRouter = router
	}
	manager.SetContext(pluginHost)
	s.heimdallPluginsMu.Unlock()
	if dir := s.config.PluginsDir; dir != "" {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerAPOCPluginsLoadingEvent(pluginsDir))
		if err := nornicdb.LoadPluginsFromDir(constructorCtx, dir, &pluginHost); err != nil {
			err = observability.RedactError(err, diagnosticCredentials...)
			s.shutdown.work.RecordError("", err)
			s.logEvent(context.Background(), slog.LevelWarn, localization.ServerAPOCPluginsLoadFailedEvent(pluginsDir, err))
		}
	}
	if dir := s.config.HeimdallPluginsDir; dir != "" && dir != s.config.PluginsDir {
		s.logEvent(context.Background(), slog.LevelDebug, localization.ServerHeimdallPluginsLoadingEvent(heimdallPluginsDir))
		if err := nornicdb.LoadPluginsFromDir(constructorCtx, dir, &pluginHost); err != nil {
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
	registerHeimdallMCPTools(server, manager, pluginHost.Database, pluginHost.Metrics, pluginHost.Bifrost)
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
