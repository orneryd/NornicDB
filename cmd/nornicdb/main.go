// Package main provides the NornicDB CLI entry point.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/cache"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/pool"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/orneryd/nornicdb/ui"
)

func main() {
	// Route logs to stdout so container/system log collectors see them.
	log.SetOutput(os.Stdout)
	preferences, err := localization.ResolveProcessPreferences(localization.AutoLanguage)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	localizer, err := localization.NewManager(preferences.Preferences, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}

	if err := newRootCommand(localizer).Execute(); err != nil {
		os.Exit(1)
	}
}

type commandLocalizerContextKey struct{}

type commandLocalization struct {
	mu      sync.RWMutex
	manager *localization.Manager
}

type commandError struct {
	cmd     *cobra.Command
	manager *localization.Manager
	message localization.Message
	cause   error
}

func (e *commandError) Error() string {
	manager := e.manager
	if e.cmd != nil {
		manager = commandLocalizer(e.cmd)
	}
	return commandText(manager, e.message)
}
func (e *commandError) Unwrap() error { return e.cause }

func commandText(manager *localization.Manager, message localization.Message) string {
	if manager == nil {
		return message.Fallback
	}
	text, _, err := manager.Render(context.Background(), message)
	if err != nil {
		return message.Fallback
	}
	return text
}

func newCommandError(cmd *cobra.Command, message localization.Message, cause error) error {
	return &commandError{cmd: cmd, message: message, cause: cause}
}

func newCLIError(manager *localization.Manager, message localization.Message, cause error) error {
	return &commandError{manager: manager, message: message, cause: cause}
}

func commandLocalizer(cmd *cobra.Command) *localization.Manager {
	if cmd == nil {
		return nil
	}
	state, _ := cmd.Context().Value(commandLocalizerContextKey{}).(*commandLocalization)
	if state == nil {
		return nil
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.manager
}

func setCommandLocalizer(cmd *cobra.Command, manager *localization.Manager) {
	if cmd == nil {
		return
	}
	state, _ := cmd.Context().Value(commandLocalizerContextKey{}).(*commandLocalization)
	if state == nil {
		state = &commandLocalization{}
		cmd.SetContext(context.WithValue(cmd.Context(), commandLocalizerContextKey{}, state))
	}
	state.mu.Lock()
	state.manager = manager
	state.mu.Unlock()
}

func commandPrintln(cmd *cobra.Command, message localization.Message) {
	cmd.Println(commandText(commandLocalizer(cmd), message))
}

func newRootCommand(localizer *localization.Manager) *cobra.Command {
	text := func(message localization.Message) string { return commandText(localizer, message) }

	rootCmd := &cobra.Command{
		Use:   "nornicdb",
		Short: text(localization.NornicDBCLIRootShort()),
		Long:  text(localization.NornicDBCLIRootLong()),
	}
	rootCmd.SetContext(context.WithValue(context.Background(), commandLocalizerContextKey{}, &commandLocalization{manager: localizer}))

	// Global config flag (applies to all subcommands that load config).
	// This is the primary configuration mechanism for Docker/K8s where the config
	// is commonly mounted at a fixed path (e.g. /config/nornicdb.yaml).
	rootCmd.PersistentFlags().String("config", getEnvStr("NORNICDB_CONFIG", ""), text(localization.NornicDBCLIFlagConfig()))

	// Version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: text(localization.NornicDBCLIVersionShort()),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("NornicDB %s\n", buildinfo.DisplayVersion())
		},
	})

	// Serve command
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: text(localization.NornicDBCLIServeShort()),
		Long:  text(localization.NornicDBCLIServeLong()),
		RunE:  runServe,
	}
	serveCmd.Flags().Int("bolt-port", getEnvInt("NORNICDB_BOLT_PORT", 7687), text(localization.NornicDBCLIFlagBoltPort()))
	serveCmd.Flags().Int("http-port", getEnvInt("NORNICDB_HTTP_PORT", 7474), text(localization.NornicDBCLIFlagHTTPPort()))
	serveCmd.Flags().String("address", getEnvStr("NORNICDB_ADDRESS", "127.0.0.1"), text(localization.NornicDBCLIFlagAddress()))
	serveCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), text(localization.NornicDBCLIFlagDataDir()))
	serveCmd.Flags().Bool("upgrade-storage", getEnvBool("NORNICDB_UPGRADE_STORAGE", false), text(localization.NornicDBCLIFlagUpgradeStorage()))
	serveCmd.Flags().String("embedding-provider", getEnvStr("NORNICDB_EMBEDDING_PROVIDER", "local"), text(localization.NornicDBCLIFlagEmbeddingProvider()))
	serveCmd.Flags().String("embedding-url", getEnvStr("NORNICDB_EMBEDDING_API_URL", "http://localhost:11434"), text(localization.NornicDBCLIFlagEmbeddingURL()))
	serveCmd.Flags().String("embedding-key", getEnvStr("NORNICDB_EMBEDDING_API_KEY", ""), text(localization.NornicDBCLIFlagEmbeddingKey()))
	serveCmd.Flags().String("embedding-model", getEnvStr("NORNICDB_EMBEDDING_MODEL", "bge-m3"), text(localization.NornicDBCLIFlagEmbeddingModel()))
	serveCmd.Flags().Int("embedding-dim", getEnvInt("NORNICDB_EMBEDDING_DIMENSIONS", 1024), text(localization.NornicDBCLIFlagEmbeddingDimensions()))
	serveCmd.Flags().Int("embedding-cache", getEnvInt("NORNICDB_EMBEDDING_CACHE_SIZE", 10000), text(localization.NornicDBCLIFlagEmbeddingCache()))
	serveCmd.Flags().Int("embedding-gpu-layers", getEnvInt("NORNICDB_EMBEDDING_GPU_LAYERS", -1), text(localization.NornicDBCLIFlagEmbeddingGPULayers()))
	serveCmd.Flags().Bool("embedding-enabled", getEnvBool("NORNICDB_EMBEDDING_ENABLED", false), text(localization.NornicDBCLIFlagEmbeddingEnabled()))
	// Per-database search index master switches and warming triggers (global
	// defaults; per-DB overrides via /admin/databases/{name}/config always win).
	// Defaults reproduce today's behaviour: both indexes enabled, both built at startup.
	serveCmd.Flags().Bool("search-bm25-enabled", getEnvBool("NORNICDB_SEARCH_BM25_ENABLED", true), text(localization.NornicDBCLIFlagSearchBM25Enabled()))
	serveCmd.Flags().String("search-bm25-warming", getEnvStr("NORNICDB_SEARCH_BM25_WARMING", "startup"), text(localization.NornicDBCLIFlagSearchBM25Warming()))
	serveCmd.Flags().Bool("search-vector-enabled", getEnvBool("NORNICDB_SEARCH_VECTOR_ENABLED", true), text(localization.NornicDBCLIFlagSearchVectorEnabled()))
	serveCmd.Flags().String("search-vector-warming", getEnvStr("NORNICDB_SEARCH_VECTOR_WARMING", "startup"), text(localization.NornicDBCLIFlagSearchVectorWarming()))
	serveCmd.Flags().String("gpu-backend", getEnvStr("NORNICDB_GPU_BACKEND", ""), text(localization.NornicDBCLIFlagGPUBackend()))
	serveCmd.Flags().Bool("no-auth", false, text(localization.NornicDBCLIFlagNoAuth()))
	serveCmd.Flags().String("admin-password", "password", text(localization.NornicDBCLIFlagAdminPassword()))
	serveCmd.Flags().Bool("mcp-enabled", getEnvBool("NORNICDB_MCP_ENABLED", true), text(localization.NornicDBCLIFlagMCPEnabled()))
	// Parallel execution flags
	serveCmd.Flags().Bool("parallel", true, text(localization.NornicDBCLIFlagParallel()))
	serveCmd.Flags().Int("parallel-workers", 0, text(localization.NornicDBCLIFlagParallelWorkers()))
	serveCmd.Flags().Int("parallel-batch-size", 1000, text(localization.NornicDBCLIFlagParallelBatchSize()))
	// Memory management flags
	serveCmd.Flags().String("memory-limit", "", text(localization.NornicDBCLIFlagMemoryLimit()))
	serveCmd.Flags().Int("gc-percent", 100, text(localization.NornicDBCLIFlagGCPercent()))
	serveCmd.Flags().Bool("pool-enabled", true, text(localization.NornicDBCLIFlagPoolEnabled()))
	serveCmd.Flags().Bool("low-memory", getEnvBool("NORNICDB_LOW_MEMORY", false), text(localization.NornicDBCLIFlagLowMemory()))
	serveCmd.Flags().Int("query-cache-size", 1000, text(localization.NornicDBCLIFlagQueryCacheSize()))
	serveCmd.Flags().String("query-cache-ttl", "5m", text(localization.NornicDBCLIFlagQueryCacheTTL()))
	// Logging flags
	serveCmd.Flags().Bool("log-queries", getEnvBool("NORNICDB_LOG_QUERIES", false), text(localization.NornicDBCLIFlagLogQueries()))
	serveCmd.Flags().Int("stdio-log-max-kb", getEnvInt("NORNICDB_STDIO_LOG_MAX_KB", 20480), text(localization.NornicDBCLIFlagStdioLogMaxKB()))
	serveCmd.Flags().Int("stdio-log-compact-seconds", getEnvInt("NORNICDB_STDIO_LOG_COMPACT_SECONDS", 3600), text(localization.NornicDBCLIFlagStdioLogCompactSeconds()))
	// Headless mode
	serveCmd.Flags().Bool("headless", getEnvBool("NORNICDB_HEADLESS", false), text(localization.NornicDBCLIFlagHeadless()))
	// Base path for reverse proxy deployment
	serveCmd.Flags().String("base-path", getEnvStr("NORNICDB_BASE_PATH", ""), text(localization.NornicDBCLIFlagBasePath()))

	// Replication / HA (pkg/replication). These map to NORNICDB_CLUSTER_* env vars.
	serveCmd.Flags().String("cluster-mode", getEnvStr("NORNICDB_CLUSTER_MODE", ""), text(localization.NornicDBCLIFlagClusterMode()))
	serveCmd.Flags().String("cluster-node-id", getEnvStr("NORNICDB_CLUSTER_NODE_ID", ""), text(localization.NornicDBCLIFlagClusterNodeID()))
	serveCmd.Flags().String("cluster-bind-addr", getEnvStr("NORNICDB_CLUSTER_BIND_ADDR", ""), text(localization.NornicDBCLIFlagClusterBindAddress()))
	serveCmd.Flags().String("cluster-advertise-addr", getEnvStr("NORNICDB_CLUSTER_ADVERTISE_ADDR", ""), text(localization.NornicDBCLIFlagClusterAdvertiseAddress()))
	serveCmd.Flags().String("cluster-data-dir", getEnvStr("NORNICDB_CLUSTER_DATA_DIR", ""), text(localization.NornicDBCLIFlagClusterDataDir()))

	// HA standby
	serveCmd.Flags().String("cluster-ha-role", getEnvStr("NORNICDB_CLUSTER_HA_ROLE", ""), text(localization.NornicDBCLIFlagClusterHARole()))
	serveCmd.Flags().String("cluster-ha-peer-addr", getEnvStr("NORNICDB_CLUSTER_HA_PEER_ADDR", ""), text(localization.NornicDBCLIFlagClusterHAPeerAddress()))

	// Raft
	serveCmd.Flags().Bool("cluster-raft-bootstrap", getEnvBool("NORNICDB_CLUSTER_RAFT_BOOTSTRAP", false), text(localization.NornicDBCLIFlagClusterRaftBootstrap()))
	serveCmd.Flags().String("cluster-raft-peers", getEnvStr("NORNICDB_CLUSTER_RAFT_PEERS", ""), text(localization.NornicDBCLIFlagClusterRaftPeers()))
	rootCmd.AddCommand(serveCmd)

	// Init command
	initCmd := &cobra.Command{
		Use:   "init",
		Short: text(localization.NornicDBCLIInitShort()),
		RunE:  runInit,
	}
	initCmd.Flags().String("data-dir", "./data", text(localization.NornicDBCLIFlagDataDir()))
	rootCmd.AddCommand(initCmd)

	// Shell command (interactive Cypher REPL)
	shellCmd := &cobra.Command{
		Use:   "shell",
		Short: text(localization.NornicDBCLIShellShort()),
		RunE:  runShell,
	}
	shellCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), text(localization.NornicDBCLIFlagDataDir()))
	shellCmd.Flags().String("uri", "bolt://localhost:7687", text(localization.NornicDBCLIFlagURI()))
	rootCmd.AddCommand(shellCmd)

	// Decay command (manual decay operations)
	decayCmd := &cobra.Command{
		Use:   "decay",
		Short: text(localization.NornicDBCLIDecayShort()),
	}
	decaySuppressCmd := &cobra.Command{
		Use:   "suppress",
		Short: text(localization.NornicDBCLIDecaySuppressShort()),
		RunE:  runDecaySuppress,
	}
	decaySuppressCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), text(localization.NornicDBCLIFlagDataDir()))
	decaySuppressCmd.Flags().Float64("threshold", 0.05, text(localization.NornicDBCLIFlagDecayThreshold()))
	decayCmd.AddCommand(decaySuppressCmd)

	decayStatsCmd := &cobra.Command{
		Use:   "stats",
		Short: text(localization.NornicDBCLIDecayStatsShort()),
		RunE:  runDecayStats,
	}
	decayStatsCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), text(localization.NornicDBCLIFlagDataDir()))
	decayCmd.AddCommand(decayStatsCmd)
	rootCmd.AddCommand(decayCmd)
	return rootCmd
}

// setCLIOverride stamps a single canonical NORNICDB_* key into
// cfg.CLIOverrides. Initializes the map lazily so non-CLI consumers
// (LoadFromEnv / LoadFromFile) don't have to remember to seed it.
//
// The CLI override layer is the highest-precedence source consulted by
// dbconfig.Resolve — an explicit boot-time CLI flag must trump any
// per-DB value stored in the dbconfig store, which in turn trumps the
// env/YAML-loaded global. See docs/operations/configuration.md for
// the full ladder.
func setCLIOverride(cfg *config.Config, key, value string) {
	if cfg.CLIOverrides == nil {
		cfg.CLIOverrides = make(map[string]string)
	}
	cfg.CLIOverrides[key] = value
}

// boolToCLIString renders a bool in the same shape the dbconfig store
// uses ("true"/"false"). The resolver's parseBoolFallback recognises
// both "true"/"false" and "1"/"0", so the literal here just needs to
// be unambiguous.
func boolToCLIString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// normalizeWarmingFlag mirrors the resolver's normalizeWarming: anything
// other than "lazy" (case-insensitive, trimmed) collapses to "startup".
// Centralised here so the CLI override path uses the same canonical
// values that dbconfig.Resolve produces.
func normalizeWarmingFlag(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "lazy":
		return "lazy"
	default:
		return "startup"
	}
}

func applyLowMemoryMode(cfg *config.Config) {
	cfg.Storage.Mode = "low"
	cfg.Database.BadgerNodeCacheMaxEntries = 1000
	cfg.Database.BadgerEdgeTypeCacheMaxTypes = 10
}

func runServe(cmd *cobra.Command, args []string) error {
	boltPort, _ := cmd.Flags().GetInt("bolt-port")
	httpPort, _ := cmd.Flags().GetInt("http-port")
	address, _ := cmd.Flags().GetString("address")
	dataDir, _ := cmd.Flags().GetString("data-dir")
	upgradeStorage, _ := cmd.Flags().GetBool("upgrade-storage")
	embeddingProvider, _ := cmd.Flags().GetString("embedding-provider")
	embeddingURL, _ := cmd.Flags().GetString("embedding-url")
	embeddingKey, _ := cmd.Flags().GetString("embedding-key")
	embeddingModel, _ := cmd.Flags().GetString("embedding-model")
	embeddingDim, _ := cmd.Flags().GetInt("embedding-dim")
	embeddingCache, _ := cmd.Flags().GetInt("embedding-cache")
	embeddingGPULayers, _ := cmd.Flags().GetInt("embedding-gpu-layers")
	embeddingEnabledFlag, _ := cmd.Flags().GetBool("embedding-enabled")
	gpuBackend, _ := cmd.Flags().GetString("gpu-backend")
	noAuth, _ := cmd.Flags().GetBool("no-auth")

	// Set environment variable for local embedder GPU configuration
	if embeddingProvider == "local" {
		os.Setenv("NORNICDB_EMBEDDING_GPU_LAYERS", fmt.Sprintf("%d", embeddingGPULayers))
	}
	adminPasswordFlag, _ := cmd.Flags().GetString("admin-password")
	adminPasswordFlagChanged := cmd.Flags().Changed("admin-password")
	mcpEnabled, _ := cmd.Flags().GetBool("mcp-enabled")
	parallelEnabled, _ := cmd.Flags().GetBool("parallel")
	parallelWorkers, _ := cmd.Flags().GetInt("parallel-workers")
	parallelBatchSize, _ := cmd.Flags().GetInt("parallel-batch-size")
	// Memory management flags
	memoryLimit, _ := cmd.Flags().GetString("memory-limit")
	gcPercent, _ := cmd.Flags().GetInt("gc-percent")
	poolEnabled, _ := cmd.Flags().GetBool("pool-enabled")
	queryCacheSize, _ := cmd.Flags().GetInt("query-cache-size")
	queryCacheTTL, _ := cmd.Flags().GetString("query-cache-ttl")
	logQueries, _ := cmd.Flags().GetBool("log-queries")
	stdioLogMaxKB, _ := cmd.Flags().GetInt("stdio-log-max-kb")
	stdioLogCompactSeconds, _ := cmd.Flags().GetInt("stdio-log-compact-seconds")
	headless, _ := cmd.Flags().GetBool("headless")
	basePath, _ := cmd.Flags().GetString("base-path")

	// Apply cluster flags as env vars so pkg/replication (and nornicdb.Open) can pick them up.
	// This keeps the replication config source-of-truth in NORNICDB_CLUSTER_* while providing a CLI UX.
	setEnvIfChanged := func(flagName, envName, value string) {
		if cmd.Flags().Changed(flagName) && value != "" {
			_ = os.Setenv(envName, value)
		}
	}
	setEnvIfChangedBool := func(flagName, envName string, value bool) {
		if cmd.Flags().Changed(flagName) {
			_ = os.Setenv(envName, fmt.Sprintf("%t", value))
		}
	}

	clusterMode, _ := cmd.Flags().GetString("cluster-mode")
	clusterNodeID, _ := cmd.Flags().GetString("cluster-node-id")
	clusterBindAddr, _ := cmd.Flags().GetString("cluster-bind-addr")
	clusterAdvertiseAddr, _ := cmd.Flags().GetString("cluster-advertise-addr")
	clusterDataDir, _ := cmd.Flags().GetString("cluster-data-dir")
	clusterHARole, _ := cmd.Flags().GetString("cluster-ha-role")
	clusterHAPeerAddr, _ := cmd.Flags().GetString("cluster-ha-peer-addr")
	clusterRaftBootstrap, _ := cmd.Flags().GetBool("cluster-raft-bootstrap")
	clusterRaftPeers, _ := cmd.Flags().GetString("cluster-raft-peers")

	setEnvIfChanged("cluster-mode", "NORNICDB_CLUSTER_MODE", clusterMode)
	setEnvIfChanged("cluster-node-id", "NORNICDB_CLUSTER_NODE_ID", clusterNodeID)
	setEnvIfChanged("cluster-bind-addr", "NORNICDB_CLUSTER_BIND_ADDR", clusterBindAddr)
	setEnvIfChanged("cluster-advertise-addr", "NORNICDB_CLUSTER_ADVERTISE_ADDR", clusterAdvertiseAddr)
	setEnvIfChanged("cluster-data-dir", "NORNICDB_CLUSTER_DATA_DIR", clusterDataDir)
	setEnvIfChanged("cluster-ha-role", "NORNICDB_CLUSTER_HA_ROLE", clusterHARole)
	setEnvIfChanged("cluster-ha-peer-addr", "NORNICDB_CLUSTER_HA_PEER_ADDR", clusterHAPeerAddr)
	setEnvIfChangedBool("cluster-raft-bootstrap", "NORNICDB_CLUSTER_RAFT_BOOTSTRAP", clusterRaftBootstrap)
	setEnvIfChanged("cluster-raft-peers", "NORNICDB_CLUSTER_RAFT_PEERS", clusterRaftPeers)

	if stdioLogCompactSeconds < 0 {
		cause := fmt.Errorf("stdio-log-compact-seconds must be >= 0")
		return newCommandError(cmd, localization.NornicDBCLIInvalidStdioInterval(stdioLogCompactSeconds), cause)
	}
	stopStdioCompactor := startStdioLogCompactor(stdioLogMaxKB, time.Duration(stdioLogCompactSeconds)*time.Second)
	defer stopStdioCompactor()

	// Apply memory configuration FIRST (before heavy allocations)
	// First, try to load from config file, then fall back to environment variables
	var cfg *config.Config
	loadedConfigFile := false
	explicitConfigPath, _ := cmd.Flags().GetString("config") // persistent
	configPath := strings.TrimSpace(explicitConfigPath)
	if configPath == "" {
		configPath = config.FindConfigFile()
	}

	var configStatus localization.Message
	if configPath == "" {
		cfg = config.LoadFromEnv()
		configStatus = localization.NornicDBCLINoConfig()
	} else {
		var err error
		cfg, err = config.LoadFromFile(configPath)
		if err != nil {
			// If the user explicitly set --config, fail fast.
			if strings.TrimSpace(explicitConfigPath) != "" {
				return newCommandError(cmd, localization.NornicDBCLIConfigLoadFailed(configPath, err), err)
			}
			configStatus = localization.NornicDBCLIConfigWarning(configPath, err)
			cfg = config.LoadFromEnv()
		} else {
			loadedConfigFile = true
			configStatus = localization.NornicDBCLIConfigLoaded(configPath)
		}
	}
	processPreferences, err := localization.ResolveProcessPreferences(cfg.Localization.Language)
	if err != nil {
		return fmt.Errorf("resolve localization preferences: %w", err)
	}
	localizer, err := localization.NewManager(processPreferences.Preferences, nil)
	if err != nil {
		return fmt.Errorf("initialize localization: %w", err)
	}
	setCommandLocalizer(cmd, localizer)
	commandPrintln(cmd, configStatus)

	resolvedAddress := resolveBindAddress(cmd, cfg, address, loadedConfigFile)
	cfg.Server.HTTPAddress = resolvedAddress
	cfg.Server.BoltAddress = resolvedAddress
	if noAuth {
		cfg.Auth.Enabled = false
	}
	if err := config.ValidateSecurityConfiguration(cfg); err != nil {
		return err
	}

	// YAML config file is the source of truth for embedding settings
	// Always use config file values if they are set (non-zero/non-empty)
	if cfg.Memory.EmbeddingDimensions > 0 {
		embeddingDim = cfg.Memory.EmbeddingDimensions
	}
	if cfg.Memory.EmbeddingProvider != "" {
		embeddingProvider = cfg.Memory.EmbeddingProvider
	}
	if cfg.Memory.EmbeddingModel != "" {
		embeddingModel = cfg.Memory.EmbeddingModel
	}
	if cfg.Memory.EmbeddingAPIURL != "" {
		embeddingURL = cfg.Memory.EmbeddingAPIURL
	}
	// Allow explicit CLI override for embedding enablement.
	if cmd.Flags().Changed("embedding-enabled") {
		cfg.Memory.EmbeddingEnabled = embeddingEnabledFlag
	}
	// Allow explicit CLI override for embedding cache.
	if cmd.Flags().Changed("embedding-cache") {
		cfg.Memory.EmbeddingCacheSize = embeddingCache
	}
	// Per-DB search index master switches.
	//
	// CLI flags here serve two purposes:
	//   1. Update cfg.Memory.Search* so the global default reflects
	//      the operator's choice (lower-precedence sources like YAML
	//      and env-loaded values are already in cfg by this point;
	//      the assignment here is the "CLI > env > YAML" overlay).
	//   2. Stamp cfg.CLIOverrides with the canonical NORNICDB_* key.
	//      dbconfig.Resolve treats CLIOverrides as the highest
	//      precedence source — strictly above per-DB store entries —
	//      so an operator's `--search-*=false` at boot acts as a
	//      kill switch even if a tenant has set per-DB values.
	//
	// Keys parallel the dbconfig store's NORNICDB_* namespace so
	// the resolver consults a single override map regardless of who
	// produced the value.
	if cmd.Flags().Changed("search-bm25-enabled") {
		v, _ := cmd.Flags().GetBool("search-bm25-enabled")
		cfg.Memory.SearchBM25Enabled = v
		setCLIOverride(cfg, "NORNICDB_SEARCH_BM25_ENABLED", boolToCLIString(v))
	}
	if cmd.Flags().Changed("search-bm25-warming") {
		v, _ := cmd.Flags().GetString("search-bm25-warming")
		warming := normalizeWarmingFlag(v)
		cfg.Memory.SearchBM25Warming = warming
		setCLIOverride(cfg, "NORNICDB_SEARCH_BM25_WARMING", warming)
	}
	if cmd.Flags().Changed("search-vector-enabled") {
		v, _ := cmd.Flags().GetBool("search-vector-enabled")
		cfg.Memory.SearchVectorEnabled = v
		setCLIOverride(cfg, "NORNICDB_SEARCH_VECTOR_ENABLED", boolToCLIString(v))
	}
	if cmd.Flags().Changed("search-vector-warming") {
		v, _ := cmd.Flags().GetString("search-vector-warming")
		warming := normalizeWarmingFlag(v)
		cfg.Memory.SearchVectorWarming = warming
		setCLIOverride(cfg, "NORNICDB_SEARCH_VECTOR_WARMING", warming)
	}

	// Override with CLI flags if provided
	if memoryLimit != "" {
		runtimeLimit, err := config.ParseMemoryLimitMB(memoryLimit)
		if err != nil {
			return newCommandError(cmd, localization.NornicDBCLIInvalidMemoryLimit(memoryLimit, err), err)
		}
		cfg.Memory.RuntimeLimitStr = memoryLimit
		cfg.Memory.RuntimeLimit = runtimeLimit
	}
	if gcPercent != 100 {
		cfg.Memory.GCPercent = gcPercent
	}
	cfg.Memory.PoolEnabled = poolEnabled
	cfg.Memory.QueryCacheEnabled = queryCacheSize > 0
	cfg.Memory.QueryCacheSize = queryCacheSize
	if ttl, err := time.ParseDuration(queryCacheTTL); err == nil {
		cfg.Memory.QueryCacheTTL = ttl
	}
	cfg.Memory.ApplyRuntimeMemory()

	// Configure object pooling
	pool.Configure(pool.PoolConfig{
		Enabled: cfg.Memory.PoolEnabled,
		MaxSize: cfg.Memory.PoolMaxSize,
	})

	// Configure query cache
	if cfg.Memory.QueryCacheEnabled {
		cache.ConfigureGlobalCache(cfg.Memory.QueryCacheSize, cfg.Memory.QueryCacheTTL)
	}

	commandPrintln(cmd, localization.NornicDBCLIStarting(buildinfo.DisplayVersion()))
	commandPrintln(cmd, localization.NornicDBCLIDataDirectory(dataDir))
	commandPrintln(cmd, localization.NornicDBCLIBoltEndpoint(boltPort))
	commandPrintln(cmd, localization.NornicDBCLIHTTPEndpoint(httpPort))
	if cfg.Memory.EmbeddingEnabled {
		commandPrintln(cmd, localization.NornicDBCLIEmbeddingsEnabled(embeddingProvider, embeddingModel, embeddingDim))
	} else {
		commandPrintln(cmd, localization.NornicDBCLIEmbeddingsDisabled())
	}
	if embeddingProvider == "local" {
		modelsDir := cfg.Memory.ModelsDir
		if modelsDir == "" {
			modelsDir = "/data/models"
		}
		gpuMode := "auto"
		if embeddingGPULayers == 0 {
			gpuMode = "CPU only"
		} else if embeddingGPULayers > 0 {
			gpuMode = fmt.Sprintf("%d layers", embeddingGPULayers)
		}
		commandPrintln(cmd, localization.NornicDBCLIEmbeddingLocal(modelsDir, embeddingModel, embeddingDim, gpuMode))
	} else {
		commandPrintln(cmd, localization.NornicDBCLIEmbeddingURL(embeddingURL))
		commandPrintln(cmd, localization.NornicDBCLIEmbeddingModel(embeddingModel, embeddingDim))
	}
	if parallelEnabled {
		workers := parallelWorkers
		if workers == 0 {
			workers = runtime.NumCPU()
		}
		commandPrintln(cmd, localization.NornicDBCLIParallelEnabled(workers, parallelBatchSize))
	} else {
		commandPrintln(cmd, localization.NornicDBCLIParallelDisabled())
	}
	// Memory management info
	if cfg.Memory.RuntimeLimit > 0 {
		commandPrintln(cmd, localization.NornicDBCLIMemoryLimit(config.FormatMemorySize(cfg.Memory.RuntimeLimit)))
	} else {
		commandPrintln(cmd, localization.NornicDBCLIMemoryUnlimited())
	}
	if cfg.Memory.GCPercent != 100 {
		commandPrintln(cmd, localization.NornicDBCLIGCPercent(cfg.Memory.GCPercent))
	}
	if cfg.Memory.PoolEnabled {
		commandPrintln(cmd, localization.NornicDBCLIObjectPooling())
	}
	if cfg.Memory.QueryCacheEnabled {
		commandPrintln(cmd, localization.NornicDBCLIQueryCache(cfg.Memory.QueryCacheSize, cfg.Memory.QueryCacheTTL))
	}
	cmd.Println()

	// Create data directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return newCommandError(cmd, localization.NornicDBCLICreateDataDirectoryFailed(err), err)
	}

	// Configure database. cfg is the canonical config snapshot built by
	// LoadFromEnv / LoadFromFile + the CLI override block above. Past
	// versions of this code copied a hand-picked subset of fields into
	// a fresh DefaultConfig() — that pattern silently dropped any new
	// field unless someone remembered to add a copy line (lab incident
	// 80719f25 hit exactly this with the Search* flags). Pass cfg
	// directly: any field that lives on Config now flows through.
	dbConfig := cfg
	dbConfig.Database.DataDir = dataDir
	dbConfig.Server.BoltPort = boltPort
	dbConfig.Server.HTTPPort = httpPort
	// Embedding endpoint/credentials are still computed locally from a
	// mix of CLI flags + env, then assigned here so the resolved values
	// land on the same struct.
	dbConfig.Memory.EmbeddingAPIURL = embeddingURL
	dbConfig.Memory.EmbeddingAPIKey = embeddingKey
	dbConfig.Memory.EmbeddingModel = embeddingModel
	dbConfig.Memory.EmbeddingDimensions = embeddingDim
	dbConfig.Database.AllowStorageUpgrade = upgradeStorage

	// Memory mode flag is CLI-only (no Config field today). Apply it
	// before Open so cache sizing is correct.
	lowMemory, _ := cmd.Flags().GetBool("low-memory")
	if lowMemory {
		applyLowMemoryMode(dbConfig)
	}

	// pkg/server installs the per-DB search-flags resolver after Open
	// returns. Tell Open to hold the warmup gate until then — server.New
	// releases it via db.MarkSearchWarmupReady once SetDbSearchFlagsResolver
	// has run. Without this, the warmup goroutine could race past resolver
	// installation and warm the default DB with global fallbacks instead of
	// the operator's per-DB overrides.
	dbConfig.DeferSearchWarmup = true

	// Phase 2 D-08 reordering: build the production *slog.Logger BEFORE
	// nornicdb.Open so storage (BadgerEngine, WAL, AsyncEngine) emits
	// through the structured handler stack from the very first line. The
	// logger flows into Open via dbConfig.Logger; storage ctors install
	// discard fallbacks if it is nil per D-01a.
	earlyLoggerInfo := observability.ServiceInfo{
		Name:    "nornicdb",
		Version: buildinfo.Version(),
		NodeID:  clusterNodeID,
	}
	earlyLogger, earlyWriterRef, earlyLogErr := observability.NewLogger(observability.LoggerConfig{
		Level:  cfg.Logging.Level,
		Format: cfg.Logging.Format,
		Output: cfg.Logging.Output,
	}, earlyLoggerInfo)
	if earlyLogErr != nil {
		fmt.Fprintln(os.Stderr, "WARN logger init: ", earlyLogErr)
	}
	localizer, err = localization.NewManager(processPreferences.Preferences, earlyLogger)
	if err != nil {
		return newCommandError(cmd, localization.NornicDBCLILocalizationInitFailed(err), err)
	}
	setCommandLocalizer(cmd, localizer)
	localization.LogProcessFallback(earlyLogger, processPreferences, localizer.Match(processPreferences.Preferences...))
	// Phase 2 D-01 storage threading: assign the slog logger into nornicdb.Config
	// so storage ctors (BadgerEngine, AsyncEngine, WAL) inherit it through
	// the field-literal threading inside nornicdb.Open (BadgerOptions{Logger: ...},
	// AsyncEngineConfig{Logger: ...}, WALConfig.SlogLogger).
	dbConfig.Logger = earlyLogger
	dbConfig.Localizer = localizer
	// earlyWriterRef is reused below by observability.New so the same
	// underlying io.Writer flushes during Provider.Shutdown (D-09a).

	// Open database
	commandPrintln(cmd, localization.NornicDBCLIOpeningDatabase())
	db, err := nornicdb.Open(dataDir, dbConfig)
	if err != nil {
		return newCommandError(cmd, localization.NornicDBCLIOpenDatabaseFailed(err), err)
	}
	defer db.Close()

	// Initialize GPU acceleration (Metal on macOS, auto-detect otherwise)
	commandPrintln(cmd, localization.NornicDBCLIInitializingGPU())
	gpuConfig := gpu.DefaultConfig()
	gpuConfig.Enabled = true
	gpuConfig.FallbackOnError = true

	// Set preferred backend from flag/env or platform default
	switch strings.ToLower(gpuBackend) {
	case "vulkan":
		gpuConfig.PreferredBackend = gpu.BackendVulkan
	case "cuda":
		gpuConfig.PreferredBackend = gpu.BackendCUDA
	case "metal":
		gpuConfig.PreferredBackend = gpu.BackendMetal
	case "opencl":
		gpuConfig.PreferredBackend = gpu.BackendOpenCL
	default:
		// Auto-detect: prefer Metal on macOS/Apple Silicon
		if runtime.GOOS == "darwin" {
			gpuConfig.PreferredBackend = gpu.BackendMetal
		}
	}

	gpuManager, gpuErr := gpu.NewManager(gpuConfig)
	if gpuErr != nil {
		commandPrintln(cmd, localization.NornicDBCLIGPUUnavailable(gpuErr))
	} else if gpuManager.IsEnabled() {
		device := gpuManager.Device()
		db.SetGPUManager(gpuManager)
		commandPrintln(cmd, localization.NornicDBCLIGPUEnabled(device.Name, string(device.Backend), device.MemoryMB))
	} else {
		// Check if GPU hardware is present but CUDA not compiled in
		device := gpuManager.Device()
		if device != nil && device.MemoryMB > 0 {
			commandPrintln(cmd, localization.NornicDBCLIGPUDetectedNoCUDA(device.Name, device.MemoryMB))
			commandPrintln(cmd, localization.NornicDBCLIBuildCUDA())
		} else {
			commandPrintln(cmd, localization.NornicDBCLIGPUDisabled())
		}
	}

	// Use system namespace storage directly for auth bootstrap.
	// Server.New() initializes the shared DatabaseManager once for runtime usage.
	storageEngine := db.GetBaseStorageForManager()
	systemStorage := storage.NewNamespacedEngine(storageEngine, "system")

	// Setup authentication
	var authenticator *auth.Authenticator
	authEnabled := serveAuthEnabled(cfg, noAuth)
	if authEnabled {
		commandPrintln(cmd, localization.NornicDBCLISettingUpAuth())
		authConfig := auth.DefaultAuthConfig()
		// Use JWT secret from config (auto-generated if not set)
		if cfg.Auth.JWTSecret != "" {
			authConfig.JWTSecret = []byte(cfg.Auth.JWTSecret)
			commandPrintln(cmd, localization.NornicDBCLIConfiguredJWT(len(cfg.Auth.JWTSecret)))
		} else {
			// Fallback to a generated secret (will change on restart!)
			authConfig.JWTSecret = []byte("nornicdb-dev-key" + fmt.Sprintf("%d", time.Now().UnixNano()))
			commandPrintln(cmd, localization.NornicDBCLINoJWT())
		}

		// Use configured default admin username (from env/config, default: "admin")
		if cfg.Auth.InitialUsername != "" {
			authConfig.DefaultAdminUsername = cfg.Auth.InitialUsername
		}

		var authErr error
		authenticator, authErr = auth.NewAuthenticator(authConfig, systemStorage)
		if authErr != nil {
			return newCommandError(cmd, localization.NornicDBCLICreateAuthenticatorFailed(authErr), authErr)
		}

		// Create admin user with configured credentials (fallback from auth defaults).
		adminUsername := authConfig.DefaultAdminUsername
		if adminUsername == "" {
			adminUsername = auth.DefaultAuthConfig().DefaultAdminUsername
		}
		adminPassword := cfg.Auth.InitialPassword
		if adminPassword == "" {
			adminPassword = adminPasswordFlag
		}
		// Preserve explicit CLI override when intentionally provided.
		if adminPasswordFlagChanged && adminPasswordFlag != "" {
			adminPassword = adminPasswordFlag
		}
		_, err := authenticator.CreateUser(adminUsername, adminPassword, []auth.Role{auth.RoleAdmin})
		if err != nil {
			// User might already exist
			commandPrintln(cmd, localization.NornicDBCLIAdminSetupFailed())
		} else {
			commandPrintln(cmd, localization.NornicDBCLIAdminCreated(adminUsername))
		}
	}
	// Note: Auth status logged at server startup

	// Phase 2 D-08: the production *slog.Logger was built BEFORE nornicdb.Open
	// above so storage, WAL, and AsyncEngine emit through the structured
	// handler stack from the very first line. The same logger + writerRef +
	// loggerInfo are reused for observability.New below — single source of
	// truth for the entire process lifetime.
	loggerInfo := earlyLoggerInfo
	logger := earlyLogger
	writerRef := earlyWriterRef

	// Phase 2 D-01 + D-04c: thread the structured logger and the slow-query
	// threshold from cfg.Logging into the primary cypher executor so the
	// LOG-07 slow-query record schema fires on the production code path.
	if exec := db.GetCypherExecutor(); exec != nil {
		exec.SetLogger(logger)
		exec.SetSlowQueryThreshold(cfg.Logging.SlowQueryThreshold)
	}

	// Create and start HTTP server
	serverConfig := server.DefaultConfig()
	serverConfig.ProcessConfig = cfg
	serverConfig.Port = httpPort
	serverConfig.BoltPort = boltPort
	serverConfig.Address = resolvedAddress
	// MCP server configuration
	serverConfig.MCPEnabled = mcpEnabled
	// Pass embedding settings to server (from loaded config)
	serverConfig.EmbeddingEnabled = cfg.Memory.EmbeddingEnabled
	serverConfig.EmbeddingProvider = embeddingProvider
	serverConfig.EmbeddingAPIURL = embeddingURL
	serverConfig.EmbeddingAPIKey = embeddingKey
	serverConfig.EmbeddingModel = embeddingModel
	serverConfig.EmbeddingDimensions = embeddingDim
	serverConfig.EmbeddingCacheSize = embeddingCache
	serverConfig.ModelsDir = cfg.Memory.ModelsDir
	serverConfig.EmbeddingCtxType = cfg.Memory.EmbeddingCtxType
	serverConfig.EmbeddingPoolingType = cfg.Memory.EmbeddingPoolingType
	serverConfig.EmbeddingAttentionType = cfg.Memory.EmbeddingAttentionType
	serverConfig.EmbeddingFlashAttn = cfg.Memory.EmbeddingFlashAttn
	serverConfig.Headless = headless
	serverConfig.BasePath = basePath
	serverConfig.Features = &cfg.Features // Pass features loaded from YAML config
	// Pass plugin directories from loaded config
	serverConfig.PluginsDir = cfg.Server.PluginsDir
	serverConfig.HeimdallPluginsDir = cfg.Server.HeimdallPluginsDir
	// CORS configuration from loaded config
	serverConfig.EnableCORS = cfg.Server.EnableCORS
	serverConfig.CORSOrigins = cfg.Server.CORSOrigins
	// Phase 2 D-01: thread the observability *slog.Logger into the server
	// Config so all pkg/server log emission flows through the production
	// 4-layer handler stack (recovering → mandatory → redactor → JSON).
	// Uses the `logger` built above (same value Provider.Logger() returns
	// later, since obs.New stores this reference). nil-safe: pkg/server.New
	// installs a discard fallback if Logger is nil.
	serverConfig.Logger = logger
	serverConfig.Localizer = localizer

	// Phase 2 D-04d: thread the canonical pkg/config.LoggingConfig snapshot
	// into the server. SlowQueryThreshold + SlowQueryLogFile readers in
	// pkg/server now go through s.config.Logging.* exclusively.
	serverConfig.Logging = cfg.Logging

	// Enable embedded UI from the ui package (unless headless mode)
	if !headless {
		server.SetUIAssets(ui.Assets)
	}

	// Pass the yaml-declared `databases:` map through Config so server.New
	// seeds dbconfig.Store during construction. Threading via Config (rather
	// than a post-construction setter) is load-bearing: New() opens the
	// system database and runs LoadWithYAMLDefaults before returning, so the
	// overrides MUST be visible to the constructor.
	if cfg != nil && len(cfg.PerDBOverrides) > 0 {
		serverConfig.PerDBYAMLOverrides = cfg.PerDBOverrides
	}

	httpServer, err := server.New(db, authenticator, serverConfig)
	if err != nil {
		return newCommandError(cmd, localization.NornicDBCLICreateServerFailed(err), err)
	}

	// HTTP server START is deferred until AFTER observability.New runs
	// below — Plan 04-02 D-02c init-order chokepoint. The Plan 04-02
	// instrumentedMux wrapper consults s.httpMetrics at Handler-mount
	// time inside Start(); we must therefore inject the bag (via
	// SetHTTPMetrics) AFTER obs.Registry() exists. The two-phase
	// bootstrap (Phase 2 D-08) keeps server.New BEFORE obs so the same
	// *slog.Logger reference threads through both — only Start() moves.

	// Create and start Bolt server for Neo4j driver compatibility.
	// Plan 02-05 D-01: Logger flows in via Config.Logger so Bolt records
	// share the assembled handler stack (recovering -> mandatory -> redact
	// -> json) and Bolt HELLO "credentials" are auto-redacted (D-03a).
	boltConfig := bolt.DefaultConfig()
	boltConfig.Host = resolvedAddress
	boltConfig.Port = boltPort
	boltConfig.LogQueries = logQueries
	boltConfig.ServerAnnouncement = cfg.Server.BoltServerAnnouncement
	// `logger` is the same *slog.Logger reference Provider.Logger() returns
	// (built BEFORE observability.New per D-08's two-phase bootstrap), so
	// records emitted by the Bolt server flow through the production
	// 4-layer recovering -> mandatory -> redact -> json handler stack.
	boltConfig.Logger = logger
	boltConfig.Localizer = localizer
	if !noAuth && authenticator != nil {
		boltAuth := bolt.NewAuthenticatorAdapter(authenticator)
		boltAuth.SetGetEffectivePermissions(httpServer.GetEffectivePermissions)
		boltConfig.Authenticator = boltAuth
		boltConfig.RequireAuth = true
	}

	// Bolt-over-WebSocket + TLS plumbing. Each setting flows through
	// the precedence ladder (CLI > env > YAML > defaults) via cfg.Server,
	// then lands here as plain Go values. Cert+key paths construct a
	// *tls.Config with cert rotation; mTLS is opt-in via ClientCAFile.
	if cfg.Server.BoltTLSEnabled && cfg.Server.BoltTLSCert != "" && cfg.Server.BoltTLSKey != "" {
		mode, err := bolt.ParseClientAuthMode(cfg.Server.BoltTLSClientAuthMode)
		if err != nil {
			return newCommandError(cmd, localization.NornicDBCLIBoltTLSClientAuthFailed(err), err)
		}
		var tlsCfg *tls.Config
		if cfg.Server.BoltTLSClientCAFile != "" {
			tlsCfg, err = bolt.LoadTLSConfigWithClientCA(
				cfg.Server.BoltTLSCert,
				cfg.Server.BoltTLSKey,
				cfg.Server.BoltTLSClientCAFile,
				mode,
			)
		} else {
			tlsCfg, err = bolt.LoadTLSConfig(cfg.Server.BoltTLSCert, cfg.Server.BoltTLSKey)
		}
		if err != nil {
			return newCommandError(cmd, localization.NornicDBCLIBoltTLSLoadFailed(err), err)
		}
		boltConfig.TLSConfig = tlsCfg
	}
	boltConfig.RequireTLS = cfg.Server.BoltTLSRequire
	if cfg.Server.BoltSniffTimeout > 0 {
		boltConfig.BoltSniffTimeout = cfg.Server.BoltSniffTimeout
	}
	if cfg.Server.BoltAuthTimeout > 0 {
		boltConfig.BoltAuthTimeout = cfg.Server.BoltAuthTimeout
	}
	if cfg.Server.BoltStatementTimeout > 0 {
		boltConfig.BoltStatementTimeout = cfg.Server.BoltStatementTimeout
	}
	boltConfig.WebSocketEnabled = cfg.Server.BoltWebSocketEnabled
	if cfg.Server.BoltWebSocketAllowedOrigins != "" {
		boltConfig.WebSocketAllowedOrigins = cfg.Server.BoltWebSocketAllowedOrigins
	}
	if cfg.Server.BoltWebSocketMaxMessageSize > 0 {
		boltConfig.WebSocketMaxMessageSize = cfg.Server.BoltWebSocketMaxMessageSize
	}
	if cfg.Server.BoltWebSocketWriteBufferSize > 0 {
		boltConfig.WebSocketWriteBufferSize = cfg.Server.BoltWebSocketWriteBufferSize
	}
	if cfg.Server.BoltWebSocketPingInterval > 0 {
		boltConfig.WebSocketPingInterval = cfg.Server.BoltWebSocketPingInterval
	}
	if cfg.Server.BoltWebSocketPongTimeout > 0 {
		boltConfig.WebSocketPongTimeout = cfg.Server.BoltWebSocketPongTimeout
	}
	// Discovery body is derived from auth.OAuthConfig at startup. The
	// Bolt server pre-encodes the response and refreshes the Date header
	// once per second.
	boltConfig.OAuthConfig = auth.GetOAuthConfig()

	// Create query executor adapter (fallback for single-DB mode) and bind Bolt
	// to the same database manager used by HTTP so multi-db/composite/fabric
	// semantics are identical across protocols.
	queryExecutor := NewDBQueryExecutor(db)
	queryExecutor.localizer = localizer
	boltServer := bolt.NewWithDatabaseManager(boltConfig, queryExecutor, httpServer.GetDatabaseManager())
	connectionTracker := multidb.NewConnectionTracker()
	boltServer.SetDatabaseConnectionAdmission(httpServer.GetDatabaseManager().ResolveConnectionLimitOwner, func(databaseName string) error {
		return connectionTracker.TryIncrementConnection(httpServer.GetDatabaseManager(), databaseName)
	}, connectionTracker.DecrementConnection)
	// Wire per-database access from HTTP server so Bolt enforces same policy (allowlist + principal roles)
	boltServer.SetDatabaseAccessModeResolver(httpServer.GetDatabaseAccessModeForRoles)
	// Wire per-DB read/write for mutation checks (Phase 4)
	boltServer.SetResolvedAccessResolver(httpServer.GetResolvedAccessForRoles)

	// Phase 1 OBS-07/OBS-08: lifecycle.Run is the canonical supervisor.
	// The previous channel-based pattern (signal.Notify + sequential
	// shutdown with workers stopped FIRST) is replaced because it
	// violated OBS-08's mandated drain order. lifecycle.Run encodes the
	// reverse-order drain as a function of the registration order below.
	//
	// 1. Logger, loggerInfo, and writerRef were built earlier (BEFORE
	//    server.New) per Phase 2 D-08's two-phase bootstrap so the same
	//    *slog.Logger flows into pkg/server's Config.Logger and into
	//    observability.New below. The 4-layer handler stack
	//    (recovering → mandatory → redactor → JSONHandler) was assembled
	//    against cfg.Logging.Level/Output + ServiceInfo at construction.

	// 2. Build the observability provider (OTel SDK + Prometheus
	//    registry; OBS-11 noop fallback if OTLP exporter init fails —
	//    NEVER fatal). The logger + writerRef thread through so
	//    Provider.Logger() exposes the assembled *slog.Logger to
	//    downstream business-package constructors per D-01.
	obs, obsErr := observability.New(cmd.Context(), cfg.Observability, loggerInfo, logger, writerRef)
	if obsErr != nil {
		return newCommandError(cmd, localization.NornicDBCLIObservabilityInitFailed(obsErr), obsErr)
	}
	log.Printf("INFO observability: instance_id=%s (source=%s)", obs.InstanceID(), obs.InstanceIDSource())

	// Phase 5 D-02d: resolve the tenant-labels-enabled bool BEFORE any Phase
	// 4 bag constructor below reads cfg.Observability.Metrics.TenantLabelsEnabled.
	// Precedence (D-02a): explicit YAML (TenantLabelsExplicit *bool, R-02) >
	// K8s autodetect (KUBERNETES_SERVICE_HOST + non-empty SA-token) > default
	// false. The helper logs the outcome via the injected slog logger
	// (Phase 2 D-08 plumbing — LOG-09 compliant, no slog.Default).
	cfg.Observability.Metrics.TenantLabelsEnabled = observability.ResolveAndLogTenantLabelsLocalized(
		cfg.Observability.Metrics.TenantLabelsExplicit, localizer, logger,
	)

	// Phase 5 / Plan 05-04: plumb the unified prometheus registry to the
	// legacy :7474/metrics adapter so handleMetrics can call
	// observability.RenderLegacy. Mirrors the Phase 4 SetHTTPMetrics
	// injection pattern (post-construction setter; nil-safe handler).
	// MUST happen BEFORE httpServer.Start() — the legacy /metrics handler
	// reads s.obsRegistry on every scrape via the RLock-protected
	// obsRegistryForHandler accessor; injecting before Start eliminates
	// the (otherwise possible) nil-body race window for early scrapes.
	httpServer.SetObsRegistry(obs.Registry())

	// 2a. Construct the Cache + Runtime metric bag (Plan 04-01 / MET-16).
	//     Inserted between the registry build (inside observability.New) and
	//     the telemetry listener — Phase 4 D-02c init-order chokepoint.
	//     Registers six families (cache_hits_total, cache_misses_total,
	//     cache_size_bytes, cache_evictions_total, process_uptime_seconds,
	//     build_info) on obs.Registry(). MUST NOT crash startup if go/process
	//     collectors are already there (Phase 1 registry.go:34-35) — the cache
	//     bag adds disjoint families so AlreadyRegisteredError cannot occur.
	cacheMetrics := observability.NewCacheMetrics(obs.Registry())

	// 2b. Plan 04-02: construct HTTP + Bolt metric bags and inject into the
	//     existing httpServer / boltServer instances via setters. Both bags
	//     register against obs.Registry() — disjoint families per the
	//     Phase-3 typed constructors so AlreadyRegisteredError cannot
	//     occur with cacheMetrics or the Phase-1 go/process collectors.
	//     The D-08 tenantLabelsEnabled bool is taken from the loaded
	//     observability config; Phase 5's K8s autodetect will set it
	//     automatically once it lands.
	httpMetrics := observability.NewHTTPMetrics(obs.Registry(), cfg.Observability.Metrics.TenantLabelsEnabled)
	httpServer.SetHTTPMetrics(httpMetrics)

	boltMetrics := observability.NewBoltMetrics(obs.Registry())
	boltServer.SetBoltMetrics(boltMetrics)
	// Plan 04-06 forward-compat: AuthMetrics bag ships in 04-06; until
	// then the Bolt HELLO completion site no-ops on its nil-check.
	// boltServer.SetAuthMetrics(authMetrics) // wired by Plan 04-06.

	// 2b'. Plan 04-03: construct CypherMetrics (MET-08) — 11 families
	//      including planner_cache_*, transaction_conflicts_total, and the
	//      D-15b/MET-26 slow_query_threshold_seconds GaugeFunc that reads
	//      cfg.Logging.SlowQueryThreshold().Seconds() on every scrape so
	//      config reload is reflected without event wiring. The Cypher
	//      bag is injected into the primary executor via SetCypherMetrics
	//      (which also propagates into the executor's owned planCache for
	//      D-12a planner-cache observation), and the cross-cutting Cache
	//      bag is injected via SetCacheMetrics (which routes into the
	//      owned SmartQueryCache for cache_hits_total{cache="query_result"}
	//      bridge per D-12a).
	cypherMetrics := observability.NewCypherMetrics(
		obs.Registry(),
		cfg.Observability.Metrics.TenantLabelsEnabled,
		func() float64 { return cfg.Logging.SlowQueryThreshold.Seconds() },
	)
	if exec := db.GetCypherExecutor(); exec != nil {
		exec.SetCypherMetrics(cypherMetrics, defaultBoltDatabaseName(db))
		exec.SetCacheMetrics(cacheMetrics)
	}

	// 2b''. Knowledge-policy metrics (decay / promotion / suppression /
	//       on-access mutations / access-flush batches). See
	//       docs/plans/knowledge-policy-observability-plan.md. Published
	//       via the package-level ref so the storage read-path filter
	//       (deep-nested; threading a handle through every iterator
	//       signature would widen the Engine interface) picks it up.
	//       Construction happens AFTER nornicdb.Open so any startup fire
	//       from within Open (e.g. the startup Reconcile counter) is lost
	//       — acceptable; the handle is picked up lazily for all
	//       subsequent fires via observability.GetKnowledgePolicyMetrics.
	kpMetrics := observability.NewKnowledgePolicyMetrics(
		obs.Registry(),
		cfg.Observability.Metrics.TenantLabelsEnabled,
		func() float64 {
			if af := db.GetAccessFlusher(); af != nil {
				return af.BufferFullness()
			}
			return 0
		},
	)
	observability.SetKnowledgePolicyMetrics(kpMetrics)

	// 2b''. Plan 04-04: construct StorageMetrics (MET-10) + MVCCMetrics
	//       (MET-11). The bags register against obs.Registry() — disjoint
	//       families, no AlreadyRegisteredError risk. Inject into the
	//       underlying *BadgerEngine via AttachMetrics, which pre-binds
	//       the four op-duration observers (MET-25). The bytes_metrics
	//       sweeper is registered as a lifecycle.Component below between
	//       pprof and workers per RESEARCH §Q4 ordering — it drains AFTER
	//       workers and BEFORE the telemetry listener so the final scrape
	//       during drain reflects last-known sizes.
	var bytesSweeper *storage.BytesMetricsSweeper
	if badgerEngine := unwrapBadgerEngine(db.GetStorage()); badgerEngine != nil {
		storageMetrics := observability.NewStorageMetrics(
			obs.Registry(),
			cfg.Observability.Metrics.TenantLabelsEnabled,
			badgerStorageProbe{be: badgerEngine},
		)
		mvccMetrics := observability.NewMVCCMetrics(
			obs.Registry(),
			cfg.Observability.Metrics.TenantLabelsEnabled,
			badgerMVCCProbe{be: badgerEngine},
		)
		badgerEngine.AttachMetrics(storageMetrics, mvccMetrics)

		// D-07 sweep cadence: 30s default; Plan 04-04 introduces
		// cfg.Storage.BytesMetricInterval. Override path is exercised by
		// the BytesMetricsSweeper interval-override unit test.
		interval := cfg.Storage.BytesMetricInterval
		if interval <= 0 {
			interval = storage.DefaultBytesMetricsInterval
		}
		bytesSweeper = storage.NewBytesMetricsSweeper(
			storageMetrics,
			badgerEngine.DB(),
			nil, /* search size callback — Plan 04-05 wires */
			interval,
		)
	}

	// 2b'''. Plan 04-05: construct EmbedMetrics (MET-12 — 6 families +
	//        ffi_panics_total) and SearchMetrics (MET-13 — 4 families).
	//        Same Phase-4 D-02c init-order chokepoint Plans 04-01..04-04
	//        used. EmbedMetrics is NOT tenant-tagged per CONTEXT MET-21
	//        omission. SearchMetrics IS tenant-tagged (D-08); the bool
	//        threads through cfg.Observability.Metrics.TenantLabelsEnabled.
	embedMetrics := observability.NewEmbedMetrics(
		obs.Registry(),
		embedQueueProbe{q: db.GetEmbedQueue()},
	)
	// D-09: attach the FFI panic counter to the underlying
	// LocalGGUFEmbedder if one exists; no-op otherwise. The cached
	// embedder wrapper layer is not yet traversed (deferred — future
	// enhancement adds CachedEmbedder.Base()).
	if exec := db.GetCypherExecutor(); exec != nil {
		_ = attachEmbedMetricsToEmbedder(exec.GetEmbedder(), embedMetrics)
	}

	searchMetrics := observability.NewSearchMetrics(
		obs.Registry(),
		cfg.Observability.Metrics.TenantLabelsEnabled,
		searchServiceProbe{svc: nil}, /* probe lazy-bound below per database */
	)

	// Best-effort attach search metrics + per-database search-service
	// probe. The default-DB search service is created lazily via
	// GetOrCreateSearchService — call it here so the metric bag has a
	// real target for IndexSizeBytes during the first scrape.
	defaultDBNameForMetrics := defaultBoltDatabaseName(db)
	if defaultSearchSvc, sErr := db.GetOrCreateSearchService(defaultDBNameForMetrics, db.GetStorage()); sErr == nil && defaultSearchSvc != nil {
		defaultSearchSvc.AttachMetrics(searchMetrics)
		// Re-construct the SearchMetrics bag with a real probe pointing
		// at the default search service. We cannot do this earlier
		// because the bag was registered against obs.Registry already;
		// instead, rely on the fact that AttachMetrics on the service
		// flows future observations through searchMetrics, while the
		// existing GaugeFunc collector references the original probe
		// (zero-byte today). Wiring multi-database probes is deferred —
		// SearchProbe today is single-database.
		_ = defaultSearchSvc
	}

	// Wire the search-service size callback into the bytes_metrics_sweeper
	// so nornicdb_storage_bytes{kind="search"} reflects HNSW size. The
	// sweeper was constructed earlier with a nil callback; we cannot
	// retro-fit a sweeper field without exposing it. For M1 the storage
	// bytes{kind=search} stays at 0 — Phase 4 ships the
	// nornicdb_search_index_size_bytes_live collector as the canonical
	// search-bytes surface; the storage-side kind=search bucket is a
	// duplicate that future plans can wire if needed.
	_ = bytesSweeper

	_ = embedMetrics
	_ = searchMetrics

	// 2b''''. Plan 04-06: construct ReplicationMetrics + AuthMetrics bags
	//        and wire them into the Bolt server (Plan-04-02 HELLO call
	//        site already added; this lights it up), the Authenticator
	//        (HTTP/gRPC adapter chokepoints), and the Replicator (when
	//        replication is enabled — Standalone/HAStandby/Raft/MultiRegion
	//        all satisfy the MetricsAware optional interface).
	//
	//        The peer_metrics_gc lifecycle.Component is registered between
	//        the bytes_metrics_sweeper and workersC per RESEARCH §Q4
	//        ordering — drains AFTER workers and BEFORE telemetry so the
	//        final scrape during drain reflects the last GC pass.
	replicationMode := os.Getenv("NORNICDB_CLUSTER_MODE")
	replicatorIface := db.GetReplicator()
	var replicatorAny any
	if replicatorIface != nil {
		replicatorAny = replicatorIface
	}
	replAuthWiring := initReplicationAuthMetrics(
		obs.Registry(),
		replicationMode,
		cfg.Observability.Metrics.TenantLabelsEnabled,
		authenticator,
		boltServer,
		replicatorAny,
	)
	_ = replAuthWiring.authMetrics
	_ = replAuthWiring.replMetrics

	// 2c. NOW that the HTTP metrics bag is injected, start the HTTP
	//     server. The instrumentedMux wrapper picks up s.httpMetrics at
	//     Handler-mount time inside Start() (Plan 04-02 D-03 chokepoint).
	if err := httpServer.Start(); err != nil {
		return newCommandError(cmd, localization.NornicDBCLIStartServerFailed(err), err)
	}

	// 2. Build the health registry.
	health := observability.NewHealth()

	// 3. Build the telemetry listener (Component) — opens :9090 in
	//    constructor so EADDRINUSE surfaces synchronously.
	telemetry, telemetryErr := observability.NewTelemetryListener(obs, health)
	if telemetryErr != nil {
		return newCommandError(cmd, localization.NornicDBCLITelemetryListenerFailed(telemetryErr), telemetryErr)
	}

	// 4. Build the optional pprof listener (Component, may be nil when
	//    cfg.Observability.Pprof.Enabled is false — OBS-06 / Phase-success-2).
	pprof, pprofErr := observability.NewPprofListener(cfg.Observability.Pprof)
	if pprofErr != nil {
		return newCommandError(cmd, localization.NornicDBCLIPprofListenerFailed(pprofErr), pprofErr)
	}

	// 5. Register health checks AFTER storage/search are open (D-03c).
	//    db.HealthCheck is the W-4 fix Pitfall 11 mitigation: it probes
	//    db.storage.NodeCount() so a closed engine returns
	//    storage.ErrStorageClosed and /readyz flips to 503.
	health.Register("storage", db.HealthCheck)

	// search_warm is informational (Required: false) — operators see it
	// in /readyz JSON during index rebuild but it does NOT gate readiness.
	// We probe lazily on every check (rather than capturing a service at
	// registration time) because GetOrCreateSearchService may not yet
	// have an instance for the default database when /readyz is first
	// scraped during cold start.
	defaultDBName := defaultBoltDatabaseName(db)
	health.Register("search_warm", func(ctx context.Context) error {
		searchSvc, sErr := db.GetOrCreateSearchService(defaultDBName, db.GetStorage())
		if sErr != nil || searchSvc == nil {
			return errors.New("search service not yet available")
		}
		if !searchSvc.IsReady() {
			return errors.New("search indexes not yet warm")
		}
		return nil
	}, observability.CheckOpts{Required: false})

	// 6. Build adapter components for HTTP, Bolt, and embed-workers.
	httpC := &httpAdapter{srv: httpServer}
	boltC := &boltAdapter{srv: boltServer, localizer: localizer}
	workersC := &workersAdapter{db: db}

	// 7. D-04a registration order:
	//    Forward = startup; reverse = drain (OBS-08 encoded directly).
	//    telemetry FIRST (drains LAST so kubelet keeps scraping during drain)
	//    → optional pprof
	//    → workers
	//    → bolt
	//    → http LAST (drains FIRST — stop accepting new requests immediately).
	components := []lifecycle.Component{telemetry}
	if pprof != nil {
		components = append(components, pprof)
	}
	// Plan 04-04: bytes_metrics_sweeper drains AFTER workers and BEFORE
	// the telemetry listener (RESEARCH §Q4 ordering). It sits between
	// pprof and workers in registration order — that places it earlier
	// in the drain path than workers (drain runs in reverse), guaranteeing
	// the final /metrics scrape during drain still reflects the last
	// sweep's gauge values before the engine starts shutting down.
	if bytesSweeper != nil {
		components = append(components, bytesSweeper)
	}
	// Plan 04-06: peer_metrics_gc drains AFTER workers and BEFORE telemetry
	// so the final scrape during drain reflects the last GC sweep. Registered
	// unconditionally — when replication is disabled the GC runs idle waiting
	// for ctx cancel (D-05b nil-metrics tolerated path).
	if replAuthWiring.peerGC != nil {
		components = append(components, replAuthWiring.peerGC)
	}
	components = append(components, workersC, boltC, httpC)

	cmd.Println()
	commandPrintln(cmd, localization.NornicDBCLIReady())
	cmd.Println()
	// Determine the display address for user-friendly output
	displayAddr := resolvedAddress
	if resolvedAddress == "0.0.0.0" || resolvedAddress == "::" {
		displayAddr = "localhost" // 0.0.0.0 is all interfaces, show localhost for convenience
	}
	mcpEndpoint := ""
	if mcpEnabled {
		mcpEndpoint = fmt.Sprintf("http://%s:%d/mcp", displayAddr, httpPort)
	}
	pprofEndpoint := ""
	if pprof != nil {
		pprofEndpoint = fmt.Sprintf("http://%s/debug/pprof/", cfg.Observability.Pprof.Listen)
	}
	metricsEndpoint := fmt.Sprintf("http://%s%s/metrics", displayAddr, cfg.Observability.Metrics.Listen)
	commandPrintln(cmd, localization.NornicDBCLIEndpoints(displayAddr, httpPort, boltPort, metricsEndpoint, pprofEndpoint, mcpEndpoint, pprof != nil))
	cmd.Println()
	if authEnabled {
		adminUsername := cfg.Auth.InitialUsername
		if adminUsername == "" {
			adminUsername = auth.DefaultAuthConfig().DefaultAdminUsername
		}
		adminPassword := cfg.Auth.InitialPassword
		if adminPassword == "" {
			adminPassword = adminPasswordFlag
		}
		if adminPasswordFlagChanged && adminPasswordFlag != "" {
			adminPassword = adminPasswordFlag
		}
		commandPrintln(cmd, localization.NornicDBCLIAuthentication(adminUsername))
	}
	cmd.Println()
	commandPrintln(cmd, localization.NornicDBCLIPressToStop())
	cmd.Println()

	// 8. Run the supervisor. lifecycle.Run installs a SIGINT/SIGTERM
	//    NotifyContext, runs every Component.Start in an errgroup, and
	//    on cancellation drains in REVERSE order on a fresh
	//    context.WithTimeout(context.Background(), 30s) (OBS-09).
	if runErr := lifecycle.Run(cmd.Context(), components...); runErr != nil {
		return newCommandError(cmd, localization.NornicDBCLISupervisedRunFailed(runErr), runErr)
	}

	commandPrintln(cmd, localization.NornicDBCLIServerStopped())
	return nil
}

func resolveBindAddress(cmd *cobra.Command, cfg *config.Config, cliAddress string, loadedConfigFile bool) string {
	resolvedAddress := strings.TrimSpace(cliAddress)
	if cmd != nil && !cmd.Flags().Changed("address") && cfg != nil {
		if loadedConfigFile && cfg.Server.HTTPAddress != "" {
			resolvedAddress = cfg.Server.HTTPAddress
		} else if loadedConfigFile && cfg.Server.BoltAddress != "" {
			resolvedAddress = cfg.Server.BoltAddress
		} else if hasExplicitProtocolBindEnv() {
			if cfg.Server.HTTPAddress != "" {
				resolvedAddress = cfg.Server.HTTPAddress
			} else if cfg.Server.BoltAddress != "" {
				resolvedAddress = cfg.Server.BoltAddress
			}
		}
	}
	if strings.TrimSpace(resolvedAddress) == "" {
		return "127.0.0.1"
	}
	return strings.TrimSpace(resolvedAddress)
}

func hasExplicitProtocolBindEnv() bool {
	for _, envName := range []string{
		"NORNICDB_BOLT_ADDRESS",
		"NORNICDB_HTTP_ADDRESS",
		"NEO4J_dbms_connector_bolt_listen__address",
		"NEO4J_dbms_connector_http_listen__address",
	} {
		if strings.TrimSpace(os.Getenv(envName)) != "" {
			return true
		}
	}
	return false
}

func startStdioLogCompactor(maxKB int, interval time.Duration) func() {
	if maxKB <= 0 {
		return func() {}
	}
	if interval <= 0 {
		interval = time.Hour
	}
	maxBytes := int64(maxKB) * 1024

	compact := func() {
		compactStreamIfOversized("stdout", os.Stdout, maxBytes)
		compactStreamIfOversized("stderr", os.Stderr, maxBytes)
	}

	// Automatic startup pass handles existing oversized log files.
	compact()

	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				compact()
			case <-stopCh:
				return
			}
		}
	}()

	return func() { close(stopCh) }
}

func compactStreamIfOversized(name string, stream *os.File, maxBytes int64) {
	if stream == nil {
		return
	}
	info, err := stream.Stat()
	if err != nil {
		return
	}
	// Only manage file-backed streams (for example launchd StandardOutPath/StandardErrorPath).
	if !info.Mode().IsRegular() || info.Size() <= maxBytes {
		return
	}

	prevSize := info.Size()
	if err := stream.Truncate(0); err != nil {
		log.Printf("⚠️ stdio log compaction failed for %s: truncate: %v", name, err)
		return
	}
	// Keep writes in the truncated file instead of previous offset.
	if _, err := stream.Seek(0, io.SeekStart); err != nil {
		log.Printf("⚠️ stdio log compaction for %s truncated but seek failed: %v", name, err)
		return
	}
	log.Printf("🧹 compacted %s stream log from %d bytes to 0 bytes (limit=%d bytes)", name, prevSize, maxBytes)
}

// DBQueryExecutor adapts nornicdb.DB to bolt.QueryExecutor interface.
type DBQueryExecutor struct {
	db        *nornicdb.DB
	localizer *localization.Manager

	txMu     sync.Mutex
	txID     string
	txDBName string
	txMgr    *txsession.Manager
}

func NewDBQueryExecutor(db *nornicdb.DB) *DBQueryExecutor {
	txDBName := defaultBoltDatabaseName(db)
	exec := &DBQueryExecutor{
		db:       db,
		txDBName: txDBName,
	}
	exec.txMgr = txsession.NewManager(30*time.Second, func(dbName string) (*cypher.StorageExecutor, error) {
		return newTxScopedExecutor(db, dbName)
	})
	return exec
}

func defaultBoltDatabaseName(db *nornicdb.DB) string {
	if db == nil {
		return "nornic"
	}
	if namespaced, ok := db.GetStorage().(interface{ Namespace() string }); ok {
		if ns := strings.TrimSpace(namespaced.Namespace()); ns != "" {
			return ns
		}
	}
	return "nornic"
}

func newTxScopedExecutor(db *nornicdb.DB, dbName string) (*cypher.StorageExecutor, error) {
	if db == nil {
		cause := errors.New("database is not initialized")
		return nil, newCLIError(nil, localization.NornicDBCLIDatabaseNotInitialized(), cause)
	}

	storageEngine := db.GetStorage()
	executor := cypher.NewStorageExecutor(storageEngine)

	// Keep query embedding and search behavior consistent with the base DB executor.
	if baseExec := db.GetCypherExecutor(); baseExec != nil {
		if embedder := baseExec.GetEmbedder(); embedder != nil {
			executor.SetEmbedder(embedder)
		}
		if inferMgr := baseExec.GetInferenceManager(); inferMgr != nil {
			executor.SetInferenceManager(inferMgr)
		}
		// Phase 2 D-01 + D-04c: inherit logger and slow-query threshold from
		// the base executor so per-transaction scoped executors emit the
		// LOG-07 slow-query record on the same logger pipeline.
		executor.SetLogger(baseExec.Logger())
		executor.SetSlowQueryThreshold(baseExec.SlowQueryThreshold())
	}
	if searchSvc, err := db.GetOrCreateSearchService(dbName, storageEngine); err == nil {
		executor.SetSearchService(searchSvc)
	}

	if q := db.GetEmbedQueue(); q != nil {
		executor.SetNodeMutatedCallback(func(nodeID string) {
			q.Enqueue(nodeID)
		})
	}

	return executor, nil
}

func (e *DBQueryExecutor) NewSessionExecutor() bolt.QueryExecutor {
	executor := NewDBQueryExecutor(e.db)
	executor.localizer = e.localizer
	return executor
}

// BaseCypherExecutor exposes the base executor used by DBQueryExecutor so
// protocol adapters (for example Bolt database-scoped executors) can inherit
// shared runtime services such as embedder/search configuration.
func (e *DBQueryExecutor) BaseCypherExecutor() *cypher.StorageExecutor {
	if e == nil || e.db == nil {
		return nil
	}
	return e.db.GetCypherExecutor()
}

// ConfigureDatabaseExecutor applies production runtime wiring to a DB-scoped
// executor created by protocol adapters (for example Bolt multi-database
// sessions), keeping behavior aligned with HTTP/GraphQL execution paths.
func (e *DBQueryExecutor) ConfigureDatabaseExecutor(exec *cypher.StorageExecutor, dbName string, storageEngine storage.Engine) {
	if e == nil || e.db == nil || exec == nil {
		return
	}
	if baseExec := e.db.GetCypherExecutor(); baseExec != nil {
		if emb := baseExec.GetEmbedder(); emb != nil {
			exec.SetEmbedder(emb)
		}
		if inferMgr := baseExec.GetInferenceManager(); inferMgr != nil {
			exec.SetInferenceManager(inferMgr)
		}
	}
	if searchSvc, err := e.db.GetOrCreateSearchService(dbName, storageEngine); err == nil {
		exec.SetSearchService(searchSvc)
	}
	if q := e.db.GetEmbedQueue(); q != nil {
		exec.SetNodeMutatedCallback(func(nodeID string) {
			q.Enqueue(nodeID)
		})
	}
}

// Execute runs a Cypher query against the database.
func (e *DBQueryExecutor) Execute(ctx context.Context, query string, params map[string]any) (*bolt.QueryResult, error) {
	e.txMu.Lock()
	txID := e.txID
	e.txMu.Unlock()

	if txID != "" {
		txSession, ok := e.txMgr.Get(txID)
		if !ok || txSession == nil {
			cause := errors.New("transaction not found")
			return nil, newCLIError(e.localizer, localization.NornicDBCLITransactionNotFound(), cause)
		}
		result, err := e.txMgr.ExecuteInSession(ctx, txSession, query, params)
		if err != nil {
			return nil, err
		}
		return &bolt.QueryResult{
			Columns:  result.Columns,
			Rows:     result.Rows,
			Metadata: result.Metadata,
		}, nil
	}

	result, err := e.db.ExecuteCypher(ctx, query, params)
	if err != nil {
		return nil, err
	}

	return &bolt.QueryResult{
		Columns:  result.Columns,
		Rows:     result.Rows,
		Metadata: result.Metadata,
	}, nil
}

func (e *DBQueryExecutor) BeginTransaction(ctx context.Context, metadata map[string]any) error {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	if e.txID != "" {
		cause := errors.New("transaction already active")
		return newCLIError(e.localizer, localization.NornicDBCLITransactionAlreadyActive(), cause)
	}

	session, err := e.txMgr.Open(ctx, e.txDBName)
	if err != nil {
		return err
	}
	e.txID = session.ID
	return nil
}

func (e *DBQueryExecutor) CommitTransaction(ctx context.Context) error {
	e.txMu.Lock()
	txID := e.txID
	e.txID = ""
	e.txMu.Unlock()
	if txID == "" {
		return nil
	}

	session, ok := e.txMgr.Get(txID)
	if !ok || session == nil {
		cause := errors.New("transaction not found")
		return newCLIError(e.localizer, localization.NornicDBCLITransactionNotFound(), cause)
	}

	_, err := e.txMgr.CommitAndDelete(ctx, session)
	return err
}

func (e *DBQueryExecutor) RollbackTransaction(ctx context.Context) error {
	e.txMu.Lock()
	txID := e.txID
	e.txID = ""
	e.txMu.Unlock()
	if txID == "" {
		return nil
	}

	session, ok := e.txMgr.Get(txID)
	if !ok || session == nil {
		return nil
	}
	return e.txMgr.RollbackAndDelete(ctx, session)
}

func runInit(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data-dir")

	commandPrintln(cmd, localization.NornicDBCLIInitProgress(dataDir))

	// Create directories
	dirs := []string{
		dataDir,
		filepath.Join(dataDir, "graph"),
		filepath.Join(dataDir, "indexes"),
		filepath.Join(dataDir, "embeddings"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return newCommandError(cmd, localization.NornicDBCLICreateDirectoryFailed(dir, err), err)
		}
	}

	// Create default config file
	configPath := filepath.Join(dataDir, "nornicdb.yaml")
	configContent := `# NornicDB Configuration
data_dir: ./data

# Embedding settings
embedding_provider: local
embedding_api_url: http://localhost:11434
embedding_model: bge-m3
embedding_dimensions: 1024

# Memory decay
decay_enabled: true
decay_recalculate_interval: 1h
decay_archive_threshold: 0.05

# Auto-linking
auto_links_enabled: true
auto_links_similarity_threshold: 0.82

# Parallel execution
parallel_enabled: true
parallel_max_workers: 0           # 0 = auto (uses all CPUs)
parallel_min_batch_size: 1000     # Only parallelize for 1000+ items

# Server
bolt_port: 7687
http_port: 7474
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		return newCommandError(cmd, localization.NornicDBCLIWriteConfigFailed(err), err)
	}

	commandPrintln(cmd, localization.NornicDBCLIInitSummary(configPath, dataDir))

	return nil
}

func runShell(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data-dir")

	// Open database
	commandPrintln(cmd, localization.NornicDBCLIShellOpening(dataDir))
	config := nornicdb.DefaultConfig()
	config.Database.DataDir = dataDir

	db, err := nornicdb.Open(dataDir, config)
	if err != nil {
		return newCommandError(cmd, localization.NornicDBCLIOpenDatabaseFailed(err), err)
	}
	defer db.Close()

	executor := db.GetCypherExecutor()
	if executor == nil {
		return newCommandError(cmd, localization.NornicDBCLIShellUnavailable(), nil)
	}

	commandPrintln(cmd, localization.NornicDBCLIShellIntro())
	cmd.Println()

	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()

	for {
		fmt.Print("nornicdb> ")
		if !scanner.Scan() {
			break // EOF or error
		}

		query := strings.TrimSpace(scanner.Text())
		if query == "" {
			continue
		}

		if query == "exit" || query == "quit" {
			break
		}

		// Execute query
		result, err := executor.Execute(ctx, query, nil)
		if err != nil {
			commandPrintln(cmd, localization.NornicDBCLIShellQueryFailed())
			continue
		}

		// Display results
		if len(result.Columns) > 0 {
			displayColumns := make([]string, len(result.Columns))
			for i, col := range result.Columns {
				displayColumns[i] = shellDisplayColumn(col)
			}
			// Print header
			cmd.Println(strings.Join(displayColumns, " | "))
			cmd.Println(strings.Repeat("-", len(strings.Join(displayColumns, " | "))))

			// Print rows
			for _, row := range result.Rows {
				values := make([]string, len(row))
				for i, v := range row {
					column := ""
					if i < len(result.Columns) {
						column = result.Columns[i]
					}
					values[i] = shellDisplayValue(column, v)
				}
				// lgtm[go/clear-text-logging] -- interactive local output is intentional;
				// shellDisplayValue redacts values for sensitive column names.
				cmd.Println(strings.Join(values, " | "))
			}
			cmd.Println()
			commandPrintln(cmd, localization.NornicDBCLIShellRows(len(result.Rows)))
		} else {
			commandPrintln(cmd, localization.NornicDBCLIShellQuerySucceeded())
		}
		cmd.Println()
	}

	if err := scanner.Err(); err != nil {
		return newCommandError(cmd, localization.NornicDBCLIReadInputFailed(err), err)
	}

	commandPrintln(cmd, localization.NornicDBCLIShellGoodbye())
	return nil
}

func shellDisplayColumn(name string) string {
	if observability.IsSensitiveFieldName(name) {
		return observability.RedactedPlaceholder()
	}
	return name
}

func shellDisplayValue(column string, value interface{}) string {
	if observability.IsSensitiveFieldName(column) {
		return observability.RedactedPlaceholder()
	}
	return fmt.Sprintf("%v", value)
}

func runDecaySuppress(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data-dir")
	threshold, _ := cmd.Flags().GetFloat64("threshold")

	commandPrintln(cmd, localization.NornicDBCLIDecayOpening(dataDir))
	cfg := nornicdb.DefaultConfig()
	cfg.Database.DataDir = dataDir
	cfg.Memory.DecayEnabled = true

	db, err := nornicdb.Open(dataDir, cfg)
	if err != nil {
		return newCommandError(cmd, localization.NornicDBCLIOpenDatabaseFailed(err), err)
	}
	defer db.Close()

	be, ok := db.GetBaseStorageForManager().(*storage.BadgerEngine)
	if !ok {
		return newCommandError(cmd, localization.NornicDBCLIDecaySuppressStorage(), nil)
	}

	nodes, err := db.GetStorage().AllNodes()
	if err != nil {
		return newCommandError(cmd, localization.NornicDBCLILoadNodesFailed(err), err)
	}

	nowNanos := storage.DecayScoringTime()
	commandPrintln(cmd, localization.NornicDBCLIDecaySuppressing(threshold, len(nodes)))

	var newlySuppressed, alreadySuppressed, aboveThreshold int
	for _, node := range nodes {
		if node.VisibilitySuppressed {
			alreadySuppressed++
			continue
		}

		ns := storage.ExtractNamespaceFromID(string(node.ID))
		scorer := be.ScorerForNamespace(ns)
		if scorer == nil {
			aboveThreshold++
			continue
		}

		var accessMeta *knowledgepolicy.AccessMetaEntry
		if meta, metaErr := be.GetAccessMeta(string(node.ID)); metaErr == nil {
			accessMeta = meta
		}

		res := scorer.ScoreNode(
			string(node.ID), node.Labels, accessMeta,
			node.CreatedAt.UnixNano(), node.UpdatedAt.UnixNano(), nowNanos,
		)

		if res.FinalScore < threshold {
			becameSuppressed, err := be.EnqueueDeindexIfSuppressed(string(node.ID), false)
			if err != nil {
				commandPrintln(cmd, localization.NornicDBCLIDecaySuppressWarning(string(node.ID), err))
				continue
			}
			applySuppressionCounters(becameSuppressed, &newlySuppressed, &alreadySuppressed)
		} else {
			aboveThreshold++
		}
	}

	cmd.Println()
	commandPrintln(cmd, localization.NornicDBCLIDecaySuppressSummary(newlySuppressed, alreadySuppressed, aboveThreshold, len(nodes)))
	return nil
}

func applySuppressionCounters(becameSuppressed bool, newlySuppressed, alreadySuppressed *int) {
	if becameSuppressed {
		*newlySuppressed++
		return
	}
	*alreadySuppressed++
}

func runDecayStats(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data-dir")

	commandPrintln(cmd, localization.NornicDBCLIDecayOpening(dataDir))
	cfg := nornicdb.DefaultConfig()
	cfg.Database.DataDir = dataDir
	cfg.Memory.DecayEnabled = true

	db, err := nornicdb.Open(dataDir, cfg)
	if err != nil {
		return newCommandError(cmd, localization.NornicDBCLIOpenDatabaseFailed(err), err)
	}
	defer db.Close()

	be, ok := db.GetBaseStorageForManager().(*storage.BadgerEngine)
	if !ok {
		return newCommandError(cmd, localization.NornicDBCLIDecayStatsStorage(), nil)
	}

	nodes, err := db.GetStorage().AllNodes()
	if err != nil {
		return newCommandError(cmd, localization.NornicDBCLILoadNodesFailed(err), err)
	}

	nowNanos := storage.DecayScoringTime()

	var totalScored, suppressed, noDecay int
	var scoreSum float64
	scoreBuckets := map[string]int{
		"0.00-0.10": 0,
		"0.10-0.25": 0,
		"0.25-0.50": 0,
		"0.50-0.75": 0,
		"0.75-1.00": 0,
		"1.00":      0,
	}
	labelStats := make(map[string]struct{ count, suppressed int })

	for _, node := range nodes {
		if node.VisibilitySuppressed {
			suppressed++
		}

		ns := storage.ExtractNamespaceFromID(string(node.ID))
		scorer := be.ScorerForNamespace(ns)
		if scorer == nil {
			noDecay++
			scoreBuckets["1.00"]++
			continue
		}

		var accessMeta *knowledgepolicy.AccessMetaEntry
		if meta, metaErr := be.GetAccessMeta(string(node.ID)); metaErr == nil {
			accessMeta = meta
		}

		res := scorer.ScoreNode(
			string(node.ID), node.Labels, accessMeta,
			node.CreatedAt.UnixNano(), node.UpdatedAt.UnixNano(), nowNanos,
		)

		if res.NoDecay {
			noDecay++
			scoreBuckets["1.00"]++
		} else {
			totalScored++
			scoreSum += res.FinalScore

			switch {
			case res.FinalScore >= 1.0:
				scoreBuckets["1.00"]++
			case res.FinalScore >= 0.75:
				scoreBuckets["0.75-1.00"]++
			case res.FinalScore >= 0.50:
				scoreBuckets["0.50-0.75"]++
			case res.FinalScore >= 0.25:
				scoreBuckets["0.25-0.50"]++
			case res.FinalScore >= 0.10:
				scoreBuckets["0.10-0.25"]++
			default:
				scoreBuckets["0.00-0.10"]++
			}
		}

		for _, label := range node.Labels {
			s := labelStats[label]
			s.count++
			if res.SuppressionEligible {
				s.suppressed++
			}
			labelStats[label] = s
		}
	}

	average := ""
	if totalScored > 0 {
		average = fmt.Sprintf("%.4f", scoreSum/float64(totalScored))
	}
	cmd.Println()
	commandPrintln(cmd, localization.NornicDBCLIDecayStatsSummary(len(nodes), suppressed, totalScored, noDecay, average))

	cmd.Println()
	commandPrintln(cmd, localization.NornicDBCLIScoreDistribution())
	for _, bucket := range []string{"1.00", "0.75-1.00", "0.50-0.75", "0.25-0.50", "0.10-0.25", "0.00-0.10"} {
		fmt.Printf("  [%-9s]: %d\n", bucket, scoreBuckets[bucket])
	}

	if len(labelStats) > 0 {
		cmd.Println()
		commandPrintln(cmd, localization.NornicDBCLIPerLabelBreakdown())
		for label, s := range labelStats {
			commandPrintln(cmd, localization.NornicDBCLIPerLabelRow(label, s.count, s.suppressed))
		}
	}

	return nil
}

// getEnvStr returns environment variable value or default
func getEnvStr(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// getEnvInt returns environment variable as int or default
func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

// getEnvBool returns environment variable as bool or default
func getEnvBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		switch strings.ToLower(val) {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		}
	}
	return defaultVal
}

func serveAuthEnabled(cfg *config.Config, noAuth bool) bool {
	if noAuth || cfg == nil {
		return false
	}
	return cfg.Auth.Enabled
}
