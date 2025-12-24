// Package main provides the NornicDB CLI entry point.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cache"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/decay"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/pool"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/orneryd/nornicdb/ui"
)

var (
	version   = "0.1.0"
	commit    = "dev"
	buildTime = "unknown" // Set via ldflags: -X main.buildTime=$(date +%Y%m%d-%H%M%S)
)

// parseMemorySize parses a human-readable memory size string.
// Supports: "1024", "1KB", "1MB", "1GB", "1TB", "0", "unlimited"
func parseMemorySize(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" || s == "UNLIMITED" {
		return 0
	}

	s = strings.TrimSuffix(s, "B")

	var multiplier int64 = 1
	switch {
	case strings.HasSuffix(s, "K"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "K")
	case strings.HasSuffix(s, "M"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "M")
	case strings.HasSuffix(s, "G"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "G")
	case strings.HasSuffix(s, "T"):
		multiplier = 1024 * 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "T")
	}

	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return val * multiplier
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "nornicdb",
		Short: "NornicDB - High-Performance Graph Database for LLM Agents",
		Long: `NornicDB is a purpose-built graph database written in Go,
designed for AI agent memory with Neo4j Bolt/Cypher compatibility.

Features:
  • Neo4j Bolt protocol compatibility
  • Cypher query language support
  • Natural memory decay (Episodic/Semantic/Procedural)
  • Automatic relationship inference
  • Built-in vector search with RRF hybrid ranking
  • Server-side embedding generation`,
	}

	// Version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("NornicDB v%s (%s) built %s\n", version, commit, buildTime)
		},
	})

	// Serve command
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start NornicDB server",
		Long:  "Start NornicDB server with Bolt protocol and HTTP API endpoints",
		RunE:  runServe,
	}
	serveCmd.Flags().Int("bolt-port", getEnvInt("NORNICDB_BOLT_PORT", 7687), "Bolt protocol port (Neo4j compatible)")
	serveCmd.Flags().Int("http-port", getEnvInt("NORNICDB_HTTP_PORT", 7474), "HTTP API port")
	serveCmd.Flags().String("address", getEnvStr("NORNICDB_ADDRESS", "127.0.0.1"), "Bind address (127.0.0.1 for localhost only, 0.0.0.0 for all interfaces)")
	serveCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), "Data directory")
	serveCmd.Flags().String("load-export", getEnvStr("NORNICDB_LOAD_EXPORT", ""), "Load data from Mimir export directory on startup")
	serveCmd.Flags().String("embedding-provider", getEnvStr("NORNICDB_EMBEDDING_PROVIDER", "local"), "Embedding provider: local, ollama, openai")
	serveCmd.Flags().String("embedding-url", getEnvStr("NORNICDB_EMBEDDING_API_URL", "http://localhost:11434"), "Embedding API URL (ollama/openai)")
	serveCmd.Flags().String("embedding-key", getEnvStr("NORNICDB_EMBEDDING_API_KEY", ""), "Embeddings API Key (openai)")
	serveCmd.Flags().String("embedding-model", getEnvStr("NORNICDB_EMBEDDING_MODEL", "bge-m3"), "Embedding model name")
	serveCmd.Flags().Int("embedding-dim", getEnvInt("NORNICDB_EMBEDDING_DIMENSIONS", 1024), "Embedding dimensions")
	serveCmd.Flags().Int("embedding-cache", getEnvInt("NORNICDB_EMBEDDING_CACHE_SIZE", 10000), "Embedding cache size (0=disabled, default 10000)")
	serveCmd.Flags().Int("embedding-gpu-layers", getEnvInt("NORNICDB_EMBEDDING_GPU_LAYERS", -1), "GPU layers for local provider: -1=auto, 0=CPU only")
	serveCmd.Flags().String("gpu-backend", getEnvStr("NORNICDB_GPU_BACKEND", ""), "GPU backend: vulkan, cuda, metal, opencl (empty=auto-detect)")
	serveCmd.Flags().Bool("no-auth", false, "Disable authentication")
	serveCmd.Flags().String("admin-password", "password", "Admin password (default: password)")
	serveCmd.Flags().Bool("mcp-enabled", getEnvBool("NORNICDB_MCP_ENABLED", true), "Enable MCP (Model Context Protocol) server for LLM tools")
	// Parallel execution flags
	serveCmd.Flags().Bool("parallel", true, "Enable parallel query execution")
	serveCmd.Flags().Int("parallel-workers", 0, "Max parallel workers (0 = auto, uses all CPUs)")
	serveCmd.Flags().Int("parallel-batch-size", 1000, "Min batch size before parallelizing")
	// Memory management flags
	serveCmd.Flags().String("memory-limit", "", "Memory limit (e.g., 2GB, 512MB, 0 for unlimited)")
	serveCmd.Flags().Int("gc-percent", 100, "GC aggressiveness (100=default, lower=more aggressive)")
	serveCmd.Flags().Bool("pool-enabled", true, "Enable object pooling for reduced allocations")
	serveCmd.Flags().Bool("low-memory", getEnvBool("NORNICDB_LOW_MEMORY", false), "Use minimal RAM (for resource constrained environments)")
	serveCmd.Flags().Int("query-cache-size", 1000, "Query plan cache size (0 to disable)")
	serveCmd.Flags().String("query-cache-ttl", "5m", "Query plan cache TTL")
	// Logging flags
	serveCmd.Flags().Bool("log-queries", getEnvBool("NORNICDB_LOG_QUERIES", false), "Log all Bolt queries to stdout (for debugging)")
	// Headless mode
	serveCmd.Flags().Bool("headless", getEnvBool("NORNICDB_HEADLESS", false), "Disable web UI and browser-related endpoints")
	// Base path for reverse proxy deployment
	serveCmd.Flags().String("base-path", getEnvStr("NORNICDB_BASE_PATH", ""), "Base URL path for reverse proxy deployment (e.g., /nornicdb)")
	rootCmd.AddCommand(serveCmd)

	// Init command
	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a new NornicDB database",
		RunE:  runInit,
	}
	initCmd.Flags().String("data-dir", "./data", "Data directory")
	rootCmd.AddCommand(initCmd)

	// Import command
	importCmd := &cobra.Command{
		Use:   "import [directory]",
		Short: "Import data from Mimir export directory",
		Args:  cobra.ExactArgs(1),
		RunE:  runImport,
	}
	importCmd.Flags().String("data-dir", "./data", "Data directory")
	importCmd.Flags().String("embedding-url", "http://localhost:11434", "Embedding API URL")
	rootCmd.AddCommand(importCmd)

	// Shell command (interactive Cypher REPL)
	shellCmd := &cobra.Command{
		Use:   "shell",
		Short: "Interactive Cypher shell",
		RunE:  runShell,
	}
	shellCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), "Data directory")
	shellCmd.Flags().String("uri", "bolt://localhost:7687", "NornicDB URI (for future Bolt client support)")
	rootCmd.AddCommand(shellCmd)

	// Decay command (manual decay operations)
	decayCmd := &cobra.Command{
		Use:   "decay",
		Short: "Memory decay operations",
	}
	decayRecalculateCmd := &cobra.Command{
		Use:   "recalculate",
		Short: "Recalculate all decay scores",
		RunE:  runDecayRecalculate,
	}
	decayRecalculateCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), "Data directory")
	decayCmd.AddCommand(decayRecalculateCmd)

	decayArchiveCmd := &cobra.Command{
		Use:   "archive",
		Short: "Archive low-score memories",
		RunE:  runDecayArchive,
	}
	decayArchiveCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), "Data directory")
	decayArchiveCmd.Flags().Float64("threshold", 0.05, "Archive threshold (default: 0.05)")
	decayCmd.AddCommand(decayArchiveCmd)

	decayStatsCmd := &cobra.Command{
		Use:   "stats",
		Short: "Show decay statistics",
		RunE:  runDecayStats,
	}
	decayStatsCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), "Data directory")
	decayCmd.AddCommand(decayStatsCmd)
	rootCmd.AddCommand(decayCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServe(cmd *cobra.Command, args []string) error {
	boltPort, _ := cmd.Flags().GetInt("bolt-port")
	httpPort, _ := cmd.Flags().GetInt("http-port")
	address, _ := cmd.Flags().GetString("address")
	dataDir, _ := cmd.Flags().GetString("data-dir")
	loadExport, _ := cmd.Flags().GetString("load-export")
	embeddingProvider, _ := cmd.Flags().GetString("embedding-provider")
	embeddingURL, _ := cmd.Flags().GetString("embedding-url")
	embeddingKey, _ := cmd.Flags().GetString("embedding-key")
	embeddingModel, _ := cmd.Flags().GetString("embedding-model")
	embeddingDim, _ := cmd.Flags().GetInt("embedding-dim")
	embeddingCache, _ := cmd.Flags().GetInt("embedding-cache")
	embeddingGPULayers, _ := cmd.Flags().GetInt("embedding-gpu-layers")
	gpuBackend, _ := cmd.Flags().GetString("gpu-backend")
	noAuth, _ := cmd.Flags().GetBool("no-auth")

	// Set environment variable for local embedder GPU configuration
	if embeddingProvider == "local" {
		os.Setenv("NORNICDB_EMBEDDING_GPU_LAYERS", fmt.Sprintf("%d", embeddingGPULayers))
	}
	adminPassword, _ := cmd.Flags().GetString("admin-password")
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
	headless, _ := cmd.Flags().GetBool("headless")
	basePath, _ := cmd.Flags().GetString("base-path")

	// Apply memory configuration FIRST (before heavy allocations)
	// First, try to load from config file, then fall back to environment variables
	var cfg *config.Config
	configPath := config.FindConfigFile()
	if configPath != "" {
		var err error
		cfg, err = config.LoadFromFile(configPath)
		if err != nil {
			fmt.Printf("⚠️  Warning: failed to load config from %s: %v\n", configPath, err)
			cfg = config.LoadFromEnv()
		} else {
			fmt.Printf("📄 Loaded config from: %s\n", configPath)
		}
	} else {
		cfg = config.LoadFromEnv()
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

	// Override with CLI flags if provided
	if memoryLimit != "" {
		cfg.Memory.RuntimeLimitStr = memoryLimit
		cfg.Memory.RuntimeLimit = parseMemorySize(memoryLimit)
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

	// Display version with commit hash and build timestamp
	versionInfo := fmt.Sprintf("v%s", version)
	if commit != "dev" && commit != "" {
		versionInfo = fmt.Sprintf("v%s-%s", version, commit[:7]) // Short hash
	}
	if buildTime != "unknown" && buildTime != "" {
		versionInfo = fmt.Sprintf("%s (built: %s)", versionInfo, buildTime)
	}
	fmt.Printf("🚀 Starting NornicDB %s\n", versionInfo)
	fmt.Printf("   Data directory:  %s\n", dataDir)
	fmt.Printf("   Bolt protocol:   bolt://localhost:%d\n", boltPort)
	fmt.Printf("   HTTP API:        http://localhost:%d\n", httpPort)
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
		fmt.Printf("   Embedding:       local GGUF (%s/%s.gguf, %d dims, GPU: %s)\n", modelsDir, embeddingModel, embeddingDim, gpuMode)
	} else {
		fmt.Printf("   Embedding URL:   %s\n", embeddingURL)
		fmt.Printf("   Embedding model: %s (%d dims)\n", embeddingModel, embeddingDim)
	}
	if parallelEnabled {
		workers := parallelWorkers
		if workers == 0 {
			workers = runtime.NumCPU()
		}
		fmt.Printf("   Parallel exec:   ✅ enabled (%d workers, batch size %d)\n", workers, parallelBatchSize)
	} else {
		fmt.Printf("   Parallel exec:   ❌ disabled\n")
	}
	// Memory management info
	if cfg.Memory.RuntimeLimit > 0 {
		fmt.Printf("   Memory limit:    %s\n", config.FormatMemorySize(cfg.Memory.RuntimeLimit))
	} else {
		fmt.Printf("   Memory limit:    unlimited\n")
	}
	if cfg.Memory.GCPercent != 100 {
		fmt.Printf("   GC percent:      %d%% (more aggressive)\n", cfg.Memory.GCPercent)
	}
	if cfg.Memory.PoolEnabled {
		fmt.Printf("   Object pooling:  ✅ enabled\n")
	}
	if cfg.Memory.QueryCacheEnabled {
		fmt.Printf("   Query cache:     ✅ %d entries, TTL %v\n", cfg.Memory.QueryCacheSize, cfg.Memory.QueryCacheTTL)
	}
	fmt.Println()

	// Create data directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("creating data directory: %w", err)
	}

	// Configure database
	dbConfig := nornicdb.DefaultConfig()
	dbConfig.DataDir = dataDir
	dbConfig.BoltPort = boltPort
	dbConfig.HTTPPort = httpPort
	dbConfig.EmbeddingAPIURL = embeddingURL
	dbConfig.EmbeddingAPIKey = embeddingKey
	dbConfig.EmbeddingModel = embeddingModel
	dbConfig.EmbeddingDimensions = embeddingDim
	dbConfig.SearchMinSimilarity = cfg.Memory.SearchMinSimilarity
	dbConfig.EmbedWorkerNumWorkers = cfg.EmbeddingWorker.NumWorkers
	dbConfig.ParallelEnabled = parallelEnabled
	dbConfig.ParallelMaxWorkers = parallelWorkers
	dbConfig.ParallelMinBatchSize = parallelBatchSize

	// Memory mode
	lowMemory, _ := cmd.Flags().GetBool("low-memory")
	dbConfig.LowMemoryMode = lowMemory

	// Encryption settings from config
	dbConfig.EncryptionEnabled = cfg.Database.EncryptionEnabled
	dbConfig.EncryptionPassword = cfg.Database.EncryptionPassword

	// Async write settings from config
	dbConfig.AsyncWritesEnabled = cfg.Database.AsyncWritesEnabled
	dbConfig.AsyncFlushInterval = cfg.Database.AsyncFlushInterval
	dbConfig.AsyncMaxNodeCacheSize = cfg.Database.AsyncMaxNodeCacheSize
	dbConfig.AsyncMaxEdgeCacheSize = cfg.Database.AsyncMaxEdgeCacheSize

	// Badger in-process cache sizing (hot read paths)
	dbConfig.BadgerNodeCacheMaxEntries = cfg.Database.BadgerNodeCacheMaxEntries
	dbConfig.BadgerEdgeTypeCacheMaxTypes = cfg.Database.BadgerEdgeTypeCacheMaxTypes

	// Open database
	fmt.Println("📂 Opening database...")
	db, err := nornicdb.Open(dataDir, dbConfig)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	// Initialize GPU acceleration (Metal on macOS, auto-detect otherwise)
	fmt.Println("🎮 Initializing GPU acceleration...")
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
		fmt.Printf("   ⚠️  GPU not available: %v (using CPU)\n", gpuErr)
	} else if gpuManager.IsEnabled() {
		device := gpuManager.Device()
		db.SetGPUManager(gpuManager)
		fmt.Printf("   ✅ GPU enabled: %s (%s, %dMB)\n", device.Name, device.Backend, device.MemoryMB)
	} else {
		// Check if GPU hardware is present but CUDA not compiled in
		device := gpuManager.Device()
		if device != nil && device.MemoryMB > 0 {
			fmt.Printf("   ⚠️  GPU detected: %s (%dMB) - CUDA not compiled in, using CPU\n",
				device.Name, device.MemoryMB)
			fmt.Println("      💡 Build with Dockerfile.cuda for GPU acceleration")
		} else {
			fmt.Println("   ⚠️  GPU disabled (CPU fallback active)")
		}
	}

	// Load data if specified
	if loadExport != "" {
		fmt.Printf("📥 Loading data from %s...\n", loadExport)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		result, err := db.LoadFromExport(ctx, loadExport)
		if err != nil {
			return fmt.Errorf("loading export: %w", err)
		}
		fmt.Printf("   ✅ Loaded %d nodes, %d edges, %d embeddings\n",
			result.NodesLoaded, result.EdgesLoaded, result.EmbeddingsLoaded)

		// Build search indexes
		fmt.Println("🔍 Building search indexes...")
		if err := db.BuildSearchIndexes(ctx); err != nil {
			return fmt.Errorf("building indexes: %w", err)
		}
		fmt.Println("   ✅ Search indexes ready")
	}

	// Initialize DatabaseManager for multi-database support (needed for user storage)
	storageEngine := db.GetBaseStorageForManager()
	globalConfig := config.LoadFromEnv()
	multiDBConfig := &multidb.Config{
		DefaultDatabase:  globalConfig.Database.DefaultDatabase,
		SystemDatabase:   "system",
		MaxDatabases:     0, // Unlimited
		AllowDropDefault: false,
	}

	dbManager, err := multidb.NewDatabaseManager(storageEngine, multiDBConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize database manager: %w", err)
	}

	// Setup authentication
	var authenticator *auth.Authenticator
	if !noAuth {
		fmt.Println("🔐 Setting up authentication...")
		authConfig := auth.DefaultAuthConfig()
		// Use JWT secret from config (auto-generated if not set)
		if cfg.Auth.JWTSecret != "" {
			authConfig.JWTSecret = []byte(cfg.Auth.JWTSecret)
			fmt.Printf("   Using configured JWT secret (%d bytes)\n", len(cfg.Auth.JWTSecret))
		} else {
			// Fallback to a generated secret (will change on restart!)
			authConfig.JWTSecret = []byte("nornicdb-dev-key" + fmt.Sprintf("%d", time.Now().UnixNano()))
			fmt.Println("   ⚠️  No JWT secret configured - tokens will invalidate on restart!")
		}

		// Get system database storage for user persistence
		systemStorage, storageErr := dbManager.GetStorage("system")
		if storageErr != nil {
			return fmt.Errorf("failed to get system database storage: %w", storageErr)
		}

		var authErr error
		authenticator, authErr = auth.NewAuthenticator(authConfig, systemStorage)
		if authErr != nil {
			return fmt.Errorf("creating authenticator: %w", authErr)
		}

		// Create admin user
		_, err := authenticator.CreateUser("admin", adminPassword, []auth.Role{auth.RoleAdmin})
		if err != nil {
			// User might already exist
			fmt.Printf("   ⚠️  Admin user: %v\n", err)
		} else {
			fmt.Println("   ✅ Admin user created (admin)")
		}
	}
	// Note: Auth status logged at server startup

	// Create and start HTTP server
	serverConfig := server.DefaultConfig()
	serverConfig.Port = httpPort
	serverConfig.Address = address
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
	serverConfig.Headless = headless
	serverConfig.BasePath = basePath
	serverConfig.Features = &cfg.Features // Pass features loaded from YAML config
	// Pass plugin directories from loaded config
	serverConfig.PluginsDir = cfg.Server.PluginsDir
	serverConfig.HeimdallPluginsDir = cfg.Server.HeimdallPluginsDir
	// CORS configuration from loaded config
	serverConfig.EnableCORS = cfg.Server.EnableCORS
	serverConfig.CORSOrigins = cfg.Server.CORSOrigins

	// Enable embedded UI from the ui package (unless headless mode)
	if !headless {
		server.SetUIAssets(ui.Assets)
	}

	// Pass dbManager to server (it will use it if authenticator needs it)
	// Note: dbManager is created earlier for authenticator initialization
	httpServer, err := server.New(db, authenticator, serverConfig)
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

	// Start HTTP server (non-blocking)
	if err := httpServer.Start(); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}

	// Create and start Bolt server for Neo4j driver compatibility
	boltConfig := bolt.DefaultConfig()
	boltConfig.Port = boltPort
	boltConfig.LogQueries = logQueries

	// Create query executor adapter
	queryExecutor := &DBQueryExecutor{db: db}
	boltServer := bolt.New(boltConfig, queryExecutor)

	// Start Bolt server in goroutine
	go func() {
		if err := boltServer.ListenAndServe(); err != nil {
			fmt.Printf("Bolt server error: %v\n", err)
		}
	}()

	fmt.Println()
	fmt.Println("✅ NornicDB is ready!")
	fmt.Println()
	// Determine the display address for user-friendly output
	displayAddr := address
	if address == "0.0.0.0" {
		displayAddr = "localhost" // 0.0.0.0 is all interfaces, show localhost for convenience
	}
	fmt.Println("Endpoints:")
	fmt.Printf("  • HTTP API:     http://%s:%d\n", displayAddr, httpPort)
	fmt.Printf("  • Bolt:         bolt://%s:%d\n", displayAddr, boltPort)
	fmt.Printf("  • Health:       http://%s:%d/health\n", displayAddr, httpPort)
	fmt.Printf("  • Search:       POST http://%s:%d/nornicdb/search\n", displayAddr, httpPort)
	fmt.Printf("  • Cypher:       POST http://%s:%d/db/neo4j/tx/commit\n", displayAddr, httpPort)
	if mcpEnabled {
		fmt.Printf("  • MCP:          http://%s:%d/mcp\n", displayAddr, httpPort)
	}
	fmt.Println()
	if !noAuth {
		fmt.Println("Authentication:")
		fmt.Printf("  • Username: admin\n")
		fmt.Printf("  • Password: %s\n", adminPassword)
	}
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println()

	// Block until shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop Bolt server
	if err := boltServer.Close(); err != nil {
		fmt.Printf("Warning: error stopping Bolt server: %v\n", err)
	}

	if err := httpServer.Stop(ctx); err != nil {
		return fmt.Errorf("stopping HTTP server: %w", err)
	}

	fmt.Println("✅ Server stopped gracefully")
	return nil
}

// DBQueryExecutor adapts nornicdb.DB to bolt.QueryExecutor interface.
type DBQueryExecutor struct {
	db *nornicdb.DB
}

// Execute runs a Cypher query against the database.
func (e *DBQueryExecutor) Execute(ctx context.Context, query string, params map[string]any) (*bolt.QueryResult, error) {
	result, err := e.db.ExecuteCypher(ctx, query, params)
	if err != nil {
		return nil, err
	}

	return &bolt.QueryResult{
		Columns: result.Columns,
		Rows:    result.Rows,
	}, nil
}

func runInit(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data-dir")

	fmt.Printf("📂 Initializing NornicDB database in %s\n", dataDir)

	// Create directories
	dirs := []string{
		dataDir,
		filepath.Join(dataDir, "graph"),
		filepath.Join(dataDir, "indexes"),
		filepath.Join(dataDir, "embeddings"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating %s: %w", dir, err)
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
		return fmt.Errorf("writing config: %w", err)
	}

	fmt.Println("✅ Database initialized successfully")
	fmt.Printf("   Config: %s\n", configPath)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Start the server:  nornicdb serve --data-dir", dataDir)
	fmt.Println("  2. Load data:         nornicdb import ./export-dir --data-dir", dataDir)

	return nil
}

func runImport(cmd *cobra.Command, args []string) error {
	exportDir := args[0]
	dataDir, _ := cmd.Flags().GetString("data-dir")
	embeddingURL, _ := cmd.Flags().GetString("embedding-url")

	fmt.Printf("📥 Importing data from %s\n", exportDir)

	// Verify export directory exists
	if _, err := os.Stat(exportDir); os.IsNotExist(err) {
		return fmt.Errorf("export directory not found: %s", exportDir)
	}

	// Configure and open database
	config := nornicdb.DefaultConfig()
	config.DataDir = dataDir
	config.EmbeddingAPIURL = embeddingURL

	db, err := nornicdb.Open(dataDir, config)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	// Load data
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	startTime := time.Now()
	result, err := db.LoadFromExport(ctx, exportDir)
	if err != nil {
		return fmt.Errorf("loading export: %w", err)
	}
	loadDuration := time.Since(startTime)

	fmt.Printf("✅ Loaded %d nodes, %d edges, %d embeddings in %v\n",
		result.NodesLoaded, result.EdgesLoaded, result.EmbeddingsLoaded, loadDuration)

	// Build search indexes
	fmt.Println("🔍 Building search indexes...")
	startTime = time.Now()
	if err := db.BuildSearchIndexes(ctx); err != nil {
		return fmt.Errorf("building indexes: %w", err)
	}
	indexDuration := time.Since(startTime)
	fmt.Printf("✅ Search indexes built in %v\n", indexDuration)

	return nil
}

func runShell(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data-dir")

	// Open database
	fmt.Printf("📂 Opening database at %s...\n", dataDir)
	config := nornicdb.DefaultConfig()
	config.DataDir = dataDir

	db, err := nornicdb.Open(dataDir, config)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	executor := db.GetCypherExecutor()
	if executor == nil {
		return fmt.Errorf("cypher executor not available")
	}

	fmt.Println("✅ Connected to NornicDB")
	fmt.Println("Type 'exit' or Ctrl+D to quit")
	fmt.Println("Enter Cypher queries (end with semicolon or newline):")
	fmt.Println()

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
			fmt.Printf("❌ Error: %v\n", err)
			continue
		}

		// Display results
		if len(result.Columns) > 0 {
			// Print header
			fmt.Println(strings.Join(result.Columns, " | "))
			fmt.Println(strings.Repeat("-", len(strings.Join(result.Columns, " | "))))

			// Print rows
			for _, row := range result.Rows {
				values := make([]string, len(row))
				for i, v := range row {
					values[i] = fmt.Sprintf("%v", v)
				}
				fmt.Println(strings.Join(values, " | "))
			}
			fmt.Printf("\n(%d row(s))\n", len(result.Rows))
		} else {
			fmt.Println("✅ Query executed successfully")
		}
		fmt.Println()
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading input: %w", err)
	}

	fmt.Println("👋 Goodbye!")
	return nil
}

func runDecayRecalculate(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data-dir")

	// Open database
	fmt.Printf("📂 Opening database at %s...\n", dataDir)
	config := nornicdb.DefaultConfig()
	config.DataDir = dataDir

	db, err := nornicdb.Open(dataDir, config)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	// Get storage engine
	storageEngine := db.GetStorage()
	if storageEngine == nil {
		return fmt.Errorf("storage engine not available")
	}

	// Create decay manager
	decayManager := decay.New(decay.DefaultConfig())

	// Get all nodes
	fmt.Println("📊 Loading nodes...")
	nodes, err := storageEngine.AllNodes()
	if err != nil {
		return fmt.Errorf("loading nodes: %w", err)
	}

	fmt.Printf("🔄 Recalculating decay scores for %d nodes...\n", len(nodes))

	updated := 0

	// Process nodes in chunks to avoid memory issues
	chunkSize := 1000
	for i := 0; i < len(nodes); i += chunkSize {
		end := i + chunkSize
		if end > len(nodes) {
			end = len(nodes)
		}

		for _, node := range nodes[i:end] {
			// Extract tier from properties (default to SEMANTIC if not found)
			tierStr := "SEMANTIC"
			if v, ok := node.Properties["tier"].(string); ok {
				tierStr = strings.ToUpper(v)
			}

			// Map to decay.Tier
			var decayTier decay.Tier
			switch tierStr {
			case "EPISODIC":
				decayTier = decay.TierEpisodic
			case "SEMANTIC":
				decayTier = decay.TierSemantic
			case "PROCEDURAL":
				decayTier = decay.TierProcedural
			default:
				decayTier = decay.TierSemantic // Default
			}

			// Create MemoryInfo
			info := &decay.MemoryInfo{
				ID:           string(node.ID),
				Tier:         decayTier,
				CreatedAt:    node.CreatedAt,
				LastAccessed: node.LastAccessed,
				AccessCount:  node.AccessCount,
			}

			// Calculate new score
			newScore := decayManager.CalculateScore(info)

			// Update node if score changed
			if node.DecayScore != newScore {
				node.DecayScore = newScore
				if err := storageEngine.UpdateNode(node); err != nil {
					fmt.Printf("⚠️  Warning: failed to update node %s: %v\n", node.ID, err)
					continue
				}
				updated++
			}
		}

		if i%10000 == 0 && i > 0 {
			fmt.Printf("   Processed %d/%d nodes...\n", i, len(nodes))
		}
	}

	fmt.Printf("✅ Recalculated decay scores: %d nodes updated\n", updated)
	return nil
}

func runDecayArchive(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data-dir")
	threshold, _ := cmd.Flags().GetFloat64("threshold")

	// Open database
	fmt.Printf("📂 Opening database at %s...\n", dataDir)
	config := nornicdb.DefaultConfig()
	config.DataDir = dataDir

	db, err := nornicdb.Open(dataDir, config)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	// Get storage engine
	storageEngine := db.GetStorage()
	if storageEngine == nil {
		return fmt.Errorf("storage engine not available")
	}

	// Create decay manager with custom threshold
	decayConfig := decay.DefaultConfig()
	decayConfig.ArchiveThreshold = threshold
	decayManager := decay.New(decayConfig)

	// Get all nodes
	fmt.Println("📊 Loading nodes...")
	nodes, err := storageEngine.AllNodes()
	if err != nil {
		return fmt.Errorf("loading nodes: %w", err)
	}

	fmt.Printf("📦 Archiving nodes with decay score < %.2f...\n", threshold)

	archived := 0

	// Process nodes
	for _, node := range nodes {
		// Extract tier from properties
		tierStr := "SEMANTIC"
		if v, ok := node.Properties["tier"].(string); ok {
			tierStr = strings.ToUpper(v)
		}

		var decayTier decay.Tier
		switch tierStr {
		case "EPISODIC":
			decayTier = decay.TierEpisodic
		case "SEMANTIC":
			decayTier = decay.TierSemantic
		case "PROCEDURAL":
			decayTier = decay.TierProcedural
		default:
			decayTier = decay.TierSemantic
		}

		// Create MemoryInfo
		info := &decay.MemoryInfo{
			ID:           string(node.ID),
			Tier:         decayTier,
			CreatedAt:    node.CreatedAt,
			LastAccessed: node.LastAccessed,
			AccessCount:  node.AccessCount,
		}

		// Calculate current score
		score := decayManager.CalculateScore(info)

		// Check if should archive
		if decayManager.ShouldArchive(score) {
			// Mark as archived by adding "archived" property
			if node.Properties == nil {
				node.Properties = make(map[string]any)
			}
			node.Properties["archived"] = true
			node.Properties["archived_at"] = time.Now().Format(time.RFC3339)
			node.Properties["archived_score"] = score

			if err := storageEngine.UpdateNode(node); err != nil {
				fmt.Printf("⚠️  Warning: failed to archive node %s: %v\n", node.ID, err)
				continue
			}
			archived++
		}
	}

	fmt.Printf("✅ Archived %d nodes (decay score < %.2f)\n", archived, threshold)
	return nil
}

func runDecayStats(cmd *cobra.Command, args []string) error {
	dataDir, _ := cmd.Flags().GetString("data-dir")

	// Open database
	fmt.Printf("📂 Opening database at %s...\n", dataDir)
	config := nornicdb.DefaultConfig()
	config.DataDir = dataDir

	db, err := nornicdb.Open(dataDir, config)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	// Get storage engine
	storageEngine := db.GetStorage()
	if storageEngine == nil {
		return fmt.Errorf("storage engine not available")
	}

	// Create decay manager
	decayManager := decay.New(decay.DefaultConfig())

	// Get all nodes
	fmt.Println("📊 Loading nodes...")
	nodes, err := storageEngine.AllNodes()
	if err != nil {
		return fmt.Errorf("loading nodes: %w", err)
	}

	// Convert nodes to MemoryInfo and calculate stats
	memories := make([]decay.MemoryInfo, 0, len(nodes))
	for _, node := range nodes {
		// Extract tier from properties
		tierStr := "SEMANTIC"
		if v, ok := node.Properties["tier"].(string); ok {
			tierStr = strings.ToUpper(v)
		}

		var decayTier decay.Tier
		switch tierStr {
		case "EPISODIC":
			decayTier = decay.TierEpisodic
		case "SEMANTIC":
			decayTier = decay.TierSemantic
		case "PROCEDURAL":
			decayTier = decay.TierProcedural
		default:
			decayTier = decay.TierSemantic
		}

		memories = append(memories, decay.MemoryInfo{
			ID:           string(node.ID),
			Tier:         decayTier,
			CreatedAt:    node.CreatedAt,
			LastAccessed: node.LastAccessed,
			AccessCount:  node.AccessCount,
		})
	}

	// Get statistics
	stats := decayManager.GetStats(memories)

	// Display statistics
	fmt.Println("📊 Decay Statistics:")
	fmt.Printf("  Total memories: %d\n", stats.TotalMemories)
	fmt.Printf("  Episodic: %d (avg decay: %.2f)\n", stats.EpisodicCount, stats.AvgByTier[decay.TierEpisodic])
	fmt.Printf("  Semantic: %d (avg decay: %.2f)\n", stats.SemanticCount, stats.AvgByTier[decay.TierSemantic])
	fmt.Printf("  Procedural: %d (avg decay: %.2f)\n", stats.ProceduralCount, stats.AvgByTier[decay.TierProcedural])
	fmt.Printf("  Archived: %d (score < %.2f)\n", stats.ArchivedCount, decayManager.GetConfig().ArchiveThreshold)
	fmt.Printf("  Average decay score: %.2f\n", stats.AvgDecayScore)

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
