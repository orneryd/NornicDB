// Package main provides the NornicDB CLI entry point.
package main

import (
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
	"github.com/orneryd/nornicdb/pkg/gpu"
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
	serveCmd.Flags().String("data-dir", getEnvStr("NORNICDB_DATA_DIR", "./data"), "Data directory")
	serveCmd.Flags().String("load-export", getEnvStr("NORNICDB_LOAD_EXPORT", ""), "Load data from Mimir export directory on startup")
	serveCmd.Flags().String("embedding-provider", getEnvStr("NORNICDB_EMBEDDING_PROVIDER", "ollama"), "Embedding provider: local, ollama, openai")
	serveCmd.Flags().String("embedding-url", getEnvStr("NORNICDB_EMBEDDING_API_URL", "http://localhost:11434"), "Embedding API URL (ollama/openai)")
	serveCmd.Flags().String("embedding-key", getEnvStr("NORNICDB_EMBEDDING_API_KEY", ""), "Embeddings API Key (openai)")
	serveCmd.Flags().String("embedding-model", getEnvStr("NORNICDB_EMBEDDING_MODEL", "mxbai-embed-large"), "Embedding model name")
	serveCmd.Flags().Int("embedding-dim", getEnvInt("NORNICDB_EMBEDDING_DIMENSIONS", 1024), "Embedding dimensions")
	serveCmd.Flags().Int("embedding-cache", getEnvInt("NORNICDB_EMBEDDING_CACHE_SIZE", 10000), "Embedding cache size (0=disabled, default 10000)")
	serveCmd.Flags().Int("embedding-gpu-layers", getEnvInt("NORNICDB_EMBEDDING_GPU_LAYERS", -1), "GPU layers for local provider: -1=auto, 0=CPU only")
	serveCmd.Flags().Bool("no-auth", false, "Disable authentication")
	serveCmd.Flags().String("admin-password", "admin", "Admin password (default: admin)")
	// Parallel execution flags
	serveCmd.Flags().Bool("parallel", true, "Enable parallel query execution")
	serveCmd.Flags().Int("parallel-workers", 0, "Max parallel workers (0 = auto, uses all CPUs)")
	serveCmd.Flags().Int("parallel-batch-size", 1000, "Min batch size before parallelizing")
	// Memory management flags
	serveCmd.Flags().String("memory-limit", "", "Memory limit (e.g., 2GB, 512MB, 0 for unlimited)")
	serveCmd.Flags().Int("gc-percent", 100, "GC aggressiveness (100=default, lower=more aggressive)")
	serveCmd.Flags().Bool("pool-enabled", true, "Enable object pooling for reduced allocations")
	serveCmd.Flags().Int("query-cache-size", 1000, "Query plan cache size (0 to disable)")
	serveCmd.Flags().String("query-cache-ttl", "5m", "Query plan cache TTL")
	// Logging flags
	serveCmd.Flags().Bool("log-queries", getEnvBool("NORNICDB_LOG_QUERIES", false), "Log all Bolt queries to stdout (for debugging)")
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
	shellCmd.Flags().String("uri", "bolt://localhost:7687", "NornicDB URI")
	rootCmd.AddCommand(shellCmd)

	// Decay command (manual decay operations)
	decayCmd := &cobra.Command{
		Use:   "decay",
		Short: "Memory decay operations",
	}
	decayCmd.AddCommand(&cobra.Command{
		Use:   "recalculate",
		Short: "Recalculate all decay scores",
		RunE:  runDecayRecalculate,
	})
	decayCmd.AddCommand(&cobra.Command{
		Use:   "archive",
		Short: "Archive low-score memories",
		RunE:  runDecayArchive,
	})
	decayCmd.AddCommand(&cobra.Command{
		Use:   "stats",
		Short: "Show decay statistics",
		RunE:  runDecayStats,
	})
	rootCmd.AddCommand(decayCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServe(cmd *cobra.Command, args []string) error {
	boltPort, _ := cmd.Flags().GetInt("bolt-port")
	httpPort, _ := cmd.Flags().GetInt("http-port")
	dataDir, _ := cmd.Flags().GetString("data-dir")
	loadExport, _ := cmd.Flags().GetString("load-export")
	embeddingProvider, _ := cmd.Flags().GetString("embedding-provider")
	embeddingURL, _ := cmd.Flags().GetString("embedding-url")
	embeddingKey, _ := cmd.Flags().GetString("embedding-key")
	embeddingModel, _ := cmd.Flags().GetString("embedding-model")
	embeddingDim, _ := cmd.Flags().GetInt("embedding-dim")
	embeddingCache, _ := cmd.Flags().GetInt("embedding-cache")
	embeddingGPULayers, _ := cmd.Flags().GetInt("embedding-gpu-layers")
	noAuth, _ := cmd.Flags().GetBool("no-auth")

	// Set environment variable for local embedder GPU configuration
	if embeddingProvider == "local" {
		os.Setenv("NORNICDB_EMBEDDING_GPU_LAYERS", fmt.Sprintf("%d", embeddingGPULayers))
	}
	adminPassword, _ := cmd.Flags().GetString("admin-password")
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

	// Apply memory configuration FIRST (before heavy allocations)
	cfg := config.LoadFromEnv()

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

	fmt.Printf("🚀 Starting NornicDB v%s (build: %s)\n", version, buildTime)
	fmt.Printf("   Data directory:  %s\n", dataDir)
	fmt.Printf("   Bolt protocol:   bolt://localhost:%d\n", boltPort)
	fmt.Printf("   HTTP API:        http://localhost:%d\n", httpPort)
	if embeddingProvider == "local" {
		modelsDir := os.Getenv("NORNICDB_MODELS_DIR")
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
	dbConfig.ParallelEnabled = parallelEnabled
	dbConfig.ParallelMaxWorkers = parallelWorkers
	dbConfig.ParallelMinBatchSize = parallelBatchSize

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

	// Prefer Metal on macOS/Apple Silicon
	if runtime.GOOS == "darwin" {
		gpuConfig.PreferredBackend = gpu.BackendMetal
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

	// Setup authentication
	var authenticator *auth.Authenticator
	if !noAuth {
		fmt.Println("🔐 Setting up authentication...")
		authConfig := auth.DefaultAuthConfig()
		authConfig.JWTSecret = []byte("nornicdb-dev-secret") // TODO: Make configurable

		var authErr error
		authenticator, authErr = auth.NewAuthenticator(authConfig)
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
	} else {
		fmt.Println("⚠️  Authentication disabled")
	}

	// Create and start HTTP server
	serverConfig := server.DefaultConfig()
	serverConfig.Port = httpPort
	// Pass embedding settings to server
	serverConfig.EmbeddingEnabled = true
	serverConfig.EmbeddingProvider = embeddingProvider
	serverConfig.EmbeddingAPIURL = embeddingURL
	serverConfig.EmbeddingModel = embeddingModel
	serverConfig.EmbeddingDimensions = embeddingDim
	serverConfig.EmbeddingCacheSize = embeddingCache

	// Enable embedded UI from the ui package
	server.SetUIAssets(ui.Assets)

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
	fmt.Println("Endpoints:")
	fmt.Printf("  • HTTP API:     http://localhost:%d\n", httpPort)
	fmt.Printf("  • Bolt:         bolt://localhost:%d\n", boltPort)
	fmt.Printf("  • Health:       http://localhost:%d/health\n", httpPort)
	fmt.Printf("  • Search:       POST http://localhost:%d/nornicdb/search\n", httpPort)
	fmt.Printf("  • Cypher:       POST http://localhost:%d/db/neo4j/tx/commit\n", httpPort)
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
embedding_provider: ollama
embedding_api_url: http://localhost:11434
embedding_model: mxbai-embed-large
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
	uri, _ := cmd.Flags().GetString("uri")
	fmt.Printf("🔌 Connecting to %s...\n", uri)
	fmt.Println("Type 'exit' or Ctrl+D to quit")
	fmt.Println()

	// TODO: Implement interactive REPL
	fmt.Println("Interactive shell coming soon...")
	fmt.Println("For now, use the HTTP API:")
	fmt.Println("  curl -X POST http://localhost:7474/db/neo4j/tx/commit \\")
	fmt.Println("    -H 'Content-Type: application/json' \\")
	fmt.Println("    -d '{\"statements\": [{\"statement\": \"MATCH (n) RETURN n LIMIT 5\"}]}'")

	return nil
}

func runDecayRecalculate(cmd *cobra.Command, args []string) error {
	fmt.Println("🔄 Recalculating decay scores...")
	// TODO: Implement
	return nil
}

func runDecayArchive(cmd *cobra.Command, args []string) error {
	fmt.Println("📦 Archiving low-score memories...")
	// TODO: Implement
	return nil
}

func runDecayStats(cmd *cobra.Command, args []string) error {
	fmt.Println("📊 Decay Statistics:")
	fmt.Println("  Total memories: 0")
	fmt.Println("  Episodic: 0 (avg decay: 0.00)")
	fmt.Println("  Semantic: 0 (avg decay: 0.00)")
	fmt.Println("  Procedural: 0 (avg decay: 0.00)")
	fmt.Println("  Archived: 0")
	// TODO: Implement
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
