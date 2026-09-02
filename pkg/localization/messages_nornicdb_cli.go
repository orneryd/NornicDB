package localization

import "fmt"

const (
	MessageNornicDBCLIRootShort                   MessageID = "nornicdbcli.root.short"
	MessageNornicDBCLIRootLong                    MessageID = "nornicdbcli.root.long"
	MessageNornicDBCLIVersionShort                MessageID = "nornicdbcli.version.short"
	MessageNornicDBCLIServeShort                  MessageID = "nornicdbcli.serve.short"
	MessageNornicDBCLIServeLong                   MessageID = "nornicdbcli.serve.long"
	MessageNornicDBCLIInitShort                   MessageID = "nornicdbcli.init.short"
	MessageNornicDBCLIShellShort                  MessageID = "nornicdbcli.shell.short"
	MessageNornicDBCLIDecayShort                  MessageID = "nornicdbcli.decay.short"
	MessageNornicDBCLIDecaySuppressShort          MessageID = "nornicdbcli.decay.suppress.short"
	MessageNornicDBCLIDecayStatsShort             MessageID = "nornicdbcli.decay.stats.short"
	MessageNornicDBCLIFlagConfig                  MessageID = "nornicdbcli.flag.config"
	MessageNornicDBCLIFlagBoltPort                MessageID = "nornicdbcli.flag.bolt_port"
	MessageNornicDBCLIFlagHTTPPort                MessageID = "nornicdbcli.flag.http_port"
	MessageNornicDBCLIFlagAddress                 MessageID = "nornicdbcli.flag.address"
	MessageNornicDBCLIFlagDataDir                 MessageID = "nornicdbcli.flag.data_dir"
	MessageNornicDBCLIFlagUpgradeStorage          MessageID = "nornicdbcli.flag.upgrade_storage"
	MessageNornicDBCLIFlagEmbeddingProvider       MessageID = "nornicdbcli.flag.embedding_provider"
	MessageNornicDBCLIFlagEmbeddingURL            MessageID = "nornicdbcli.flag.embedding_url"
	MessageNornicDBCLIFlagEmbeddingKey            MessageID = "nornicdbcli.flag.embedding_key"
	MessageNornicDBCLIFlagEmbeddingModel          MessageID = "nornicdbcli.flag.embedding_model"
	MessageNornicDBCLIFlagEmbeddingDimensions     MessageID = "nornicdbcli.flag.embedding_dimensions"
	MessageNornicDBCLIFlagEmbeddingCache          MessageID = "nornicdbcli.flag.embedding_cache"
	MessageNornicDBCLIFlagEmbeddingGPULayers      MessageID = "nornicdbcli.flag.embedding_gpu_layers"
	MessageNornicDBCLIFlagEmbeddingEnabled        MessageID = "nornicdbcli.flag.embedding_enabled"
	MessageNornicDBCLIFlagSearchBM25Enabled       MessageID = "nornicdbcli.flag.search_bm25_enabled"
	MessageNornicDBCLIFlagSearchBM25Warming       MessageID = "nornicdbcli.flag.search_bm25_warming"
	MessageNornicDBCLIFlagSearchVectorEnabled     MessageID = "nornicdbcli.flag.search_vector_enabled"
	MessageNornicDBCLIFlagSearchVectorWarming     MessageID = "nornicdbcli.flag.search_vector_warming"
	MessageNornicDBCLIFlagGPUBackend              MessageID = "nornicdbcli.flag.gpu_backend"
	MessageNornicDBCLIFlagNoAuth                  MessageID = "nornicdbcli.flag.no_auth"
	MessageNornicDBCLIFlagAdminPassword           MessageID = "nornicdbcli.flag.admin_password"
	MessageNornicDBCLIFlagMCPEnabled              MessageID = "nornicdbcli.flag.mcp_enabled"
	MessageNornicDBCLIFlagParallel                MessageID = "nornicdbcli.flag.parallel"
	MessageNornicDBCLIFlagParallelWorkers         MessageID = "nornicdbcli.flag.parallel_workers"
	MessageNornicDBCLIFlagParallelBatchSize       MessageID = "nornicdbcli.flag.parallel_batch_size"
	MessageNornicDBCLIFlagMemoryLimit             MessageID = "nornicdbcli.flag.memory_limit"
	MessageNornicDBCLIFlagGCPercent               MessageID = "nornicdbcli.flag.gc_percent"
	MessageNornicDBCLIFlagPoolEnabled             MessageID = "nornicdbcli.flag.pool_enabled"
	MessageNornicDBCLIFlagLowMemory               MessageID = "nornicdbcli.flag.low_memory"
	MessageNornicDBCLIFlagQueryCacheSize          MessageID = "nornicdbcli.flag.query_cache_size"
	MessageNornicDBCLIFlagQueryCacheTTL           MessageID = "nornicdbcli.flag.query_cache_ttl"
	MessageNornicDBCLIFlagLogQueries              MessageID = "nornicdbcli.flag.log_queries"
	MessageNornicDBCLIFlagStdioLogMaxKB           MessageID = "nornicdbcli.flag.stdio_log_max_kb"
	MessageNornicDBCLIFlagStdioLogCompactSeconds  MessageID = "nornicdbcli.flag.stdio_log_compact_seconds"
	MessageNornicDBCLIFlagHeadless                MessageID = "nornicdbcli.flag.headless"
	MessageNornicDBCLIFlagBasePath                MessageID = "nornicdbcli.flag.base_path"
	MessageNornicDBCLIFlagClusterMode             MessageID = "nornicdbcli.flag.cluster_mode"
	MessageNornicDBCLIFlagClusterNodeID           MessageID = "nornicdbcli.flag.cluster_node_id"
	MessageNornicDBCLIFlagClusterBindAddress      MessageID = "nornicdbcli.flag.cluster_bind_address"
	MessageNornicDBCLIFlagClusterAdvertiseAddress MessageID = "nornicdbcli.flag.cluster_advertise_address"
	MessageNornicDBCLIFlagClusterDataDir          MessageID = "nornicdbcli.flag.cluster_data_dir"
	MessageNornicDBCLIFlagClusterHARole           MessageID = "nornicdbcli.flag.cluster_ha_role"
	MessageNornicDBCLIFlagClusterHAPeerAddress    MessageID = "nornicdbcli.flag.cluster_ha_peer_address"
	MessageNornicDBCLIFlagClusterRaftBootstrap    MessageID = "nornicdbcli.flag.cluster_raft_bootstrap"
	MessageNornicDBCLIFlagClusterRaftPeers        MessageID = "nornicdbcli.flag.cluster_raft_peers"
	MessageNornicDBCLIFlagURI                     MessageID = "nornicdbcli.flag.uri"
	MessageNornicDBCLIFlagDecayThreshold          MessageID = "nornicdbcli.flag.decay_threshold"
	MessageNornicDBCLIInvalidMemoryLimit          MessageID = "nornicdbcli.validation.invalid_memory_limit"
)

func nornicDBCLIText(id MessageID, fallback string) Message {
	return Message{ID: id, Fallback: fallback}
}

func NornicDBCLIRootShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIRootShort, "NornicDB - High-Performance Graph Database for LLM Agents")
}
func NornicDBCLIRootLong() Message {
	return nornicDBCLIText(MessageNornicDBCLIRootLong, `NornicDB is a purpose-built graph database written in Go,
designed for AI agent memory with Neo4j Bolt/Cypher compatibility.

Features:
  • Neo4j Bolt protocol compatibility
  • Cypher query language support
  • Knowledge-layer scoring with declarative decay profiles
  • Automatic relationship inference
  • Built-in vector search with RRF hybrid ranking
  • Server-side embedding generation`)
}
func NornicDBCLIVersionShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIVersionShort, "Print version information")
}
func NornicDBCLIServeShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIServeShort, "Start NornicDB server")
}
func NornicDBCLIServeLong() Message {
	return nornicDBCLIText(MessageNornicDBCLIServeLong, "Start NornicDB server with Bolt protocol and HTTP API endpoints")
}
func NornicDBCLIInitShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIInitShort, "Initialize a new NornicDB database")
}
func NornicDBCLIShellShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIShellShort, "Interactive Cypher shell")
}
func NornicDBCLIDecayShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIDecayShort, "Memory decay operations")
}
func NornicDBCLIDecaySuppressShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIDecaySuppressShort, "Suppress nodes below visibility threshold")
}
func NornicDBCLIDecayStatsShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIDecayStatsShort, "Show decay statistics")
}
func NornicDBCLIFlagConfig() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagConfig, "Path to YAML config file (overrides auto-discovery)")
}
func NornicDBCLIFlagBoltPort() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagBoltPort, "Bolt protocol port (Neo4j compatible)")
}
func NornicDBCLIFlagHTTPPort() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagHTTPPort, "HTTP API port")
}
func NornicDBCLIFlagAddress() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagAddress, "Bind address (127.0.0.1 for localhost only, 0.0.0.0 for all interfaces)")
}
func NornicDBCLIFlagDataDir() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagDataDir, "Data directory")
}
func NornicDBCLIFlagUpgradeStorage() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagUpgradeStorage, "Authorize one-way upgrade of the data directory's storage version through every migration arm this binary understands. Back up before enabling.")
}
func NornicDBCLIFlagEmbeddingProvider() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagEmbeddingProvider, "Embedding provider: local, ollama, openai")
}
func NornicDBCLIFlagEmbeddingURL() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagEmbeddingURL, "Embedding API URL (ollama/openai)")
}
func NornicDBCLIFlagEmbeddingKey() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagEmbeddingKey, "Embeddings API Key (openai)")
}
func NornicDBCLIFlagEmbeddingModel() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagEmbeddingModel, "Embedding model name")
}
func NornicDBCLIFlagEmbeddingDimensions() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagEmbeddingDimensions, "Embedding dimensions")
}
func NornicDBCLIFlagEmbeddingCache() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagEmbeddingCache, "Embedding cache size (0=disabled, default 10000)")
}
func NornicDBCLIFlagEmbeddingGPULayers() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagEmbeddingGPULayers, "GPU layers for local provider: -1=auto, 0=CPU only")
}
func NornicDBCLIFlagEmbeddingEnabled() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagEmbeddingEnabled, "Enable embedding generation (semantic search). Default is off unless enabled via config/env.")
}
func NornicDBCLIFlagSearchBM25Enabled() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagSearchBM25Enabled, "Enable BM25 fulltext search (default: true). Per-DB override via /admin/databases/{name}/config wins.")
}
func NornicDBCLIFlagSearchBM25Warming() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagSearchBM25Warming, "BM25 build trigger: startup (build at boot) or lazy (defer to first inbound search query). Default: startup.")
}
func NornicDBCLIFlagSearchVectorEnabled() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagSearchVectorEnabled, "Enable vector (ANN) search across all strategies (HNSW, IVF-HNSW, brute-force, GPU, Metal, Qdrant). When false, no node embeddings load into RAM. Per-DB override wins.")
}
func NornicDBCLIFlagSearchVectorWarming() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagSearchVectorWarming, "Vector build trigger: startup or lazy. Default: startup.")
}
func NornicDBCLIFlagGPUBackend() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagGPUBackend, "GPU backend: vulkan, cuda, metal, opencl (empty=auto-detect)")
}
func NornicDBCLIFlagNoAuth() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagNoAuth, "Disable authentication")
}
func NornicDBCLIFlagAdminPassword() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagAdminPassword, "Admin password (default: password)")
}
func NornicDBCLIFlagMCPEnabled() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagMCPEnabled, "Enable MCP (Model Context Protocol) server for LLM tools")
}
func NornicDBCLIFlagParallel() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagParallel, "Enable parallel query execution")
}
func NornicDBCLIFlagParallelWorkers() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagParallelWorkers, "Max parallel workers (0 = auto, uses all CPUs)")
}
func NornicDBCLIFlagParallelBatchSize() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagParallelBatchSize, "Min batch size before parallelizing")
}
func NornicDBCLIFlagMemoryLimit() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagMemoryLimit, "Memory limit in MB as an integer (e.g., 500, 0 for unlimited)")
}
func NornicDBCLIFlagGCPercent() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagGCPercent, "GC aggressiveness (100=default, lower=more aggressive)")
}
func NornicDBCLIFlagPoolEnabled() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagPoolEnabled, "Enable object pooling for reduced allocations")
}
func NornicDBCLIFlagLowMemory() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagLowMemory, "Use minimal RAM (for resource constrained environments)")
}
func NornicDBCLIFlagQueryCacheSize() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagQueryCacheSize, "Per-database query result cache size (0 to disable)")
}
func NornicDBCLIFlagQueryCacheTTL() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagQueryCacheTTL, "Per-database query result cache TTL in milliseconds")
}
func NornicDBCLIFlagLogQueries() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagLogQueries, "Log all Bolt queries to stdout (for debugging)")
}
func NornicDBCLIFlagStdioLogMaxKB() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagStdioLogMaxKB, "Max size of stdout/stderr log files in KB before automatic truncation (0 disables)")
}
func NornicDBCLIFlagStdioLogCompactSeconds() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagStdioLogCompactSeconds, "Interval in seconds for automatic stdout/stderr log size checks")
}
func NornicDBCLIFlagHeadless() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagHeadless, "Disable web UI and browser-related endpoints")
}
func NornicDBCLIFlagBasePath() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagBasePath, "Base URL path for reverse proxy deployment (e.g., /nornicdb)")
}
func NornicDBCLIFlagClusterMode() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagClusterMode, "Cluster mode: standalone|ha_standby|raft|multi_region (empty disables clustering)")
}
func NornicDBCLIFlagClusterNodeID() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagClusterNodeID, "Cluster node ID (empty auto-generates)")
}
func NornicDBCLIFlagClusterBindAddress() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagClusterBindAddress, "Cluster bind address for replication protocol (e.g., 127.0.0.1:7000)")
}
func NornicDBCLIFlagClusterAdvertiseAddress() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagClusterAdvertiseAddress, "Cluster advertise address (defaults to bind addr)")
}
func NornicDBCLIFlagClusterDataDir() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagClusterDataDir, "Cluster state directory (defaults to <data-dir>/replication)")
}
func NornicDBCLIFlagClusterHARole() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagClusterHARole, "HA standby role: primary|standby")
}
func NornicDBCLIFlagClusterHAPeerAddress() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagClusterHAPeerAddress, "HA standby peer cluster address (host:port)")
}
func NornicDBCLIFlagClusterRaftBootstrap() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagClusterRaftBootstrap, "Raft bootstrap (true for first node in a new cluster)")
}
func NornicDBCLIFlagClusterRaftPeers() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagClusterRaftPeers, "Raft peers (format: node2:host2:7000,node3:host3:7000)")
}
func NornicDBCLIFlagURI() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagURI, "NornicDB URI (for future Bolt client support)")
}
func NornicDBCLIFlagDecayThreshold() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagDecayThreshold, "Visibility suppression threshold (default: 0.05)")
}

// NornicDBCLIInvalidMemoryLimit identifies an invalid memory-limit flag value.
func NornicDBCLIInvalidMemoryLimit(value string, cause error) Message {
	return Message{ID: MessageNornicDBCLIInvalidMemoryLimit, Fallback: fmt.Sprintf("invalid --memory-limit value %q: %s", value, cause), Data: map[string]any{"Value": value, "Cause": cause.Error()}}
}
