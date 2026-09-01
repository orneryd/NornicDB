package localization

import "fmt"

const (
	MessageNornicDBCLIInvalidStdioInterval      MessageID = "nornicdbcli.validation.invalid_stdio_interval"
	MessageNornicDBCLINoConfig                  MessageID = "nornicdbcli.config.not_found"
	MessageNornicDBCLIConfigWarning             MessageID = "nornicdbcli.config.load_warning"
	MessageNornicDBCLIConfigLoaded              MessageID = "nornicdbcli.config.loaded"
	MessageNornicDBCLIStarting                  MessageID = "nornicdbcli.serve.starting"
	MessageNornicDBCLIDataDirectory             MessageID = "nornicdbcli.serve.data_directory"
	MessageNornicDBCLIBoltEndpoint              MessageID = "nornicdbcli.serve.bolt_endpoint"
	MessageNornicDBCLIHTTPEndpoint              MessageID = "nornicdbcli.serve.http_endpoint"
	MessageNornicDBCLIEmbeddingsEnabled         MessageID = "nornicdbcli.serve.embeddings_enabled"
	MessageNornicDBCLIEmbeddingsDisabled        MessageID = "nornicdbcli.serve.embeddings_disabled"
	MessageNornicDBCLIEmbeddingLocal            MessageID = "nornicdbcli.serve.embedding_local"
	MessageNornicDBCLIEmbeddingURL              MessageID = "nornicdbcli.serve.embedding_url"
	MessageNornicDBCLIEmbeddingModel            MessageID = "nornicdbcli.serve.embedding_model"
	MessageNornicDBCLIParallelEnabled           MessageID = "nornicdbcli.serve.parallel_enabled"
	MessageNornicDBCLIParallelDisabled          MessageID = "nornicdbcli.serve.parallel_disabled"
	MessageNornicDBCLIMemoryLimit               MessageID = "nornicdbcli.serve.memory_limit"
	MessageNornicDBCLIMemoryUnlimited           MessageID = "nornicdbcli.serve.memory_unlimited"
	MessageNornicDBCLIGCPercent                 MessageID = "nornicdbcli.serve.gc_percent"
	MessageNornicDBCLIObjectPooling             MessageID = "nornicdbcli.serve.object_pooling"
	MessageNornicDBCLIQueryCache                MessageID = "nornicdbcli.serve.query_cache"
	MessageNornicDBCLIOpeningDatabase           MessageID = "nornicdbcli.progress.opening_database"
	MessageNornicDBCLIInitializingGPU           MessageID = "nornicdbcli.progress.initializing_gpu"
	MessageNornicDBCLIGPUUnavailable            MessageID = "nornicdbcli.serve.gpu_unavailable"
	MessageNornicDBCLIGPUEnabled                MessageID = "nornicdbcli.serve.gpu_enabled"
	MessageNornicDBCLIGPUDetectedNoCUDA         MessageID = "nornicdbcli.serve.gpu_detected_no_cuda"
	MessageNornicDBCLIBuildCUDA                 MessageID = "nornicdbcli.serve.build_cuda"
	MessageNornicDBCLIGPUDisabled               MessageID = "nornicdbcli.serve.gpu_disabled"
	MessageNornicDBCLISettingUpAuth             MessageID = "nornicdbcli.progress.setting_up_auth"
	MessageNornicDBCLIConfiguredJWT             MessageID = "nornicdbcli.serve.configured_jwt"
	MessageNornicDBCLINoJWT                     MessageID = "nornicdbcli.serve.no_jwt"
	MessageNornicDBCLIAdminSetupFailed          MessageID = "nornicdbcli.serve.admin_setup_failed"
	MessageNornicDBCLIAdminCreated              MessageID = "nornicdbcli.serve.admin_created"
	MessageNornicDBCLIReady                     MessageID = "nornicdbcli.serve.ready"
	MessageNornicDBCLIEndpoints                 MessageID = "nornicdbcli.serve.endpoints"
	MessageNornicDBCLIAuthentication            MessageID = "nornicdbcli.serve.authentication"
	MessageNornicDBCLIPressToStop               MessageID = "nornicdbcli.serve.press_to_stop"
	MessageNornicDBCLIServerStopped             MessageID = "nornicdbcli.serve.stopped"
	MessageNornicDBCLIInitProgress              MessageID = "nornicdbcli.init.progress"
	MessageNornicDBCLIInitSummary               MessageID = "nornicdbcli.init.summary"
	MessageNornicDBCLIShellOpening              MessageID = "nornicdbcli.shell.opening"
	MessageNornicDBCLIShellIntro                MessageID = "nornicdbcli.shell.intro"
	MessageNornicDBCLIShellQueryFailed          MessageID = "nornicdbcli.shell.query_failed"
	MessageNornicDBCLIShellRows                 MessageID = "nornicdbcli.shell.rows"
	MessageNornicDBCLIShellQuerySucceeded       MessageID = "nornicdbcli.shell.query_succeeded"
	MessageNornicDBCLIShellGoodbye              MessageID = "nornicdbcli.shell.goodbye"
	MessageNornicDBCLIDecayOpening              MessageID = "nornicdbcli.decay.opening"
	MessageNornicDBCLIDecaySuppressing          MessageID = "nornicdbcli.decay.suppressing"
	MessageNornicDBCLIDecaySuppressWarning      MessageID = "nornicdbcli.decay.suppress_warning"
	MessageNornicDBCLIDecaySuppressSummary      MessageID = "nornicdbcli.decay.suppress_summary"
	MessageNornicDBCLIDecayStatsSummary         MessageID = "nornicdbcli.decay.stats_summary"
	MessageNornicDBCLIScoreDistribution         MessageID = "nornicdbcli.decay.score_distribution"
	MessageNornicDBCLIPerLabelBreakdown         MessageID = "nornicdbcli.decay.per_label_breakdown"
	MessageNornicDBCLIPerLabelRow               MessageID = "nornicdbcli.decay.per_label_row"
	MessageNornicDBCLIShellUnavailable          MessageID = "nornicdbcli.validation.shell_unavailable"
	MessageNornicDBCLIDecaySuppressStorage      MessageID = "nornicdbcli.validation.decay_suppress_storage"
	MessageNornicDBCLIDecayStatsStorage         MessageID = "nornicdbcli.validation.decay_stats_storage"
	MessageNornicDBCLIConfigLoadFailed          MessageID = "nornicdbcli.config.load_failed"
	MessageNornicDBCLICreateDataDirectoryFailed MessageID = "nornicdbcli.serve.create_data_directory_failed"
	MessageNornicDBCLIOpenDatabaseFailed        MessageID = "nornicdbcli.operation.open_database_failed"
	MessageNornicDBCLICreateAuthenticatorFailed MessageID = "nornicdbcli.serve.create_authenticator_failed"
	MessageNornicDBCLICreateServerFailed        MessageID = "nornicdbcli.serve.create_server_failed"
	MessageNornicDBCLIBoltTLSClientAuthFailed   MessageID = "nornicdbcli.serve.bolt_tls_client_auth_failed"
	MessageNornicDBCLIBoltTLSLoadFailed         MessageID = "nornicdbcli.serve.bolt_tls_load_failed"
	MessageNornicDBCLIObservabilityInitFailed   MessageID = "nornicdbcli.serve.observability_init_failed"
	MessageNornicDBCLIStartServerFailed         MessageID = "nornicdbcli.serve.start_server_failed"
	MessageNornicDBCLITelemetryListenerFailed   MessageID = "nornicdbcli.serve.telemetry_listener_failed"
	MessageNornicDBCLIPprofListenerFailed       MessageID = "nornicdbcli.serve.pprof_listener_failed"
	MessageNornicDBCLISupervisedRunFailed       MessageID = "nornicdbcli.serve.supervised_run_failed"
	MessageNornicDBCLILocalizationInitFailed    MessageID = "nornicdbcli.serve.localization_init_failed"
	MessageNornicDBCLIBoltAdapterFailed         MessageID = "nornicdbcli.adapter.bolt_failed"
	MessageNornicDBCLIDatabaseNotInitialized    MessageID = "nornicdbcli.transaction.database_not_initialized"
	MessageNornicDBCLITransactionNotFound       MessageID = "nornicdbcli.transaction.not_found"
	MessageNornicDBCLITransactionAlreadyActive  MessageID = "nornicdbcli.transaction.already_active"
	MessageNornicDBCLICreateDirectoryFailed     MessageID = "nornicdbcli.init.create_directory_failed"
	MessageNornicDBCLIWriteConfigFailed         MessageID = "nornicdbcli.init.write_config_failed"
	MessageNornicDBCLIReadInputFailed           MessageID = "nornicdbcli.shell.read_input_failed"
	MessageNornicDBCLILoadNodesFailed           MessageID = "nornicdbcli.decay.load_nodes_failed"
)

func cliOutput(id MessageID, fallback string, data map[string]any) Message {
	return Message{ID: id, Fallback: fallback, Data: data}
}

func NornicDBCLIInvalidStdioInterval(value int) Message {
	return cliOutput(MessageNornicDBCLIInvalidStdioInterval, fmt.Sprintf("invalid stdio-log-compact-seconds %d: must be >= 0", value), map[string]any{"Value": value})
}
func NornicDBCLINoConfig() Message {
	return cliOutput(MessageNornicDBCLINoConfig, "📄 No config file found (using defaults + environment variables)", nil)
}
func NornicDBCLIConfigWarning(path string, cause error) Message {
	return cliOutput(MessageNornicDBCLIConfigWarning, fmt.Sprintf("⚠️  Warning: failed to load config from %s: %v", path, cause), map[string]any{"Path": path, "Cause": cause.Error()})
}
func NornicDBCLIConfigLoaded(path string) Message {
	return cliOutput(MessageNornicDBCLIConfigLoaded, "📄 Loaded config from: "+path, map[string]any{"Path": path})
}
func NornicDBCLIStarting(version string) Message {
	return cliOutput(MessageNornicDBCLIStarting, "🚀 Starting NornicDB "+version, map[string]any{"Version": version})
}
func NornicDBCLIDataDirectory(path string) Message {
	return cliOutput(MessageNornicDBCLIDataDirectory, "   Data directory:  "+path, map[string]any{"Path": path})
}
func NornicDBCLIBoltEndpoint(port int) Message {
	return cliOutput(MessageNornicDBCLIBoltEndpoint, fmt.Sprintf("   Bolt protocol:   bolt://localhost:%d", port), map[string]any{"Port": port})
}
func NornicDBCLIHTTPEndpoint(port int) Message {
	return cliOutput(MessageNornicDBCLIHTTPEndpoint, fmt.Sprintf("   HTTP API:        http://localhost:%d", port), map[string]any{"Port": port})
}
func NornicDBCLIEmbeddingsEnabled(provider, model string, dimensions int) Message {
	return cliOutput(MessageNornicDBCLIEmbeddingsEnabled, fmt.Sprintf("   Embeddings:      ✅ enabled (%s, %s, %d dims)", provider, model, dimensions), map[string]any{"Provider": provider, "Model": model, "Dimensions": dimensions})
}
func NornicDBCLIEmbeddingsDisabled() Message {
	return cliOutput(MessageNornicDBCLIEmbeddingsDisabled, "   Embeddings:      ❌ disabled (set NORNICDB_EMBEDDING_ENABLED=true or use --embedding-enabled)", nil)
}
func NornicDBCLIEmbeddingLocal(path, model string, dimensions int, gpu string) Message {
	return cliOutput(MessageNornicDBCLIEmbeddingLocal, fmt.Sprintf("   Embedding:       local GGUF (%s/%s.gguf, %d dims, GPU: %s)", path, model, dimensions, gpu), map[string]any{"Path": path, "Model": model, "Dimensions": dimensions, "GPU": gpu})
}
func NornicDBCLIEmbeddingURL(url string) Message {
	return cliOutput(MessageNornicDBCLIEmbeddingURL, "   Embedding URL:   "+url, map[string]any{"URL": url})
}
func NornicDBCLIEmbeddingModel(model string, dimensions int) Message {
	return cliOutput(MessageNornicDBCLIEmbeddingModel, fmt.Sprintf("   Embedding model: %s (%d dims)", model, dimensions), map[string]any{"Model": model, "Dimensions": dimensions})
}
func NornicDBCLIParallelEnabled(workers, batch int) Message {
	return cliOutput(MessageNornicDBCLIParallelEnabled, fmt.Sprintf("   Parallel exec:   ✅ enabled (%d workers, batch size %d)", workers, batch), map[string]any{"Workers": workers, "Batch": batch})
}
func NornicDBCLIParallelDisabled() Message {
	return cliOutput(MessageNornicDBCLIParallelDisabled, "   Parallel exec:   ❌ disabled", nil)
}
func NornicDBCLIMemoryLimit(value string) Message {
	return cliOutput(MessageNornicDBCLIMemoryLimit, "   Memory limit:    "+value, map[string]any{"Value": value})
}
func NornicDBCLIMemoryUnlimited() Message {
	return cliOutput(MessageNornicDBCLIMemoryUnlimited, "   Memory limit:    unlimited", nil)
}
func NornicDBCLIGCPercent(value int) Message {
	return cliOutput(MessageNornicDBCLIGCPercent, fmt.Sprintf("   GC percent:      %d%% (more aggressive)", value), map[string]any{"Value": value})
}
func NornicDBCLIObjectPooling() Message {
	return cliOutput(MessageNornicDBCLIObjectPooling, "   Object pooling:  ✅ enabled", nil)
}
func NornicDBCLIQueryCache(size int, ttl any) Message {
	return cliOutput(MessageNornicDBCLIQueryCache, fmt.Sprintf("   Query cache:     ✅ %d entries, TTL %v", size, ttl), map[string]any{"Size": size, "TTL": fmt.Sprint(ttl)})
}
func NornicDBCLIOpeningDatabase() Message {
	return cliOutput(MessageNornicDBCLIOpeningDatabase, "📂 Opening database...", nil)
}
func NornicDBCLIInitializingGPU() Message {
	return cliOutput(MessageNornicDBCLIInitializingGPU, "🎮 Initializing GPU acceleration...", nil)
}
func NornicDBCLIGPUUnavailable(cause error) Message {
	return cliOutput(MessageNornicDBCLIGPUUnavailable, fmt.Sprintf("   ⚠️  GPU not available: %v (using CPU)", cause), map[string]any{"Cause": cause.Error()})
}
func NornicDBCLIGPUEnabled(name, backend string, memory int) Message {
	return cliOutput(MessageNornicDBCLIGPUEnabled, fmt.Sprintf("   ✅ GPU enabled: %s (%s, %dMB)", name, backend, memory), map[string]any{"Name": name, "Backend": backend, "Memory": memory})
}
func NornicDBCLIGPUDetectedNoCUDA(name string, memory int) Message {
	return cliOutput(MessageNornicDBCLIGPUDetectedNoCUDA, fmt.Sprintf("   ⚠️  GPU detected: %s (%dMB) - CUDA not compiled in, using CPU", name, memory), map[string]any{"Name": name, "Memory": memory})
}
func NornicDBCLIBuildCUDA() Message {
	return cliOutput(MessageNornicDBCLIBuildCUDA, "      💡 Build with Dockerfile.cuda for GPU acceleration", nil)
}
func NornicDBCLIGPUDisabled() Message {
	return cliOutput(MessageNornicDBCLIGPUDisabled, "   ⚠️  GPU disabled (CPU fallback active)", nil)
}
func NornicDBCLISettingUpAuth() Message {
	return cliOutput(MessageNornicDBCLISettingUpAuth, "🔐 Setting up authentication...", nil)
}
func NornicDBCLIConfiguredJWT(bytes int) Message {
	return cliOutput(MessageNornicDBCLIConfiguredJWT, fmt.Sprintf("   Using configured JWT secret (%d bytes)", bytes), map[string]any{"Bytes": bytes})
}
func NornicDBCLINoJWT() Message {
	return cliOutput(MessageNornicDBCLINoJWT, "   ⚠️  No JWT secret configured - tokens will invalidate on restart!", nil)
}
func NornicDBCLIAdminSetupFailed() Message {
	return cliOutput(MessageNornicDBCLIAdminSetupFailed, "   ⚠️  Admin user setup failed or user already exists", nil)
}
func NornicDBCLIAdminCreated(username string) Message {
	return cliOutput(MessageNornicDBCLIAdminCreated, fmt.Sprintf("   ✅ Admin user created (%s)", username), map[string]any{"Username": username})
}
func NornicDBCLIReady() Message {
	return cliOutput(MessageNornicDBCLIReady, "✅ NornicDB is ready!", nil)
}
func NornicDBCLIEndpoints(address string, httpPort, boltPort int, metrics, pprof, mcp string, hasPprof bool) Message {
	return cliOutput(MessageNornicDBCLIEndpoints, "Endpoints:", map[string]any{"Address": address, "HTTPPort": httpPort, "BoltPort": boltPort, "Metrics": metrics, "Pprof": pprof, "MCP": mcp, "MCPEnabled": mcp != "", "PprofEnabled": hasPprof})
}
func NornicDBCLIAuthentication(username string) Message {
	return cliOutput(MessageNornicDBCLIAuthentication, "Authentication:\n  • Username: "+username+"\n  • Password: <redacted>", map[string]any{"Username": username})
}
func NornicDBCLIPressToStop() Message {
	return cliOutput(MessageNornicDBCLIPressToStop, "Press Ctrl+C to stop", nil)
}
func NornicDBCLIServerStopped() Message {
	return cliOutput(MessageNornicDBCLIServerStopped, "✅ Server stopped gracefully", nil)
}
func NornicDBCLIInitProgress(path string) Message {
	return cliOutput(MessageNornicDBCLIInitProgress, "📂 Initializing NornicDB database in "+path, map[string]any{"Path": path})
}
func NornicDBCLIInitSummary(config, data string) Message {
	return cliOutput(MessageNornicDBCLIInitSummary, fmt.Sprintf("✅ Database initialized successfully\n   Config: %s\n\nNext steps:\n  1. Start the server:  nornicdb serve --data-dir %s\n  2. Load data:         use Cypher/Bolt ingestion", config, data), map[string]any{"Config": config, "Data": data})
}
func NornicDBCLIShellOpening(path string) Message {
	return cliOutput(MessageNornicDBCLIShellOpening, "📂 Opening database at "+path+"...", map[string]any{"Path": path})
}
func NornicDBCLIShellIntro() Message {
	return cliOutput(MessageNornicDBCLIShellIntro, "✅ Connected to NornicDB\nType 'exit' or Ctrl+D to quit\nEnter Cypher queries (end with semicolon or newline):", nil)
}
func NornicDBCLIShellQueryFailed() Message {
	return cliOutput(MessageNornicDBCLIShellQueryFailed, "❌ Query execution failed", nil)
}
func NornicDBCLIShellRows(count int) Message {
	return Message{ID: MessageNornicDBCLIShellRows, Fallback: fmt.Sprintf("(%d row(s))", count), Data: map[string]any{"Count": count}, PluralCount: count}
}
func NornicDBCLIShellQuerySucceeded() Message {
	return cliOutput(MessageNornicDBCLIShellQuerySucceeded, "✅ Query executed successfully", nil)
}
func NornicDBCLIShellGoodbye() Message {
	return cliOutput(MessageNornicDBCLIShellGoodbye, "👋 Goodbye!", nil)
}
func NornicDBCLIDecayOpening(path string) Message {
	return cliOutput(MessageNornicDBCLIDecayOpening, "Opening database at "+path+"...", map[string]any{"Path": path})
}
func NornicDBCLIDecaySuppressing(threshold float64, count int) Message {
	return cliOutput(MessageNornicDBCLIDecaySuppressing, fmt.Sprintf("Suppressing nodes with score below %.4f (%d nodes to evaluate)...", threshold, count), map[string]any{"Threshold": fmt.Sprintf("%.4f", threshold), "Count": count})
}
func NornicDBCLIDecaySuppressWarning(node string, cause error) Message {
	return cliOutput(MessageNornicDBCLIDecaySuppressWarning, fmt.Sprintf("  warning: failed to suppress %s: %v", node, cause), map[string]any{"Node": node, "Cause": cause.Error()})
}
func NornicDBCLIDecaySuppressSummary(newly, already, above, total int) Message {
	return cliOutput(MessageNornicDBCLIDecaySuppressSummary, fmt.Sprintf("Suppression complete:\n  Newly suppressed:     %d\n  Already suppressed:   %d\n  Above threshold:      %d\n  Total evaluated:      %d", newly, already, above, total), map[string]any{"Newly": newly, "Already": already, "Above": above, "Total": total})
}
func NornicDBCLIDecayStatsSummary(total, suppressed, scored, noDecay int, average string) Message {
	return cliOutput(MessageNornicDBCLIDecayStatsSummary, "Decay Statistics (knowledge-layer):", map[string]any{"Total": total, "Suppressed": suppressed, "Scored": scored, "NoDecay": noDecay, "Average": average, "HasAverage": average != ""})
}
func NornicDBCLIScoreDistribution() Message {
	return cliOutput(MessageNornicDBCLIScoreDistribution, "Score distribution:", nil)
}
func NornicDBCLIPerLabelBreakdown() Message {
	return cliOutput(MessageNornicDBCLIPerLabelBreakdown, "Per-label breakdown:", nil)
}
func NornicDBCLIPerLabelRow(label string, count, eligible int) Message {
	return cliOutput(MessageNornicDBCLIPerLabelRow, fmt.Sprintf("  %-20s: %d nodes, %d suppression-eligible", label, count, eligible), map[string]any{"Label": label, "Count": count, "Eligible": eligible})
}
func NornicDBCLIShellUnavailable() Message {
	return cliOutput(MessageNornicDBCLIShellUnavailable, "cypher executor not available", nil)
}
func NornicDBCLIDecaySuppressStorage() Message {
	return cliOutput(MessageNornicDBCLIDecaySuppressStorage, "decay suppress requires BadgerEngine storage", nil)
}
func NornicDBCLIDecayStatsStorage() Message {
	return cliOutput(MessageNornicDBCLIDecayStatsStorage, "decay stats requires BadgerEngine storage", nil)
}

func cliOperationError(id MessageID, fallback string, cause error, data map[string]any) Message {
	if data == nil {
		data = make(map[string]any, 1)
	}
	data["Cause"] = cause.Error()
	return cliOutput(id, fallback, data)
}

func NornicDBCLIConfigLoadFailed(path string, cause error) Message {
	return cliOperationError(MessageNornicDBCLIConfigLoadFailed, fmt.Sprintf("failed to load config from %s: %v", path, cause), cause, map[string]any{"Path": path})
}

func NornicDBCLICreateDataDirectoryFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLICreateDataDirectoryFailed, fmt.Sprintf("creating data directory: %v", cause), cause, nil)
}

func NornicDBCLIOpenDatabaseFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLIOpenDatabaseFailed, fmt.Sprintf("opening database: %v", cause), cause, nil)
}

func NornicDBCLICreateAuthenticatorFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLICreateAuthenticatorFailed, fmt.Sprintf("creating authenticator: %v", cause), cause, nil)
}

func NornicDBCLICreateServerFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLICreateServerFailed, fmt.Sprintf("creating server: %v", cause), cause, nil)
}

func NornicDBCLIBoltTLSClientAuthFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLIBoltTLSClientAuthFailed, fmt.Sprintf("bolt tls client auth mode: %v", cause), cause, nil)
}

func NornicDBCLIBoltTLSLoadFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLIBoltTLSLoadFailed, fmt.Sprintf("bolt tls load: %v", cause), cause, nil)
}

func NornicDBCLIObservabilityInitFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLIObservabilityInitFailed, fmt.Sprintf("observability init: %v", cause), cause, nil)
}

func NornicDBCLIStartServerFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLIStartServerFailed, fmt.Sprintf("starting server: %v", cause), cause, nil)
}

func NornicDBCLITelemetryListenerFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLITelemetryListenerFailed, fmt.Sprintf("telemetry listener: %v", cause), cause, nil)
}

func NornicDBCLIPprofListenerFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLIPprofListenerFailed, fmt.Sprintf("pprof listener: %v", cause), cause, nil)
}

func NornicDBCLISupervisedRunFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLISupervisedRunFailed, fmt.Sprintf("supervised run: %v", cause), cause, nil)
}

func NornicDBCLILocalizationInitFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLILocalizationInitFailed, fmt.Sprintf("initialize localization: %v", cause), cause, nil)
}

func NornicDBCLIBoltAdapterFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLIBoltAdapterFailed, fmt.Sprintf("bolt: %v", cause), cause, nil)
}

func NornicDBCLIDatabaseNotInitialized() Message {
	return cliOutput(MessageNornicDBCLIDatabaseNotInitialized, "database is not initialized", nil)
}

func NornicDBCLITransactionNotFound() Message {
	return cliOutput(MessageNornicDBCLITransactionNotFound, "transaction not found", nil)
}

func NornicDBCLITransactionAlreadyActive() Message {
	return cliOutput(MessageNornicDBCLITransactionAlreadyActive, "transaction already active", nil)
}

func NornicDBCLICreateDirectoryFailed(path string, cause error) Message {
	return cliOperationError(MessageNornicDBCLICreateDirectoryFailed, fmt.Sprintf("creating %s: %v", path, cause), cause, map[string]any{"Path": path})
}

func NornicDBCLIWriteConfigFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLIWriteConfigFailed, fmt.Sprintf("writing config: %v", cause), cause, nil)
}

func NornicDBCLIReadInputFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLIReadInputFailed, fmt.Sprintf("reading input: %v", cause), cause, nil)
}

func NornicDBCLILoadNodesFailed(cause error) Message {
	return cliOperationError(MessageNornicDBCLILoadNodesFailed, fmt.Sprintf("loading nodes: %v", cause), cause, nil)
}
