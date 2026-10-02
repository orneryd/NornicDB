package cypher

import (
	"context"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBug_SchemaCommandsWithAsyncEngine reproduces the bug where CREATE CONSTRAINT
// and CREATE INDEX queries fail with "invalid label name: Node)" when an AsyncEngine
// is active (i.e. production deployments).
//
// Root cause: tryAsyncCreateNodeBatch intercepts all queries starting with CREATE
// before the schema handler runs. Without an async engine (unit tests / MemoryEngine)
// the early-return at "if engines.asyncEngine != nil" skips tryAsyncCreateNodeBatch,
// masking the bug. In production the async engine is always active, causing schema
// commands to be parsed as node CREATE patterns.
//
// Reported via: integration testing — schema
// initialization failed on every startup when NornicDB was backed by an AsyncEngine.
//
// Environment: macOS 15 (Apple Silicon M1), NornicDB arm64-metal-bge-heimdall image,
// MCP server integration, Docker with Colima (aarch64, VZ).
func TestBug_SchemaCommandsWithAsyncEngine(t *testing.T) {
	// Setup full production-equivalent stack: BadgerEngine -> WALEngine -> AsyncEngine.
	// This is the critical difference from MemoryEngine-based tests — the AsyncEngine
	// being present is what triggers tryAsyncCreateNodeBatch and exposes the bug.
	tmpDir, err := os.MkdirTemp("", "schema_async_routing_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	badger, err := storage.NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badger.Close()

	wal, err := storage.NewWAL(tmpDir+"/wal", nil)
	require.NoError(t, err)
	defer wal.Close()

	walEngine := storage.NewWALEngine(badger, wal)
	asyncEngine := storage.NewAsyncEngine(walEngine, nil)
	defer asyncEngine.Close()

	store := storage.NewNamespacedEngine(asyncEngine, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// These are the exact schema queries sent during GraphManager
	// initialization (pkg/managers/GraphManager.ts). Before the fix each query
	// returned: invalid label name: "Node)" (must be alphanumeric starting with
	// letter or underscore) because tryAsyncCreateNodeBatch parsed FOR (n:Node)
	// as a node CREATE pattern and captured "Node)" as the label.
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "CREATE TEXT INDEX ti",
			query: "CREATE TEXT INDEX ti FOR (n:P) ON (n.name)",
		},
		{
			name:  "CREATE POINT INDEX pi",
			query: "CREATE POINT INDEX pi FOR (n:P) ON (n.loc)",
		},
		{
			name: "CREATE CONSTRAINT node_id_unique",
			query: `
				CREATE CONSTRAINT node_id_unique IF NOT EXISTS
				FOR (n:Node) REQUIRE n.id IS UNIQUE
			`,
		},
		{
			name: "CREATE FULLTEXT INDEX node_search",
			query: `
				CREATE FULLTEXT INDEX node_search IF NOT EXISTS
				FOR (n:Node) ON EACH [n.properties]
			`,
		},
		{
			name: "CREATE INDEX node_type",
			query: `
				CREATE INDEX node_type IF NOT EXISTS
				FOR (n:Node) ON (n.type)
			`,
		},
		{
			name: "CREATE CONSTRAINT watch_config_id_unique",
			query: `
				CREATE CONSTRAINT watch_config_id_unique IF NOT EXISTS
				FOR (w:WatchConfig) REQUIRE w.id IS UNIQUE
			`,
		},
		{
			name: "CREATE INDEX watch_config_path",
			query: `
				CREATE INDEX watch_config_path IF NOT EXISTS
				FOR (w:WatchConfig) ON (w.path)
			`,
		},
		{
			name: "CREATE INDEX file_path",
			query: `
				CREATE INDEX file_path IF NOT EXISTS
				FOR (f:File) ON (f.path)
			`,
		},
		{
			name: "CREATE FULLTEXT INDEX file_metadata_search",
			query: `
				CREATE FULLTEXT INDEX file_metadata_search IF NOT EXISTS
				FOR (f:File) ON EACH [f.path, f.name, f.language]
			`,
		},
		{
			name: "CREATE VECTOR INDEX node_embedding_index",
			query: `
				CREATE VECTOR INDEX node_embedding_index IF NOT EXISTS
				FOR (n:Node) ON (n.embedding)
				OPTIONS {indexConfig: {vector.dimensions: 1024}}
			`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, tt.query, nil)
			assert.NoError(t, err, "schema command should succeed with async engine active")
		})
	}
	indexes, err := exec.Execute(ctx, "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties WHERE name IN ['ti', 'pi'] RETURN name, type, entityType, labelsOrTypes, properties ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"pi", "POINT", "NODE", []string{"P"}, []string{"loc"}},
		{"ti", "TEXT", "NODE", []string{"P"}, []string{"name"}},
	}, indexes.Rows)
	nodes, err := exec.Execute(ctx, "MATCH (n:P) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, nodes.Rows)
}
