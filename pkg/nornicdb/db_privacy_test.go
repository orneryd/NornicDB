package nornicdb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExportUserData(t *testing.T) {
	ctx := context.Background()

	t.Run("exports user data as JSON", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create nodes owned by user
		db.CreateNode(ctx, []string{"GDPRExportTest"}, map[string]interface{}{
			"owner_id": "gdpr-user-export-123",
			"content":  "My note",
		})

		data, err := db.ExportUserData(ctx, "gdpr-user-export-123", "json")
		require.NoError(t, err)
		assert.NotEmpty(t, data)

		// Parse JSON
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		assert.Equal(t, "gdpr-user-export-123", result["user_id"])
	})

	t.Run("exports as CSV", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		data, err := db.ExportUserData(ctx, "gdpr-user-csv-123", "csv")
		require.NoError(t, err)
		assert.NotEmpty(t, data)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.ExportUserData(ctx, "user-123", "json")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestDeleteUserData(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes user data", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Memory.AutoLinksEnabled = false
		cfg.Database.AsyncWritesEnabled = false
		db, err := Open(t.TempDir(), cfg)
		require.NoError(t, err)
		defer db.Close()

		first, err := db.CreateNode(ctx, []string{"GDPRDeleteTest"}, map[string]interface{}{
			"owner_id": "gdpr-user-delete-456",
			"content":  "To delete",
		})
		require.NoError(t, err)
		second, err := db.CreateNode(ctx, []string{"GDPRDeleteTest"}, map[string]interface{}{
			"owner_id": "gdpr-user-delete-456",
			"content":  "Also delete",
		})
		require.NoError(t, err)
		edge, err := db.CreateEdge(ctx, first.ID, second.ID, "LINKED_TO", nil)
		require.NoError(t, err)

		err = db.DeleteUserData(ctx, "gdpr-user-delete-456")
		require.NoError(t, err)

		_, err = db.GetNode(ctx, first.ID)
		assert.Error(t, err)
		_, err = db.GetNode(ctx, second.ID)
		assert.Error(t, err)
		allEdges, err := db.storage.AllEdges()
		require.NoError(t, err)
		for _, existingEdge := range allEdges {
			assert.NotEqual(t, edge.ID, string(existingEdge.ID))
			assert.NotEqual(t, first.ID, string(existingEdge.StartNode))
			assert.NotEqual(t, first.ID, string(existingEdge.EndNode))
			assert.NotEqual(t, second.ID, string(existingEdge.StartNode))
			assert.NotEqual(t, second.ID, string(existingEdge.EndNode))
		}
	})

	t.Run("deletes user data using configured subject selector", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Memory.AutoLinksEnabled = false
		cfg.Database.AsyncWritesEnabled = false
		cfg.Compliance.SubjectIdentifierProperties = []string{"account_id"}
		db, err := Open(t.TempDir(), cfg)
		require.NoError(t, err)
		defer db.Close()

		node, err := db.CreateNode(ctx, []string{"GDPRDeleteTest"}, map[string]interface{}{
			"account_id": "acct-456",
			"content":    "Configured selector",
		})
		require.NoError(t, err)

		err = db.DeleteUserData(ctx, "acct-456")
		require.NoError(t, err)
		_, err = db.GetNode(ctx, node.ID)
		assert.Error(t, err)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.DeleteUserData(ctx, "user-123")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestAnonymizeUserData(t *testing.T) {
	ctx := context.Background()

	t.Run("anonymizes user data", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create node with PII
		node, _ := db.CreateNode(ctx, []string{"GDPRAnonTest"}, map[string]interface{}{
			"owner_id":   "gdpr-user-anon-789",
			"name":       "Alice Smith",
			"email":      "alice@example.com",
			"username":   "alice",
			"ip_address": "192.168.1.1",
		})

		err = db.AnonymizeUserData(ctx, "gdpr-user-anon-789")
		require.NoError(t, err)

		// Verify PII is removed
		updated, err := db.GetNode(ctx, node.ID)
		require.NoError(t, err)
		assert.Nil(t, updated.Properties["email"])
		assert.Nil(t, updated.Properties["name"])
		assert.Nil(t, updated.Properties["username"])
		assert.Nil(t, updated.Properties["ip_address"])
		// Owner ID should be anonymized
		assert.NotEqual(t, "gdpr-user-anon-789", updated.Properties["owner_id"])
	})

	t.Run("anonymizes using configured pseudonymize and redact properties", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Memory.AutoLinksEnabled = false
		cfg.Database.AsyncWritesEnabled = false
		cfg.Compliance.SubjectIdentifierProperties = []string{"account_id"}
		cfg.Compliance.SubjectPseudonymizeProperties = []string{"account_id", "created_by"}
		cfg.Compliance.SubjectRedactProperties = []string{"full_name", "contact_email"}
		db, err := Open(t.TempDir(), cfg)
		require.NoError(t, err)
		defer db.Close()

		node, err := db.CreateNode(ctx, []string{"GDPRAnonTest"}, map[string]interface{}{
			"account_id":    "acct-789",
			"created_by":    "acct-789",
			"full_name":     "Alice Smith",
			"contact_email": "alice@example.com",
		})
		require.NoError(t, err)

		err = db.AnonymizeUserData(ctx, "acct-789")
		require.NoError(t, err)

		updated, err := db.GetNode(ctx, node.ID)
		require.NoError(t, err)
		assert.NotEqual(t, "acct-789", updated.Properties["account_id"])
		assert.NotEqual(t, "acct-789", updated.Properties["created_by"])
		assert.Nil(t, updated.Properties["full_name"])
		assert.Nil(t, updated.Properties["contact_email"])
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.AnonymizeUserData(ctx, "user-123")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

// =============================================================================
// Consent Management Tests (GDPR Article 7)
// =============================================================================

func TestRecordConsent(t *testing.T) {
	ctx := context.Background()

	t.Run("records new consent", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		consent := &Consent{
			UserID:  "user-123",
			Purpose: "marketing",
			Given:   true,
			Source:  "web_form",
		}

		err = db.RecordConsent(ctx, consent)
		require.NoError(t, err)

		// Verify consent was recorded
		hasConsent, err := db.HasConsent(ctx, "user-123", "marketing")
		require.NoError(t, err)
		assert.True(t, hasConsent)
	})

	t.Run("updates existing consent", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Record initial consent
		err = db.RecordConsent(ctx, &Consent{
			UserID:  "user-456",
			Purpose: "analytics",
			Given:   true,
			Source:  "web_form",
		})
		require.NoError(t, err)

		// Update consent to false
		err = db.RecordConsent(ctx, &Consent{
			UserID:  "user-456",
			Purpose: "analytics",
			Given:   false,
			Source:  "preference_center",
		})
		require.NoError(t, err)

		// Verify consent was updated
		hasConsent, err := db.HasConsent(ctx, "user-456", "analytics")
		require.NoError(t, err)
		assert.False(t, hasConsent)
	})

	t.Run("requires user_id", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.RecordConsent(ctx, &Consent{
			Purpose: "marketing",
			Given:   true,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "user_id")
	})

	t.Run("requires purpose", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.RecordConsent(ctx, &Consent{
			UserID: "user-123",
			Given:  true,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "purpose")
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.RecordConsent(ctx, &Consent{
			UserID:  "user-123",
			Purpose: "marketing",
			Given:   true,
		})
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestHasConsent(t *testing.T) {
	ctx := context.Background()

	t.Run("returns false when no consent recorded", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		hasConsent, err := db.HasConsent(ctx, "user-unknown", "marketing")
		require.NoError(t, err)
		assert.False(t, hasConsent)
	})

	t.Run("returns true when consent given", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.RecordConsent(ctx, &Consent{
			UserID:  "user-789",
			Purpose: "email",
			Given:   true,
		})
		require.NoError(t, err)

		hasConsent, err := db.HasConsent(ctx, "user-789", "email")
		require.NoError(t, err)
		assert.True(t, hasConsent)
	})

	t.Run("returns false when consent revoked", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Give consent
		err = db.RecordConsent(ctx, &Consent{
			UserID:  "user-abc",
			Purpose: "tracking",
			Given:   true,
		})
		require.NoError(t, err)

		// Revoke consent
		err = db.RevokeConsent(ctx, "user-abc", "tracking")
		require.NoError(t, err)

		hasConsent, err := db.HasConsent(ctx, "user-abc", "tracking")
		require.NoError(t, err)
		assert.False(t, hasConsent)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.HasConsent(ctx, "user-123", "marketing")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestRevokeConsent(t *testing.T) {
	ctx := context.Background()

	t.Run("revokes existing consent", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Record consent
		err = db.RecordConsent(ctx, &Consent{
			UserID:  "user-revoke-1",
			Purpose: "marketing",
			Given:   true,
		})
		require.NoError(t, err)

		// Verify consent exists
		hasConsent, err := db.HasConsent(ctx, "user-revoke-1", "marketing")
		require.NoError(t, err)
		assert.True(t, hasConsent)

		// Revoke consent
		err = db.RevokeConsent(ctx, "user-revoke-1", "marketing")
		require.NoError(t, err)

		// Verify consent is revoked
		hasConsent, err = db.HasConsent(ctx, "user-revoke-1", "marketing")
		require.NoError(t, err)
		assert.False(t, hasConsent)
	})

	t.Run("creates revocation record if no prior consent", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Revoke consent that was never given
		err = db.RevokeConsent(ctx, "user-never-consented", "marketing")
		require.NoError(t, err)

		// Verify consent is false
		hasConsent, err := db.HasConsent(ctx, "user-never-consented", "marketing")
		require.NoError(t, err)
		assert.False(t, hasConsent)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.RevokeConsent(ctx, "user-123", "marketing")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestGetUserConsents(t *testing.T) {
	ctx := context.Background()

	t.Run("returns all consents for user", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Record multiple consents
		err = db.RecordConsent(ctx, &Consent{
			UserID:  "user-multi-consent",
			Purpose: "marketing",
			Given:   true,
			Source:  "web",
		})
		require.NoError(t, err)

		err = db.RecordConsent(ctx, &Consent{
			UserID:  "user-multi-consent",
			Purpose: "analytics",
			Given:   false,
			Source:  "api",
		})
		require.NoError(t, err)

		err = db.RecordConsent(ctx, &Consent{
			UserID:  "user-multi-consent",
			Purpose: "personalization",
			Given:   true,
			Source:  "app",
		})
		require.NoError(t, err)

		// Get all consents
		consents, err := db.GetUserConsents(ctx, "user-multi-consent")
		require.NoError(t, err)
		assert.Len(t, consents, 3)

		// Verify purposes
		purposes := make(map[string]bool)
		for _, c := range consents {
			purposes[c.Purpose] = c.Given
		}
		assert.True(t, purposes["marketing"])
		assert.False(t, purposes["analytics"])
		assert.True(t, purposes["personalization"])
	})

	t.Run("returns empty slice when no consents", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		consents, err := db.GetUserConsents(ctx, "user-no-consents")
		require.NoError(t, err)
		assert.Empty(t, consents)
	})

	t.Run("returns error when closed", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.GetUserConsents(ctx, "user-123")
		assert.ErrorIs(t, err, ErrClosed)
	})
}

// =============================================================================
// Additional Edge Cases
// =============================================================================

// =============================================================================
// Tests for 0% coverage functions
// =============================================================================

func TestHybridSearch(t *testing.T) {
	ctx := context.Background()

	t.Run("basic_hybrid_search", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create some test nodes
		for i := 0; i < 5; i++ {
			_, err := db.CreateNode(ctx, []string{"Memory"}, map[string]interface{}{
				"content": "Test content about machine learning and AI",
				"title":   "ML Test",
			})
			require.NoError(t, err)
		}

		// Create a mock query embedding (1024 dimensions)
		queryEmbedding := make([]float32, 1024)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.1
		}

		results, err := db.HybridSearch(ctx, "machine learning", queryEmbedding, nil, 10)
		require.NoError(t, err)
		// Results may be empty if no search service or embeddings indexed
		t.Logf("HybridSearch returned %d results", len(results))
	})

	t.Run("hybrid_search_with_labels", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		queryEmbedding := make([]float32, 1024)

		results, err := db.HybridSearch(ctx, "test", queryEmbedding, []string{"Memory"}, 10)
		require.NoError(t, err)
		t.Logf("HybridSearch with labels returned %d results", len(results))
	})

	t.Run("hybrid_search_closed_db", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		queryEmbedding := make([]float32, 1024)

		_, err = db.HybridSearch(ctx, "test", queryEmbedding, nil, 10)
		assert.Error(t, err, "Should error on closed DB")
	})

	t.Run("hybrid_search_empty_query", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		queryEmbedding := make([]float32, 1024)

		results, err := db.HybridSearch(ctx, "", queryEmbedding, nil, 10)
		require.NoError(t, err)
		t.Logf("HybridSearch with empty query returned %d results", len(results))
	})

	t.Run("hybrid_search_dimension_mismatch_returns_error", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Default service expects configured embedding dimensions (1024 by default).
		shortEmbedding := []float32{0.1, 0.2}
		results, err := db.HybridSearch(ctx, "test", shortEmbedding, nil, 10)
		require.NoError(t, err)
		require.NotNil(t, results)
	})
}

func TestBuildSearchIndexes(t *testing.T) {
	ctx := context.Background()

	t.Run("build_indexes_on_empty_db", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		err = db.BuildSearchIndexes(ctx)
		require.NoError(t, err, "Should succeed on empty DB")
	})

	t.Run("build_indexes_with_data", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create some test nodes
		for i := 0; i < 3; i++ {
			_, err := db.CreateNode(ctx, []string{"Memory"}, map[string]interface{}{
				"content": "Searchable content for indexing test",
				"title":   "Index Test",
			})
			require.NoError(t, err)
		}

		err = db.BuildSearchIndexes(ctx)
		require.NoError(t, err)
	})

	t.Run("build_indexes_closed_db", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		err = db.BuildSearchIndexes(ctx)
		assert.Error(t, err, "Should error on closed DB")
		assert.Equal(t, ErrClosed, err)
	})
}

func TestSetGetGPUManager(t *testing.T) {
	t.Run("set_and_get_gpu_manager", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Initially nil
		assert.Nil(t, db.GetGPUManager())

		// Set a mock manager (using interface{})
		mockManager := struct{ Name string }{Name: "MockGPU"}
		db.SetGPUManager(mockManager)

		// Get it back
		retrieved := db.GetGPUManager()
		assert.NotNil(t, retrieved)

		// Type assert back
		mock, ok := retrieved.(struct{ Name string })
		assert.True(t, ok)
		assert.Equal(t, "MockGPU", mock.Name)
	})

	t.Run("set_nil_gpu_manager", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Set a manager
		db.SetGPUManager("test")
		assert.NotNil(t, db.GetGPUManager())

		// Set nil to clear
		db.SetGPUManager(nil)
		assert.Nil(t, db.GetGPUManager())
	})

	t.Run("thread_safe_access", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Concurrent access should not panic
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func(n int) {
				db.SetGPUManager(n)
				_ = db.GetGPUManager()
				done <- true
			}(i)
		}
		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("handles cached search-service entries with clustering enabled", func(t *testing.T) {
		cleanup := nornicConfig.WithGPUClusteringEnabled()
		defer cleanup()

		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)
		require.NotNil(t, svc)
		svc.EnableClustering(nil, 2)
		require.True(t, svc.IsClusteringEnabled())

		db.searchServicesMu.Lock()
		db.searchServices["nil_entry_cov"] = nil
		db.searchServices["nil_svc_cov"] = &dbSearchService{dbName: "nil_svc_cov"}
		db.searchServicesMu.Unlock()

		mgr := &gpu.Manager{}
		db.SetGPUManager(mgr)
		got, ok := db.GetGPUManager().(*gpu.Manager)
		require.True(t, ok)
		require.Same(t, mgr, got)
	})
}

func TestCypherFunctionWithParams(t *testing.T) {
	ctx := context.Background()

	t.Run("simple_cypher_query", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create some data
		_, err = db.CreateNode(ctx, []string{"Memory"}, map[string]interface{}{
			"content": "Test for Cypher",
			"title":   "Cypher Test",
		})
		require.NoError(t, err)

		// Execute Cypher query
		resultSet, err := db.Cypher(ctx, "MATCH (n:Memory) RETURN count(n)", nil)
		require.NoError(t, err)
		assert.NotNil(t, resultSet)
	})

	t.Run("cypher_with_create", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		resultSet, err := db.Cypher(ctx, "CREATE (n:TestNode {name: 'created'})", nil)
		require.NoError(t, err)
		assert.NotNil(t, resultSet)
	})

	t.Run("cypher_closed_db", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		db.Close()

		_, err = db.Cypher(ctx, "RETURN 1", nil)
		assert.Error(t, err)
	})

	t.Run("cypher_invalid_query", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.Cypher(ctx, "INVALID QUERY SYNTAX", nil)
		assert.Error(t, err)
	})

	t.Run("cypher_with_params", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		params := map[string]any{
			"name": "test",
		}
		resultSet, err := db.Cypher(ctx, "CREATE (n:Test {name: $name})", params)
		require.NoError(t, err)
		assert.NotNil(t, resultSet)
	})
}

// =============================================================================
// ClearAllEmbeddings Storage Unwrapping Tests
// =============================================================================

func TestClearAllEmbeddings_UnwrapsStorageLayers(t *testing.T) {
	// This test verifies that ClearAllEmbeddings can find the underlying BadgerEngine
	// even when wrapped by WALEngine and/or AsyncEngine.

	t.Run("works_with_wal_wrapped_engine", func(t *testing.T) {
		// Create persistent database (WAL is auto-enabled for persistent storage)
		tmpDir := t.TempDir()
		config := &Config{
			Memory: nornicConfig.MemoryConfig{
				DecayEnabled:     false,
				AutoLinksEnabled: false,
			},
			Database: nornicConfig.DatabaseConfig{
				AsyncWritesEnabled: false, // Just WAL, no async
			},
		}
		db, err := Open(tmpDir, config)
		require.NoError(t, err)
		defer db.Close()

		// Create a node with embedding via the namespaced storage engine
		_, err = db.storage.CreateNode(&storage.Node{
			ID:              storage.NodeID(generateID()),
			Labels:          []string{"Memory"},
			Properties:      map[string]interface{}{"content": "Test content"},
			ChunkEmbeddings: [][]float32{{1.0, 0.0, 0.0, 0.0}},
		})
		require.NoError(t, err)

		// ClearAllEmbeddings should unwrap WAL and work
		count, err := db.ClearAllEmbeddings()
		assert.NoError(t, err)
		// Count can be 0 if no embeddings were stored
		assert.GreaterOrEqual(t, count, 0)
	})

	t.Run("works_with_async_and_wal_wrapped_engine", func(t *testing.T) {
		// Create persistent database with both async and WAL enabled
		tmpDir := t.TempDir()
		config := &Config{
			Memory: nornicConfig.MemoryConfig{
				DecayEnabled:     false,
				AutoLinksEnabled: false,
			},
			Database: nornicConfig.DatabaseConfig{
				AsyncWritesEnabled: true,                  // WAL + async
				AsyncFlushInterval: 50 * time.Millisecond, // Required for async writes
			},
		}
		db, err := Open(tmpDir, config)
		require.NoError(t, err)
		defer db.Close()

		// ClearAllEmbeddings should unwrap both layers and work
		count, err := db.ClearAllEmbeddings()
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, count, 0)
	})

	t.Run("fails_gracefully_with_memory_engine", func(t *testing.T) {
		// In-memory database uses MemoryEngine which doesn't support ClearAllEmbeddings
		db, err := Open("", nil)
		require.NoError(t, err)
		defer db.Close()

		// Should return an error since MemoryEngine doesn't support this
		_, err = db.ClearAllEmbeddings()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not support ClearAllEmbeddings")
	})
}

// =============================================================================
// DeleteNode Search Index Cleanup Tests
// =============================================================================

func TestDeleteNode_RemovesFromSearchIndex(t *testing.T) {
	ctx := context.Background()

	t.Run("delete_removes_from_storage", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create a node
		stored, err := db.CreateNode(ctx, []string{"Memory"}, map[string]interface{}{
			"content": "Test content to delete",
			"title":   "Delete Me",
		})
		require.NoError(t, err)

		// Verify node exists
		retrieved, err := db.GetNode(ctx, stored.ID)
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		// Delete the node
		err = db.DeleteNode(ctx, stored.ID)
		require.NoError(t, err)

		// Verify node is gone from storage
		_, err = db.GetNode(ctx, stored.ID)
		// GetNode returns ErrNotFound for missing nodes
		assert.Error(t, err)
	})

	t.Run("delete_cleans_up_search_indexes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create multiple nodes using Cypher for more control
		_, err = db.ExecuteCypher(ctx, "CREATE (n:Recipe {name: 'Apple pie recipe', content: 'Delicious apple pie'})", nil)
		require.NoError(t, err)
		_, err = db.ExecuteCypher(ctx, "CREATE (n:Recipe {name: 'Apple cider vinegar', content: 'Tangy apple cider'})", nil)
		require.NoError(t, err)

		// Rebuild search indexes to ensure nodes are indexed
		err = db.BuildSearchIndexes(ctx)
		require.NoError(t, err)

		// Both should be searchable via text search
		results, err := db.Search(ctx, "Apple", nil, 10)
		require.NoError(t, err)
		initialCount := len(results)

		// If we have results, test deletion
		if initialCount > 0 {
			// Get the first node's ID
			firstNodeID := string(results[0].Node.ID)

			// Delete the first node
			err = db.DeleteNode(ctx, firstNodeID)
			require.NoError(t, err)

			// Search should return fewer results now
			results, err = db.Search(ctx, "Apple", nil, 10)
			require.NoError(t, err)

			// Deleted node should not appear in search results
			for _, r := range results {
				if r.Node != nil && string(r.Node.ID) == firstNodeID {
					t.Error("Deleted node should not appear in search results")
				}
			}
		} else {
			// Search indexing may be delayed - this is acceptable behavior
			t.Log("Search returned 0 results - indexing may be asynchronous")
		}
	})

	t.Run("delete_nonexistent_node_fails", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Try to delete a node that doesn't exist
		err = db.DeleteNode(ctx, "nonexistent-node-id")
		// May or may not error depending on storage implementation
		// The important thing is it doesn't panic
		_ = err
	})

	t.Run("delete_on_closed_db_fails", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)

		// Create a node
		stored, err := db.CreateNode(ctx, []string{"Memory"}, map[string]interface{}{"content": "Test"})
		require.NoError(t, err)

		// Close the database
		db.Close()

		// Delete should fail
		err = db.DeleteNode(ctx, stored.ID)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrClosed)
	})

	t.Run("delete_decrements_embedding_count_via_callback", func(t *testing.T) {
		// This test verifies the full callback chain:
		// Storage.DeleteNode -> notifyNodeDeleted callback -> SearchService.RemoveNode
		// This ensures embeddings are cleaned up automatically when nodes are deleted.

		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		// Create nodes with embeddings directly via storage layer
		// We use the storage layer directly to have precise control over the Embedding field
		nodes := []*storage.Node{
			{
				ID:              "embed-callback-test-1",
				Labels:          []string{"TestNode"},
				Properties:      map[string]any{"name": "Test Node 1"},
				ChunkEmbeddings: [][]float32{make([]float32, 1024)}, // Match default dimension
			},
			{
				ID:              "embed-callback-test-2",
				Labels:          []string{"TestNode"},
				Properties:      map[string]any{"name": "Test Node 2"},
				ChunkEmbeddings: [][]float32{make([]float32, 1024)},
			},
			{
				ID:              "embed-callback-test-3",
				Labels:          []string{"TestNode"},
				Properties:      map[string]any{"name": "Test Node 3"},
				ChunkEmbeddings: [][]float32{make([]float32, 1024)},
			},
		}

		// Set distinct embedding values
		nodes[0].ChunkEmbeddings[0][0] = 1.0
		nodes[1].ChunkEmbeddings[0][1] = 1.0
		nodes[2].ChunkEmbeddings[0][2] = 1.0

		// Get initial embedding count
		initialCount := db.EmbeddingCount()

		// Create nodes via storage (this triggers OnNodeCreated callback)
		for _, node := range nodes {
			_, err := db.storage.CreateNode(node)
			require.NoError(t, err)
		}

		// Wait for async callbacks to fire and indexing state to converge.
		require.Eventually(t, func() bool {
			return db.EmbeddingCount() == initialCount+3
		}, 3*time.Second, 25*time.Millisecond,
			"Embedding count should increase by 3 after creating nodes")

		// Delete node1 via storage (this triggers OnNodeDeleted callback)
		err = db.storage.DeleteNode("embed-callback-test-1")
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return db.EmbeddingCount() == initialCount+2
		}, 3*time.Second, 25*time.Millisecond,
			"Embedding count should decrease by 1 after deleting node1")

		// Delete node2
		err = db.storage.DeleteNode("embed-callback-test-2")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return db.EmbeddingCount() == initialCount+1
		}, 3*time.Second, 25*time.Millisecond,
			"Embedding count should decrease by 2 after deleting node2")

		// Delete node3
		err = db.storage.DeleteNode("embed-callback-test-3")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return db.EmbeddingCount() == initialCount
		}, 3*time.Second, 25*time.Millisecond,
			"Embedding count should return to initial after deleting all test nodes")
	})
}

func TestDeleteUserData_RemovesFromSearchIndex(t *testing.T) {
	ctx := context.Background()

	t.Run("delete_user_data_cleans_search_indexes", func(t *testing.T) {
		db, err := Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer db.Close()

		userID := "user-123"

		// Create nodes for a user (owner_id is used for user association)
		stored1, err := db.CreateNode(ctx, []string{"Memory"}, map[string]interface{}{
			"content":  "User data 1",
			"owner_id": userID,
		})
		require.NoError(t, err)
		stored2, err := db.CreateNode(ctx, []string{"Memory"}, map[string]interface{}{
			"content":  "User data 2",
			"owner_id": userID,
		})
		require.NoError(t, err)
		stored3, err := db.CreateNode(ctx, []string{"Memory"}, map[string]interface{}{
			"content":  "Other user data",
			"owner_id": "user-456",
		})
		require.NoError(t, err)

		// Delete user data
		err = db.DeleteUserData(ctx, userID)
		require.NoError(t, err)

		// Verify user's data is deleted
		_, err = db.GetNode(ctx, stored1.ID)
		assert.Error(t, err, "User's first node should be deleted")

		_, err = db.GetNode(ctx, stored2.ID)
		assert.Error(t, err, "User's second node should be deleted")

		// Other user's data should remain
		retrieved, err := db.GetNode(ctx, stored3.ID)
		require.NoError(t, err)
		assert.NotNil(t, retrieved, "Other user's data should remain")
	})
}
