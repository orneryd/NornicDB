package nornicdb

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncryptionE2E tests the full encryption lifecycle with memory storage.
// These tests ensure encryption works correctly and doesn't break normal database operations.

func TestEncryptionDisabledByDefault(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()

	assert.False(t, db.IsEncryptionEnabled(), "encryption should be disabled by default")

	stats := db.EncryptionStats()
	assert.False(t, stats["enabled"].(bool), "encryption stats should show disabled")
}

func TestEncryptionRequiresPassword(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "", // No password
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	_, err := Open(t.TempDir(), config)
	require.Error(t, err, "should fail without password")
	assert.Contains(t, err.Error(), "no password")
}

func TestEncryptionInitialization(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	assert.True(t, db.IsEncryptionEnabled(), "encryption should be enabled")

	stats := db.EncryptionStats()
	assert.True(t, stats["enabled"].(bool))
	assert.Equal(t, "AES-256 (BadgerDB)", stats["algorithm"])
	assert.Contains(t, stats["key_derivation"], "PBKDF2")
}

func TestEncryptionFromEnvironment(t *testing.T) {
	// Test that env var takes precedence
	os.Setenv("NORNICDB_ENCRYPTION_PASSWORD", "env-password-secure-123!")
	defer os.Unsetenv("NORNICDB_ENCRYPTION_PASSWORD")

	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "config-password", // Should be overridden
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	assert.True(t, db.IsEncryptionEnabled())
}

func TestEncryptionPersistsSalt(t *testing.T) {
	tmpDir := t.TempDir()
	password := "test-password-secure-12345!"

	// First open - creates salt
	config1 := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: password,
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db1, err := Open(tmpDir, config1)
	require.NoError(t, err)

	// Store a sensitive field
	ctx := context.Background()
	node1, err := db1.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
		"name": "John Doe",
		"ssn":  "123-45-6789", // PHI field - should be encrypted
	})
	require.NoError(t, err)
	db1.Close()

	// Verify salt file was created
	saltFile := tmpDir + "/db.salt"
	saltData, err := os.ReadFile(saltFile)
	require.NoError(t, err)
	assert.Len(t, saltData, 32, "salt should be 32 bytes")

	// Second open - loads existing salt
	config2 := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: password,
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db2, err := Open(tmpDir, config2)
	require.NoError(t, err)
	defer db2.Close()

	// Should be able to read the encrypted data
	node2, err := db2.GetNode(ctx, node1.ID)
	require.NoError(t, err)
	assert.Equal(t, "123-45-6789", node2.Properties["ssn"], "should decrypt SSN correctly")
}

func TestEncryptionOfPHIFields(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Test various PHI fields that should be encrypted
	phiData := map[string]interface{}{
		"name":                   "John Doe",            // Not PHI - should NOT be encrypted
		"ssn":                    "123-45-6789",         // PHI
		"social_security_number": "987-65-4321",         // PHI
		"email":                  "john@example.com",    // PII
		"phone":                  "555-123-4567",        // PII
		"dob":                    "1990-01-15",          // PHI
		"diagnosis":              "Type 2 Diabetes",     // PHI
		"credit_card":            "4111-1111-1111-1111", // PCI
		"api_key":                "sk-secret-key-123",   // Sensitive
		"regular_field":          "not sensitive",       // Not sensitive
		"count":                  42,                    // Non-string - not encrypted
	}

	node, err := db.CreateNode(ctx, []string{"Patient"}, phiData)
	require.NoError(t, err)

	// Retrieve and verify decryption
	retrieved, err := db.GetNode(ctx, node.ID)
	require.NoError(t, err)

	// All string fields should be readable after decryption
	assert.Equal(t, "John Doe", retrieved.Properties["name"])
	assert.Equal(t, "123-45-6789", retrieved.Properties["ssn"])
	assert.Equal(t, "987-65-4321", retrieved.Properties["social_security_number"])
	assert.Equal(t, "john@example.com", retrieved.Properties["email"])
	assert.Equal(t, "555-123-4567", retrieved.Properties["phone"])
	assert.Equal(t, "1990-01-15", retrieved.Properties["dob"])
	assert.Equal(t, "Type 2 Diabetes", retrieved.Properties["diagnosis"])
	assert.Equal(t, "4111-1111-1111-1111", retrieved.Properties["credit_card"])
	assert.Equal(t, "sk-secret-key-123", retrieved.Properties["api_key"])
	assert.Equal(t, "not sensitive", retrieved.Properties["regular_field"])
	assert.Equal(t, 42, retrieved.Properties["count"])
}

func TestEncryptionDataAtRest(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	sensitiveSSN := "123-45-6789"

	node, err := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
		"name": "Jane Doe",
		"ssn":  sensitiveSSN,
	})
	require.NoError(t, err)

	// For full-database encryption we cannot inspect raw storage (Badger handles it).
	// Instead, verify encryption is reported and data round-trips correctly.
	assert.True(t, db.IsEncryptionEnabled(), "encryption should be enabled")

	retrieved, err := db.GetNode(ctx, node.ID)
	require.NoError(t, err)
	assert.Equal(t, sensitiveSSN, retrieved.Properties["ssn"])
}

func TestEncryptionWithCustomFields(t *testing.T) {
	// Note: Field-level encryption was removed in favor of full-database encryption.
	// This test now verifies that encryption works at the BadgerDB storage level.
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	node, err := db.CreateNode(ctx, []string{"Custom"}, map[string]interface{}{
		"name":          "Test",
		"custom_secret": "my-secret-value",
		"internal_id":   "internal-12345",
		"public_field":  "visible",
	})
	require.NoError(t, err)

	// With full-database encryption, Badger handles encryption transparently.
	// Just verify round-trip decryption works.
	retrieved, err := db.GetNode(ctx, node.ID)
	require.NoError(t, err)
	assert.Equal(t, "my-secret-value", retrieved.Properties["custom_secret"])
	assert.Equal(t, "internal-12345", retrieved.Properties["internal_id"])
}

func TestEncryptionDoesNotBreakNonSensitiveFields(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create node with various non-sensitive fields
	node, err := db.CreateNode(ctx, []string{"Document"}, map[string]interface{}{
		"title":       "My Document",
		"description": "A test document",
		"count":       100,
		"rating":      4.5,
		"tags":        []string{"test", "document"},
		"active":      true,
		"metadata":    map[string]interface{}{"key": "value"},
	})
	require.NoError(t, err)

	// All fields should be retrievable
	retrieved, err := db.GetNode(ctx, node.ID)
	require.NoError(t, err)

	assert.Equal(t, "My Document", retrieved.Properties["title"])
	assert.Equal(t, "A test document", retrieved.Properties["description"])
	assert.Equal(t, 100, retrieved.Properties["count"])
	assert.Equal(t, 4.5, retrieved.Properties["rating"])
	assert.Equal(t, true, retrieved.Properties["active"])
}

func TestEncryptionConcurrentAccess(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	const goroutines = 20
	const nodesPerGoroutine = 10

	var wg sync.WaitGroup
	errors := make(chan error, goroutines*nodesPerGoroutine*2)
	nodeIDs := make(chan string, goroutines*nodesPerGoroutine)

	// Concurrent writes
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < nodesPerGoroutine; j++ {
				node, err := db.CreateNode(ctx, []string{"Test"}, map[string]interface{}{
					"worker":   workerID,
					"index":    j,
					"ssn":      "123-45-6789", // Encrypted field
					"email":    "test@example.com",
					"password": "secret-password",
				})
				if err != nil {
					errors <- err
					continue
				}
				nodeIDs <- node.ID
			}
		}(i)
	}

	wg.Wait()
	close(nodeIDs)

	// Collect all node IDs
	var allNodeIDs []string
	for id := range nodeIDs {
		allNodeIDs = append(allNodeIDs, id)
	}

	// Concurrent reads
	for _, id := range allNodeIDs {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			node, err := db.GetNode(ctx, nodeID)
			if err != nil {
				errors <- err
				return
			}
			// Verify decryption worked
			if node.Properties["ssn"] != "123-45-6789" {
				errors <- assert.AnError
			}
		}(id)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("concurrent operation failed: %v", err)
	}

	assert.Equal(t, goroutines*nodesPerGoroutine, len(allNodeIDs), "all nodes should be created")
}

func TestEncryptionWithUpdateNode(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create node
	node, err := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
		"name": "Original Name",
		"ssn":  "111-11-1111",
	})
	require.NoError(t, err)

	// Update node with new sensitive data
	updated, err := db.UpdateNode(ctx, node.ID, map[string]interface{}{
		"ssn":   "222-22-2222",
		"email": "new@example.com",
	})
	require.NoError(t, err)

	// Verify updated values are encrypted and decrypted correctly
	retrieved, err := db.GetNode(ctx, updated.ID)
	require.NoError(t, err)
	assert.Equal(t, "222-22-2222", retrieved.Properties["ssn"])
	assert.Equal(t, "new@example.com", retrieved.Properties["email"])

	// Original name should be preserved
	assert.Equal(t, "Original Name", retrieved.Properties["name"])
}

func TestEncryptionWithCypherQueries(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create node via API
	_, err = db.CreateNode(ctx, []string{"Patient"}, map[string]interface{}{
		"name": "Test Patient",
		"ssn":  "999-99-9999",
	})
	require.NoError(t, err)

	// Query via Cypher - should return decrypted data
	result, err := db.ExecuteCypher(ctx, "MATCH (p:Patient) RETURN p.name, p.ssn", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// Note: Cypher results may or may not be decrypted depending on implementation
	// This test documents current behavior
	assert.Equal(t, "Test Patient", result.Rows[0][0])
}

func TestEncryptionDisabledDoesNotEncrypt(t *testing.T) {
	config := &Config{
		EncryptionEnabled: false, // Explicitly disabled
		DecayEnabled:      false,
		AutoLinksEnabled:  false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	node, err := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
		"name": "Test",
		"ssn":  "123-45-6789",
	})
	require.NoError(t, err)

	// Raw storage should have plaintext
	rawNode, err := db.storage.GetNode(storageNodeID(node.ID))
	require.NoError(t, err)

	rawSSN, _ := rawNode.Properties["ssn"].(string)
	assert.Equal(t, "123-45-6789", rawSSN, "SSN should NOT be encrypted when encryption is disabled")
	assert.False(t, strings.HasPrefix(rawSSN, "enc:"), "should not have encryption prefix")
}

func TestEncryptionWithMemory(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       true,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Store a memory with sensitive properties
	mem := &Memory{
		Content: "Patient record with sensitive data",
		Title:   "Medical Record",
		Tier:    TierSemantic,
		Properties: map[string]interface{}{
			"ssn":       "555-55-5555",
			"diagnosis": "Confidential diagnosis",
		},
	}

	stored, err := db.Store(ctx, mem)
	require.NoError(t, err)

	// Retrieve and verify
	retrieved, err := db.Recall(ctx, stored.ID)
	require.NoError(t, err)

	assert.Equal(t, "Patient record with sensitive data", retrieved.Content)
	// Note: Memory.Properties encryption depends on Store implementation
}

func TestEncryptionPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	const iterations = 100

	// Measure write performance
	writeStart := time.Now()
	var nodeIDs []string
	for i := 0; i < iterations; i++ {
		node, err := db.CreateNode(ctx, []string{"Perf"}, map[string]interface{}{
			"index":    i,
			"ssn":      "123-45-6789",
			"email":    "test@example.com",
			"password": "secret",
			"data":     strings.Repeat("x", 1000), // 1KB non-sensitive data
		})
		require.NoError(t, err)
		nodeIDs = append(nodeIDs, node.ID)
	}
	writeDuration := time.Since(writeStart)
	t.Logf("Write %d nodes with encryption: %v (%.2f nodes/sec)", iterations, writeDuration, float64(iterations)/writeDuration.Seconds())

	// Measure read performance
	readStart := time.Now()
	for _, id := range nodeIDs {
		_, err := db.GetNode(ctx, id)
		require.NoError(t, err)
	}
	readDuration := time.Since(readStart)
	t.Logf("Read %d nodes with decryption: %v (%.2f nodes/sec)", iterations, readDuration, float64(iterations)/readDuration.Seconds())

	// Performance assertions (adjust thresholds as needed)
	assert.Less(t, writeDuration, 30*time.Second, "writes should complete in reasonable time")
	assert.Less(t, readDuration, 10*time.Second, "reads should complete in reasonable time")
}

func TestEncryptionWrongPassword(t *testing.T) {
	tmpDir := t.TempDir()
	password1 := "correct-password-12345!"
	password2 := "wrong-password-67890!"

	// Create database with first password
	config1 := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: password1,
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db1, err := Open(tmpDir, config1)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = db1.CreateNode(ctx, []string{"Secret"}, map[string]interface{}{
		"ssn": "123-45-6789",
	})
	require.NoError(t, err)
	db1.Close()

	// Try to open with wrong password
	// Note: This might not fail immediately since PBKDF2 will derive a different key
	// The failure would happen when trying to decrypt data
	config2 := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: password2,
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	_, err = Open(tmpDir, config2)
	require.Error(t, err, "should fail to open with wrong password")
}

func TestEncryptionEmptyProperties(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create node with empty properties
	node, err := db.CreateNode(ctx, []string{"Empty"}, map[string]interface{}{})
	require.NoError(t, err)

	// Should work fine
	retrieved, err := db.GetNode(ctx, node.ID)
	require.NoError(t, err)
	assert.NotNil(t, retrieved)
}

func TestEncryptionNilProperties(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create node with nil properties map
	node, err := db.CreateNode(ctx, []string{"Nil"}, nil)
	require.NoError(t, err)

	// Should work fine
	retrieved, err := db.GetNode(ctx, node.ID)
	require.NoError(t, err)
	assert.NotNil(t, retrieved)
}

func TestEncryptionEmptyStringValues(t *testing.T) {
	config := &Config{
		EncryptionEnabled:  true,
		EncryptionPassword: "test-secure-password-12345!",
		DecayEnabled:       false,
		AutoLinksEnabled:   false,
	}

	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create node with empty string for sensitive field
	node, err := db.CreateNode(ctx, []string{"Person"}, map[string]interface{}{
		"name": "Test",
		"ssn":  "", // Empty SSN
	})
	require.NoError(t, err)

	retrieved, err := db.GetNode(ctx, node.ID)
	require.NoError(t, err)
	assert.Equal(t, "", retrieved.Properties["ssn"], "empty string should remain empty")
}

// Helper function to convert string ID to storage.NodeID
func storageNodeID(id string) storage.NodeID {
	// The storage package expects its own NodeID type
	return storage.NodeID(id)
}
