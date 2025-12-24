// Package multidb provides limit enforcement for multi-database support.
package multidb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Test Helpers
// ============================================================================

// setupTestManager creates a DatabaseManager with a test database.
func setupTestManager(t *testing.T) (*DatabaseManager, string) {
	t.Helper()
	inner := storage.NewMemoryEngine()
	manager, err := NewDatabaseManager(inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		manager.Close()
	})

	dbName := "testdb"
	err = manager.CreateDatabase(dbName)
	require.NoError(t, err)

	return manager, dbName
}

// setupTestManagerWithLimits creates a DatabaseManager with a test database and limits.
func setupTestManagerWithLimits(t *testing.T, limits *Limits) (*DatabaseManager, string) {
	t.Helper()
	manager, dbName := setupTestManager(t)
	err := manager.SetDatabaseLimits(dbName, limits)
	require.NoError(t, err)
	return manager, dbName
}

// ============================================================================
// databaseLimitChecker Tests
// ============================================================================

func TestDatabaseLimitChecker_CheckStorageLimits_NoLimits(t *testing.T) {
	manager, dbName := setupTestManager(t)

	// Get storage to create limit checker
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create some nodes
	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:     storage.NodeID("node-" + string(rune(i))),
			Labels: []string{"Test"},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	// Get limit checker (should have no limits)
	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Should not error with no limits
	err = checker.CheckStorageLimits("create_node", nil, nil)
	assert.NoError(t, err)

	err = checker.CheckStorageLimits("create_edge", nil, nil)
	assert.NoError(t, err)
}

func TestDatabaseLimitChecker_CheckStorageLimits_MaxNodes(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 5,
			MaxEdges: 0, // Unlimited
		},
	})

	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create nodes up to limit
	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:     storage.NodeID("node-" + string(rune(i))),
			Labels: []string{"Test"},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	// Get limit checker
	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Should fail when at limit
	err = checker.CheckStorageLimits("create_node", nil, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrStorageLimitExceeded)
	assert.Contains(t, err.Error(), "max_nodes limit")
	assert.Contains(t, err.Error(), "5/5")
}

func TestDatabaseLimitChecker_CheckStorageLimits_MaxNodes_UnderLimit(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 10,
			MaxEdges: 0,
		},
	})

	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create nodes under limit
	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:     storage.NodeID("node-" + string(rune(i))),
			Labels: []string{"Test"},
		}
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Should succeed when under limit
	err = checker.CheckStorageLimits("create_node", nil, nil)
	assert.NoError(t, err)
}

func TestDatabaseLimitChecker_CheckStorageLimits_MaxEdges(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 0, // Unlimited
			MaxEdges: 3,
		},
	})

	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create nodes first
	node1 := &storage.Node{ID: storage.NodeID("node-1"), Labels: []string{"Test"}}
	node2 := &storage.Node{ID: storage.NodeID("node-2"), Labels: []string{"Test"}}
	_, err = store.CreateNode(node1)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	// Create edges up to limit
	for i := 0; i < 3; i++ {
		edge := &storage.Edge{
			ID:        storage.EdgeID("edge-" + string(rune(i))),
			StartNode: storage.NodeID("node-1"),
			EndNode:   storage.NodeID("node-2"),
			Type:      "RELATES_TO",
		}
		err := store.CreateEdge(edge)
		require.NoError(t, err)
	}

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Should fail when at limit
	err = checker.CheckStorageLimits("create_edge", nil, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrStorageLimitExceeded)
	assert.Contains(t, err.Error(), "max_edges limit")
	assert.Contains(t, err.Error(), "3/3")
}

func TestDatabaseLimitChecker_CheckStorageLimits_MaxEdges_UnderLimit(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 0,
			MaxEdges: 10,
		},
	})

	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create nodes
	node1 := &storage.Node{ID: storage.NodeID("node-1"), Labels: []string{"Test"}}
	node2 := &storage.Node{ID: storage.NodeID("node-2"), Labels: []string{"Test"}}
	_, err = store.CreateNode(node1)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	// Create edges under limit
	for i := 0; i < 3; i++ {
		edge := &storage.Edge{
			ID:        storage.EdgeID("edge-" + string(rune(i))),
			StartNode: storage.NodeID("node-1"),
			EndNode:   storage.NodeID("node-2"),
			Type:      "RELATES_TO",
		}
		err := store.CreateEdge(edge)
		require.NoError(t, err)
	}

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Should succeed when under limit
	err = checker.CheckStorageLimits("create_edge", nil, nil)
	assert.NoError(t, err)
}

func TestDatabaseLimitChecker_CheckStorageLimits_InvalidOperation(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 5,
			MaxEdges: 5,
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Invalid operation should not error (just not checked)
	err = checker.CheckStorageLimits("invalid_op", nil, nil)
	assert.NoError(t, err)
}

func TestDatabaseLimitChecker_CheckStorageLimits_MaxBytes_Node(t *testing.T) {
	// Set a small MaxBytes limit (1KB)
	maxBytes := int64(1024)
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 0, // Unlimited
			MaxEdges: 0, // Unlimited
			MaxBytes: maxBytes,
		},
	})

	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create a node that fits within the limit
	smallNode := &storage.Node{
		ID:     storage.NodeID("node-small"),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"name": "Small Node",
		},
	}
	_, err = store.CreateNode(smallNode)
	require.NoError(t, err)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Get current size
	currentSize, _, _ := manager.GetStorageSize(dbName)

	// Create a large node that would exceed the limit
	largeNode := &storage.Node{
		ID:     storage.NodeID("node-large"),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"data": make([]byte, int(maxBytes-currentSize+100)), // Would exceed limit
		},
	}

	// Should fail with clear error message
	err = checker.CheckStorageLimits("create_node", largeNode, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrStorageLimitExceeded)
	assert.Contains(t, err.Error(), "max_bytes limit")
	assert.Contains(t, err.Error(), dbName)
	assert.Contains(t, err.Error(), "would exceed")
	// Verify error includes current size, limit, and new entity size
	assert.Contains(t, err.Error(), "current:")
	assert.Contains(t, err.Error(), "limit:")
	assert.Contains(t, err.Error(), "new entity:")
}

func TestDatabaseLimitChecker_CheckStorageLimits_MaxBytes_Edge(t *testing.T) {
	// Set a small MaxBytes limit (1KB)
	maxBytes := int64(1024)
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 0, // Unlimited
			MaxEdges: 0, // Unlimited
			MaxBytes: maxBytes,
		},
	})

	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create nodes first
	node1 := &storage.Node{ID: storage.NodeID("node-1"), Labels: []string{"Test"}}
	node2 := &storage.Node{ID: storage.NodeID("node-2"), Labels: []string{"Test"}}
	_, err = store.CreateNode(node1)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	// Create a small edge
	smallEdge := &storage.Edge{
		ID:        storage.EdgeID("edge-small"),
		StartNode: storage.NodeID("node-1"),
		EndNode:   storage.NodeID("node-2"),
		Type:      "RELATES_TO",
		Properties: map[string]any{
			"weight": 1.0,
		},
	}
	err = store.CreateEdge(smallEdge)
	require.NoError(t, err)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Get current size
	currentSize, _, _ := manager.GetStorageSize(dbName)

	// Create a large edge that would exceed the limit
	largeEdge := &storage.Edge{
		ID:        storage.EdgeID("edge-large"),
		StartNode: storage.NodeID("node-1"),
		EndNode:   storage.NodeID("node-2"),
		Type:      "RELATES_TO",
		Properties: map[string]any{
			"data": make([]byte, int(maxBytes-currentSize+100)), // Would exceed limit
		},
	}

	// Should fail with clear error message
	err = checker.CheckStorageLimits("create_edge", nil, largeEdge)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrStorageLimitExceeded)
	assert.Contains(t, err.Error(), "max_bytes limit")
	assert.Contains(t, err.Error(), dbName)
	assert.Contains(t, err.Error(), "would exceed")
	// Verify error includes current size, limit, and new entity size
	assert.Contains(t, err.Error(), "current:")
	assert.Contains(t, err.Error(), "limit:")
	assert.Contains(t, err.Error(), "new entity:")
}

func TestDatabaseLimitChecker_CheckStorageLimits_MaxBytes_UnderLimit(t *testing.T) {
	// Set a reasonable MaxBytes limit (10KB)
	maxBytes := int64(10 * 1024)
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 0, // Unlimited
			MaxEdges: 0, // Unlimited
			MaxBytes: maxBytes,
		},
	})

	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	// Create a node
	node := &storage.Node{
		ID:     storage.NodeID("node-1"),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"name": "Test Node",
			"data": "Some data",
		},
	}
	_, err = store.CreateNode(node)
	require.NoError(t, err)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Create another small node that should fit
	smallNode := &storage.Node{
		ID:     storage.NodeID("node-2"),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"name": "Another Node",
		},
	}

	// Should succeed when under limit
	err = checker.CheckStorageLimits("create_node", smallNode, nil)
	assert.NoError(t, err)
}

func TestDatabaseLimitChecker_CheckStorageLimits_ErrorMessages_AllLimits(t *testing.T) {
	t.Run("MaxNodes error message", func(t *testing.T) {
		manager, dbName := setupTestManagerWithLimits(t, &Limits{
			Storage: StorageLimits{
				MaxNodes: 3,
				MaxEdges: 0,
			},
		})

		store, err := manager.GetStorage(dbName)
		require.NoError(t, err)

		// Create nodes up to limit
		for i := 0; i < 3; i++ {
			node := &storage.Node{
				ID:     storage.NodeID("node-" + string(rune(i))),
				Labels: []string{"Test"},
			}
			_, err := store.CreateNode(node)
			require.NoError(t, err)
		}

		checker, err := newDatabaseLimitChecker(manager, dbName)
		require.NoError(t, err)

		err = checker.CheckStorageLimits("create_node", nil, nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrStorageLimitExceeded)
		assert.Contains(t, err.Error(), "max_nodes limit")
		assert.Contains(t, err.Error(), dbName)
		assert.Contains(t, err.Error(), "3/3") // Shows current/max
	})

	t.Run("MaxEdges error message", func(t *testing.T) {
		manager, dbName := setupTestManagerWithLimits(t, &Limits{
			Storage: StorageLimits{
				MaxNodes: 0,
				MaxEdges: 2,
			},
		})

		store, err := manager.GetStorage(dbName)
		require.NoError(t, err)

		// Create nodes first
		node1 := &storage.Node{ID: storage.NodeID("node-1"), Labels: []string{"Test"}}
		node2 := &storage.Node{ID: storage.NodeID("node-2"), Labels: []string{"Test"}}
		_, err = store.CreateNode(node1)
		require.NoError(t, err)
		_, err = store.CreateNode(node2)
		require.NoError(t, err)

		// Create edges up to limit
		for i := 0; i < 2; i++ {
			edge := &storage.Edge{
				ID:        storage.EdgeID("edge-" + string(rune(i))),
				StartNode: storage.NodeID("node-1"),
				EndNode:   storage.NodeID("node-2"),
				Type:      "RELATES_TO",
			}
			err := store.CreateEdge(edge)
			require.NoError(t, err)
		}

		checker, err := newDatabaseLimitChecker(manager, dbName)
		require.NoError(t, err)

		err = checker.CheckStorageLimits("create_edge", nil, nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrStorageLimitExceeded)
		assert.Contains(t, err.Error(), "max_edges limit")
		assert.Contains(t, err.Error(), dbName)
		assert.Contains(t, err.Error(), "2/2") // Shows current/max
	})

	t.Run("MaxBytes error message", func(t *testing.T) {
		maxBytes := int64(500) // Very small limit
		manager, dbName := setupTestManagerWithLimits(t, &Limits{
			Storage: StorageLimits{
				MaxNodes: 0,
				MaxEdges: 0,
				MaxBytes: maxBytes,
			},
		})

		checker, err := newDatabaseLimitChecker(manager, dbName)
		require.NoError(t, err)

		// Create a node that would exceed the limit
		largeNode := &storage.Node{
			ID:     storage.NodeID("node-large"),
			Labels: []string{"Test"},
			Properties: map[string]any{
				"data": make([]byte, 1000), // Definitely exceeds 500 bytes
			},
		}

		err = checker.CheckStorageLimits("create_node", largeNode, nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrStorageLimitExceeded)
		assert.Contains(t, err.Error(), "max_bytes limit")
		assert.Contains(t, err.Error(), dbName)
		assert.Contains(t, err.Error(), "would exceed")
		// Verify error message includes all relevant information
		assert.Contains(t, err.Error(), "current:")
		assert.Contains(t, err.Error(), "limit:")
		assert.Contains(t, err.Error(), "new entity:")
	})
}

func TestDatabaseLimitChecker_CheckQueryLimits_NoLimits(t *testing.T) {
	manager, dbName := setupTestManager(t)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	ctx := context.Background()
	newCtx, cancel, err := checker.CheckQueryLimits(ctx)
	require.NoError(t, err)
	assert.Equal(t, ctx, newCtx)
	cancel() // Should be safe to call
}

func TestDatabaseLimitChecker_CheckQueryLimits_MaxConcurrentQueries(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Query: QueryLimits{
			MaxConcurrentQueries: 2,
			MaxQueryTime:         0, // No timeout
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	ctx := context.Background()

	// First query should succeed
	_, cancel1, err := checker.CheckQueryLimits(ctx)
	require.NoError(t, err)

	// Second query should succeed
	_, cancel2, err := checker.CheckQueryLimits(ctx)
	require.NoError(t, err)

	// Third query should fail (at limit)
	_, _, err = checker.CheckQueryLimits(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrQueryLimitExceeded)
	assert.Contains(t, err.Error(), "max_concurrent_queries limit")
	assert.Contains(t, err.Error(), "2/2")

	// Cleanup
	cancel1()
	cancel2()
}

func TestDatabaseLimitChecker_CheckQueryLimits_MaxConcurrentQueries_Decrement(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Query: QueryLimits{
			MaxConcurrentQueries: 2,
			MaxQueryTime:         0,
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	ctx := context.Background()

	// Fill up to limit
	_, cancel1, err := checker.CheckQueryLimits(ctx)
	require.NoError(t, err)
	_, cancel2, err := checker.CheckQueryLimits(ctx)
	require.NoError(t, err)

	// Should be at limit
	_, _, err = checker.CheckQueryLimits(ctx)
	assert.Error(t, err)

	// Release one query
	cancel1()

	// Should now be able to start another query
	ctx3, cancel3, err := checker.CheckQueryLimits(ctx)
	require.NoError(t, err)
	assert.NotNil(t, ctx3)

	// Cleanup
	cancel2()
	cancel3()
}

func TestDatabaseLimitChecker_CheckQueryLimits_MaxQueryTime(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Query: QueryLimits{
			MaxConcurrentQueries: 0, // Unlimited
			MaxQueryTime:         100 * time.Millisecond,
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	ctx := context.Background()
	newCtx, cancel, err := checker.CheckQueryLimits(ctx)
	require.NoError(t, err)

	// Context should have timeout
	deadline, ok := newCtx.Deadline()
	assert.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(100*time.Millisecond), deadline, 50*time.Millisecond)

	// Context should be cancelled after timeout
	time.Sleep(150 * time.Millisecond)
	select {
	case <-newCtx.Done():
		// Expected
	default:
		t.Error("Context should be cancelled after timeout")
	}

	cancel()
}

func TestDatabaseLimitChecker_CheckQueryLimits_ConcurrentAndTimeout(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Query: QueryLimits{
			MaxConcurrentQueries: 1,
			MaxQueryTime:         50 * time.Millisecond,
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	ctx := context.Background()
	newCtx, cancel, err := checker.CheckQueryLimits(ctx)
	require.NoError(t, err)

	// Should have both concurrent limit and timeout
	deadline, ok := newCtx.Deadline()
	assert.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(50*time.Millisecond), deadline, 20*time.Millisecond)

	cancel()
}

func TestDatabaseLimitChecker_GetQueryLimits(t *testing.T) {
	limits := &Limits{
		Query: QueryLimits{
			MaxQueryTime:         60 * time.Second,
			MaxResults:           1000,
			MaxConcurrentQueries: 5,
		},
	}

	manager, dbName := setupTestManagerWithLimits(t, limits)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	queryLimits := checker.GetQueryLimits()
	require.NotNil(t, queryLimits)

	ql, ok := queryLimits.(*QueryLimits)
	require.True(t, ok)
	assert.Equal(t, 60*time.Second, ql.MaxQueryTime)
	assert.Equal(t, int64(1000), ql.MaxResults)
	assert.Equal(t, 5, ql.MaxConcurrentQueries)
}

func TestDatabaseLimitChecker_GetQueryLimits_NoLimits(t *testing.T) {
	manager, dbName := setupTestManager(t)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	queryLimits := checker.GetQueryLimits()
	assert.Nil(t, queryLimits)
}

func TestDatabaseLimitChecker_GetRateLimits(t *testing.T) {
	limits := &Limits{
		Rate: RateLimits{
			MaxQueriesPerSecond: 100,
			MaxWritesPerSecond:  50,
		},
	}

	manager, dbName := setupTestManagerWithLimits(t, limits)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	rateLimits := checker.GetRateLimits()
	require.NotNil(t, rateLimits)
	assert.Equal(t, 100, rateLimits.MaxQueriesPerSecond)
	assert.Equal(t, 50, rateLimits.MaxWritesPerSecond)
}

func TestDatabaseLimitChecker_GetRateLimits_NoLimits(t *testing.T) {
	manager, dbName := setupTestManager(t)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	rateLimits := checker.GetRateLimits()
	assert.Nil(t, rateLimits)
}

func TestDatabaseLimitChecker_CheckQueryRate_NoLimit(t *testing.T) {
	manager, dbName := setupTestManager(t)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Should not error with no limit
	err = checker.CheckQueryRate()
	assert.NoError(t, err)
}

func TestDatabaseLimitChecker_CheckQueryRate_WithinLimit(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Rate: RateLimits{
			MaxQueriesPerSecond: 10,
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Should allow queries within limit
	for i := 0; i < 10; i++ {
		err := checker.CheckQueryRate()
		assert.NoError(t, err, "query %d should be allowed", i)
	}
}

func TestDatabaseLimitChecker_CheckQueryRate_ExceedsLimit(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Rate: RateLimits{
			MaxQueriesPerSecond: 5,
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Use up all tokens
	for i := 0; i < 5; i++ {
		err := checker.CheckQueryRate()
		require.NoError(t, err)
	}

	// Next query should be rate limited
	err = checker.CheckQueryRate()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrRateLimitExceeded)
	assert.Contains(t, err.Error(), "max_queries_per_second")
	assert.Contains(t, err.Error(), "5")
}

func TestDatabaseLimitChecker_CheckWriteRate_NoLimit(t *testing.T) {
	manager, dbName := setupTestManager(t)

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Should not error with no limit
	err = checker.CheckWriteRate()
	assert.NoError(t, err)
}

func TestDatabaseLimitChecker_CheckWriteRate_WithinLimit(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Rate: RateLimits{
			MaxWritesPerSecond: 10,
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Should allow writes within limit
	for i := 0; i < 10; i++ {
		err := checker.CheckWriteRate()
		assert.NoError(t, err, "write %d should be allowed", i)
	}
}

func TestDatabaseLimitChecker_CheckWriteRate_ExceedsLimit(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Rate: RateLimits{
			MaxWritesPerSecond: 3,
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Use up all tokens
	for i := 0; i < 3; i++ {
		err := checker.CheckWriteRate()
		require.NoError(t, err)
	}

	// Next write should be rate limited
	err = checker.CheckWriteRate()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrRateLimitExceeded)
	assert.Contains(t, err.Error(), "max_writes_per_second")
	assert.Contains(t, err.Error(), "3")
}

func TestDatabaseLimitChecker_NewDatabaseLimitChecker_DatabaseNotFound(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Try to create checker for non-existent database
	_, err := newDatabaseLimitChecker(manager, "nonexistent")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseNotFound)
}

func TestDatabaseLimitChecker_CheckStorageLimits_StorageError(t *testing.T) {
	// This test verifies error handling when GetStorage fails
	// We can't easily simulate this without mocking, but we can test
	// the error path by using a database that goes offline
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 10,
		},
	})

	checker, err := newDatabaseLimitChecker(manager, dbName)
	require.NoError(t, err)

	// Set database offline
	err = manager.SetDatabaseStatus(dbName, "offline")
	require.NoError(t, err)

	// CheckStorageLimits should fail because GetStorage will fail
	err = checker.CheckStorageLimits("create_node", nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database is offline")
}

// ============================================================================
// rateLimiter Tests
// ============================================================================

func TestRateLimiter_Allow_WithinLimit(t *testing.T) {
	limiter := newRateLimiter(10)

	// Should allow all requests within limit
	for i := 0; i < 10; i++ {
		assert.True(t, limiter.Allow(), "request %d should be allowed", i)
	}
}

func TestRateLimiter_Allow_ExceedsLimit(t *testing.T) {
	limiter := newRateLimiter(5)

	// Use up all tokens
	for i := 0; i < 5; i++ {
		assert.True(t, limiter.Allow())
	}

	// Next request should be denied
	assert.False(t, limiter.Allow())
}

func TestRateLimiter_Allow_TokenRefill(t *testing.T) {
	limiter := newRateLimiter(5)

	// Use up all tokens
	for i := 0; i < 5; i++ {
		assert.True(t, limiter.Allow())
	}

	// Should be denied immediately
	assert.False(t, limiter.Allow())

	// Wait for token refill (1 second per token)
	time.Sleep(1200 * time.Millisecond)

	// Should allow at least one more request
	assert.True(t, limiter.Allow())
}

func TestRateLimiter_Allow_Concurrent(t *testing.T) {
	limiter := newRateLimiter(100)

	// Run concurrent requests
	results := make(chan bool, 200)
	for i := 0; i < 200; i++ {
		go func() {
			results <- limiter.Allow()
		}()
	}

	// Collect results
	allowed := 0
	for i := 0; i < 200; i++ {
		if <-results {
			allowed++
		}
	}

	// Should allow exactly 100 requests
	assert.Equal(t, 100, allowed)
}

func TestRateLimiter_Allow_ZeroRate(t *testing.T) {
	limiter := newRateLimiter(0)

	// Zero rate should never allow
	assert.False(t, limiter.Allow())
}

// ============================================================================
// ConnectionTracker Tests
// ============================================================================

func TestConnectionTracker_CheckConnectionLimit_NoLimit(t *testing.T) {
	manager, dbName := setupTestManager(t)
	tracker := NewConnectionTracker()

	// Should not error with no limit
	err := tracker.CheckConnectionLimit(manager, dbName)
	assert.NoError(t, err)
}

func TestConnectionTracker_CheckConnectionLimit_WithinLimit(t *testing.T) {
	limits := &Limits{
		Connection: ConnectionLimits{
			MaxConnections: 5,
		},
	}
	manager, dbName := setupTestManagerWithLimits(t, limits)
	tracker := NewConnectionTracker()

	// Should allow connections within limit
	for i := 0; i < 5; i++ {
		err := tracker.CheckConnectionLimit(manager, dbName)
		assert.NoError(t, err, "connection %d should be allowed", i)
		tracker.IncrementConnection(dbName)
	}
}

func TestConnectionTracker_CheckConnectionLimit_ExceedsLimit(t *testing.T) {
	limits := &Limits{
		Connection: ConnectionLimits{
			MaxConnections: 3,
		},
	}
	manager, dbName := setupTestManagerWithLimits(t, limits)
	tracker := NewConnectionTracker()

	// Fill up to limit
	for i := 0; i < 3; i++ {
		err := tracker.CheckConnectionLimit(manager, dbName)
		require.NoError(t, err)
		tracker.IncrementConnection(dbName)
	}

	// Next connection should fail
	err := tracker.CheckConnectionLimit(manager, dbName)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrConnectionLimitExceeded)
	assert.Contains(t, err.Error(), "max_connections limit")
	assert.Contains(t, err.Error(), "3/3")
}

func TestConnectionTracker_CheckConnectionLimit_DatabaseNotFound(t *testing.T) {
	manager, _ := setupTestManager(t)
	tracker := NewConnectionTracker()

	err := tracker.CheckConnectionLimit(manager, "nonexistent")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseNotFound)
}

func TestConnectionTracker_IncrementConnection(t *testing.T) {
	tracker := NewConnectionTracker()

	// Initially zero
	assert.Equal(t, 0, tracker.GetConnectionCount("db1"))

	// Increment
	tracker.IncrementConnection("db1")
	assert.Equal(t, 1, tracker.GetConnectionCount("db1"))

	tracker.IncrementConnection("db1")
	assert.Equal(t, 2, tracker.GetConnectionCount("db1"))

	// Different database
	tracker.IncrementConnection("db2")
	assert.Equal(t, 1, tracker.GetConnectionCount("db2"))
	assert.Equal(t, 2, tracker.GetConnectionCount("db1"))
}

func TestConnectionTracker_DecrementConnection(t *testing.T) {
	tracker := NewConnectionTracker()

	// Increment a few times
	tracker.IncrementConnection("db1")
	tracker.IncrementConnection("db1")
	tracker.IncrementConnection("db1")
	assert.Equal(t, 3, tracker.GetConnectionCount("db1"))

	// Decrement
	tracker.DecrementConnection("db1")
	assert.Equal(t, 2, tracker.GetConnectionCount("db1"))

	tracker.DecrementConnection("db1")
	assert.Equal(t, 1, tracker.GetConnectionCount("db1"))

	tracker.DecrementConnection("db1")
	assert.Equal(t, 0, tracker.GetConnectionCount("db1"))
}

func TestConnectionTracker_DecrementConnection_DoesNotGoNegative(t *testing.T) {
	tracker := NewConnectionTracker()

	// Decrement when already zero
	tracker.DecrementConnection("db1")
	assert.Equal(t, 0, tracker.GetConnectionCount("db1"))

	tracker.DecrementConnection("db1")
	assert.Equal(t, 0, tracker.GetConnectionCount("db1"))
}

func TestConnectionTracker_GetConnectionCount(t *testing.T) {
	tracker := NewConnectionTracker()

	// Initially zero for all databases
	assert.Equal(t, 0, tracker.GetConnectionCount("db1"))
	assert.Equal(t, 0, tracker.GetConnectionCount("db2"))

	// Increment and check
	tracker.IncrementConnection("db1")
	assert.Equal(t, 1, tracker.GetConnectionCount("db1"))
	assert.Equal(t, 0, tracker.GetConnectionCount("db2"))

	tracker.IncrementConnection("db2")
	assert.Equal(t, 1, tracker.GetConnectionCount("db1"))
	assert.Equal(t, 1, tracker.GetConnectionCount("db2"))
}

func TestConnectionTracker_Concurrent(t *testing.T) {
	limits := &Limits{
		Connection: ConnectionLimits{
			MaxConnections: 100,
		},
	}
	manager, dbName := setupTestManagerWithLimits(t, limits)
	tracker := NewConnectionTracker()

	// Run concurrent operations
	done := make(chan bool, 200)
	for i := 0; i < 200; i++ {
		go func() {
			_ = tracker.TryIncrementConnection(manager, dbName)
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 200; i++ {
		<-done
	}

	// Should have exactly 100 connections (or at most 100 due to race conditions)
	// TryIncrementConnection is atomic, so it should never exceed the limit.
	count := tracker.GetConnectionCount(dbName)
	assert.Equal(t, 100, count, "should not exceed connection limit")
}

func TestConnectionTracker_CheckConnectionLimit_AfterDecrement(t *testing.T) {
	limits := &Limits{
		Connection: ConnectionLimits{
			MaxConnections: 2,
		},
	}
	manager, dbName := setupTestManagerWithLimits(t, limits)
	tracker := NewConnectionTracker()

	// Fill up to limit
	for i := 0; i < 2; i++ {
		err := tracker.CheckConnectionLimit(manager, dbName)
		require.NoError(t, err)
		tracker.IncrementConnection(dbName)
	}

	// Should be at limit
	err := tracker.CheckConnectionLimit(manager, dbName)
	assert.Error(t, err)

	// Decrement one
	tracker.DecrementConnection(dbName)

	// Should now be able to add another
	err = tracker.CheckConnectionLimit(manager, dbName)
	assert.NoError(t, err)
}
