// Package multidb provides limit enforcement for multi-database support.
package multidb

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strconv"
	"sync"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(start time.Time) *manualClock {
	return &manualClock{now: start}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// ============================================================================
// Test Helpers
// ============================================================================

// setupTestManager creates a DatabaseManager with a test database.
func setupTestManager(t *testing.T) (*DatabaseManager, string) {
	t.Helper()
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		manager.Close()
	})

	dbName := "testdb"
	err = manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: dbName})
	require.NoError(t, err)

	return manager, dbName
}

// setupTestManagerWithLimits creates a DatabaseManager with a test database and limits.
func setupTestManagerWithLimits(t *testing.T, limits *Limits) (*DatabaseManager, string) {
	t.Helper()
	manager, dbName := setupTestManager(t)
	err := manager.SetDatabaseLimits(t.Context(), dbName, limits)
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
	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
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
	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
	require.NoError(t, err)

	// Should fail when at limit
	err = checker.CheckStorageLimits("create_node", nil, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrStorageLimitExceeded)
	assert.Contains(t, err.Error(), "max_nodes limit")
	assert.Contains(t, err.Error(), "5/5")
	requireLocalizedMessageID(t, err, localization.MessageMultidbMaxNodesReached)
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

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
	require.NoError(t, err)

	// Should succeed when under limit
	err = checker.CheckStorageLimits("create_node", nil, nil)
	assert.NoError(t, err)
}

// TestDatabaseLimitChecker_CheckStorageLimits_MaxEdgesCapacity is the canonical
// MaxEdges boundary table: under-limit and exactly-at-limit
// cases share one seed fixture while keeping each case's original
// assertions.
func TestDatabaseLimitChecker_CheckStorageLimits_MaxEdgesCapacity(t *testing.T) {
	seedEdgeEndpoints := func(t *testing.T, manager *DatabaseManager, dbName string) storage.Engine {
		t.Helper()
		store, err := manager.GetStorage(dbName)
		require.NoError(t, err)
		node1 := &storage.Node{ID: storage.NodeID("node-1"), Labels: []string{"Test"}}
		node2 := &storage.Node{ID: storage.NodeID("node-2"), Labels: []string{"Test"}}
		_, err = store.CreateNode(node1)
		require.NoError(t, err)
		_, err = store.CreateNode(node2)
		require.NoError(t, err)
		return store
	}

	for _, tc := range []struct {
		name      string
		maxEdges  int64
		seedEdges int
		wantErr   bool
		token     string
		countPair string
		message   localization.MessageID
	}{
		{
			// Original MaxEdges_UnderLimit: headroom stays writable.
			name:      "under-limit",
			maxEdges:  10,
			seedEdges: 3,
		},
		{
			// Original MaxEdges: exactly at the configured cap.
			name:      "at-limit",
			maxEdges:  3,
			seedEdges: 3,
			wantErr:   true,
			token:     "max_edges limit",
			countPair: "3/3",
			message:   localization.MessageMultidbMaxEdgesReached,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, dbName := setupTestManagerWithLimits(t, &Limits{
				Storage: StorageLimits{
					MaxNodes: 0, // Unlimited
					MaxEdges: tc.maxEdges,
				},
			})
			store := seedEdgeEndpoints(t, manager, dbName)

			for i := 0; i < tc.seedEdges; i++ {
				edge := &storage.Edge{
					ID:        storage.EdgeID("edge-" + strconv.Itoa(i)),
					StartNode: storage.NodeID("node-1"),
					EndNode:   storage.NodeID("node-2"),
					Type:      "RELATES_TO",
				}
				require.NoError(t, store.CreateEdge(edge))
			}
			if !tc.wantErr {
				require.Less(t, int64(tc.seedEdges), tc.maxEdges, "under-limit case must keep headroom")
			}

			manager.mu.Lock()
			_, checker, err := manager.databaseViewLocked(dbName, false)
			manager.mu.Unlock()
			require.NoError(t, err)
			require.NotNil(t, checker)

			err = checker.CheckStorageLimits("create_edge", nil, nil)
			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
			assert.ErrorIs(t, err, ErrStorageLimitExceeded)
			assert.Contains(t, err.Error(), tc.token)
			assert.Contains(t, err.Error(), tc.countPair)
			requireLocalizedMessageID(t, err, tc.message)
		})
	}
}

func TestDatabaseLimitChecker_CheckStorageLimits_InvalidOperation(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Storage: StorageLimits{
			MaxNodes: 5,
			MaxEdges: 5,
		},
	})

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
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

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
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
	// Set a small MaxBytes limit (5KB — must accommodate nodes, edges, and index catalogs)
	maxBytes := int64(5 * 1024)
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

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
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

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
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

		manager.mu.Lock()
		_, checker, err := manager.databaseViewLocked(dbName, false)
		manager.mu.Unlock()
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

		manager.mu.Lock()
		_, checker, err := manager.databaseViewLocked(dbName, false)
		manager.mu.Unlock()
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

		manager.mu.Lock()
		_, checker, err := manager.databaseViewLocked(dbName, false)
		manager.mu.Unlock()
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

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
	require.NoError(t, err)

	ctx := context.Background()
	newCtx, cancel, err := checker.CheckQueryLimits(ctx)
	require.NoError(t, err)
	assert.Equal(t, ctx, newCtx)
	cancel() // Should be safe to call
}

func TestDatabaseLimitChecker_CheckQueryLimits_MaxConcurrentQueries(t *testing.T) {
	for _, releaseAndRetry := range []bool{false, true} {
		name := "at-capacity"
		if releaseAndRetry {
			name = "decrement-restores-slot"
		}
		t.Run(name, func(t *testing.T) {
			manager, dbName := setupTestManagerWithLimits(t, &Limits{Query: QueryLimits{MaxConcurrentQueries: 2, MaxQueryTime: 0}})
			manager.mu.Lock()
			_, checker, err := manager.databaseViewLocked(dbName, false)
			manager.mu.Unlock()
			require.NoError(t, err)
			ctx := context.Background()
			_, cancel1, err := checker.CheckQueryLimits(ctx)
			require.NoError(t, err)
			defer func() {
				if cancel1 != nil {
					cancel1()
				}
			}()
			_, cancel2, err := checker.CheckQueryLimits(ctx)
			require.NoError(t, err)
			defer cancel2()
			_, _, err = checker.CheckQueryLimits(ctx)
			assert.Error(t, err)
			assert.ErrorIs(t, err, ErrQueryLimitExceeded)
			assert.Contains(t, err.Error(), "max_concurrent_queries limit")
			assert.Contains(t, err.Error(), "2/2")
			requireLocalizedMessageID(t, err, localization.MessageMultidbMaxConcurrentQueries)
			if releaseAndRetry {
				cancel1()
				cancel1 = nil
				ctx3, cancel3, err := checker.CheckQueryLimits(ctx)
				require.NoError(t, err)
				defer cancel3()
				assert.NotNil(t, ctx3)
			}
		})
	}
}

func TestDatabaseLimitChecker_CheckQueryLimits_MaxQueryTime(t *testing.T) {
	manager, dbName := setupTestManagerWithLimits(t, &Limits{
		Query: QueryLimits{
			MaxConcurrentQueries: 0, // Unlimited
			MaxQueryTime:         100 * time.Millisecond,
		},
	})

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
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

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
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

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
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

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
	require.NoError(t, err)

	queryLimits := checker.GetQueryLimits()
	assert.Nil(t, queryLimits)
}

func TestDatabaseLimitChecker_GetRateLimits(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "unlimited"
		if configured {
			name = "configured"
		}
		t.Run(name, func(t *testing.T) {
			var limits *Limits
			if configured {
				limits = &Limits{Rate: RateLimits{MaxQueriesPerSecond: 100, MaxWritesPerSecond: 50}}
			}
			manager, dbName := setupTestManager(t)
			if configured {
				require.NoError(t, manager.SetDatabaseLimits(t.Context(), dbName, limits))
			}
			manager.mu.Lock()
			_, checker, err := manager.databaseViewLocked(dbName, false)
			manager.mu.Unlock()
			require.NoError(t, err)
			rateLimits := checker.GetRateLimits()
			if !configured {
				assert.Nil(t, rateLimits)
				return
			}
			require.NotNil(t, rateLimits)
			assert.Equal(t, 100, rateLimits.MaxQueriesPerSecond)
			assert.Equal(t, 50, rateLimits.MaxWritesPerSecond)
		})
	}
}

// TestDatabaseLimitChecker_RateChecks is the canonical query/write rate table.
// Both kinds share one fixture and one assertion set: no configured limit,
// exactly `limit` allowed operations at the limit boundary, and one overflow
// declaration per kind that keeps its original token, limit and localized ID.
func TestDatabaseLimitChecker_RateChecks(t *testing.T) {
	checkerFor := func(t *testing.T, limits *Limits) *databaseLimitChecker {
		t.Helper()
		manager, dbName := setupTestManager(t)
		if limits != nil {
			require.NoError(t, manager.SetDatabaseLimits(t.Context(), dbName, limits))
		}
		manager.mu.Lock()
		_, checker, err := manager.databaseViewLocked(dbName, false)
		manager.mu.Unlock()
		require.NoError(t, err)
		require.NotNil(t, checker)
		return checker
	}

	for _, kind := range []struct {
		name          string
		token         string
		exceededLimit int
		message       localization.MessageID
		applyLimit    func(*RateLimits, int)
		check         func(*databaseLimitChecker) func() error
	}{
		{
			name:          "query",
			token:         "max_queries_per_second",
			exceededLimit: 5,
			message:       localization.MessageMultidbQueryRateExceeded,
			applyLimit:    func(r *RateLimits, limit int) { r.MaxQueriesPerSecond = limit },
			check:         func(c *databaseLimitChecker) func() error { return c.CheckQueryRate },
		},
		{
			name:          "write",
			token:         "max_writes_per_second",
			exceededLimit: 3,
			message:       localization.MessageMultidbWriteRateExceeded,
			applyLimit:    func(r *RateLimits, limit int) { r.MaxWritesPerSecond = limit },
			check:         func(c *databaseLimitChecker) func() error { return c.CheckWriteRate },
		},
	} {
		t.Run(kind.name+"/no-configured-limit", func(t *testing.T) {
			// Without a configured rate neither kind ever consumes quota.
			check := kind.check(checkerFor(t, nil))
			for i := 0; i < 100; i++ {
				assert.NoError(t, check(), "%s %d should be allowed without a limit", kind.name, i)
			}
		})

		t.Run(kind.name+"/within-limit", func(t *testing.T) {
			// A limit of 10 allows exactly ten operations, matching the
			// original WithinLimit declarations for both kinds.
			const limit = 10
			limits := &Limits{}
			kind.applyLimit(&limits.Rate, limit)
			check := kind.check(checkerFor(t, limits))
			for i := 0; i < limit; i++ {
				assert.NoError(t, check(), "%s %d should be allowed", kind.name, i)
			}
		})

		t.Run(kind.name+"/exceeds-limit", func(t *testing.T) {
			// Original overflow limits: five queries and three writes.
			limits := &Limits{}
			kind.applyLimit(&limits.Rate, kind.exceededLimit)
			check := kind.check(checkerFor(t, limits))
			for i := 0; i < kind.exceededLimit; i++ {
				require.NoError(t, check())
			}
			err := check()
			assert.Error(t, err)
			assert.ErrorIs(t, err, ErrRateLimitExceeded)
			assert.Contains(t, err.Error(), kind.token)
			assert.Contains(t, err.Error(), strconv.Itoa(kind.exceededLimit))
			requireLocalizedMessageID(t, err, kind.message)
		})
	}
}

func TestDatabaseLimitChecker_RateChecksRaceWithLimitUpdates(t *testing.T) {
	manager, dbName := setupTestManager(t)
	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
	require.NoError(t, err)

	var waitGroup sync.WaitGroup
	waitGroup.Add(3)
	go func() {
		defer waitGroup.Done()
		for index := 0; index < 1000; index++ {
			checker.policy().updateLimits(&Limits{Rate: RateLimits{MaxQueriesPerSecond: index%3 + 1, MaxWritesPerSecond: index%5 + 1}})
		}
	}()
	go func() {
		defer waitGroup.Done()
		for index := 0; index < 1000; index++ {
			_ = checker.CheckQueryRate()
		}
	}()
	go func() {
		defer waitGroup.Done()
		for index := 0; index < 1000; index++ {
			_ = checker.CheckWriteRate()
		}
	}()
	waitGroup.Wait()
}

func TestDatabaseLimitChecker_NewDatabaseLimitChecker_DatabaseNotFound(t *testing.T) {
	manager, _ := setupTestManager(t)

	// Try to create checker for non-existent database
	manager.mu.Lock()
	_, _, err := manager.databaseViewLocked("nonexistent", false)
	manager.mu.Unlock()
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

	manager.mu.Lock()
	_, checker, err := manager.databaseViewLocked(dbName, false)
	manager.mu.Unlock()
	require.NoError(t, err)

	// Set database offline
	err = manager.SetDatabaseStatus(t.Context(), dbName, "offline")
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
	clock := newManualClock(time.Unix(0, 0))
	limiter := newRateLimiterWithClock(10, clock.Now)

	// Should allow all requests within limit
	for i := 0; i < 10; i++ {
		assert.True(t, limiter.Allow(), "request %d should be allowed", i)
	}
}

func TestRateLimiter_Allow_ExceedsLimit(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	limiter := newRateLimiterWithClock(5, clock.Now)

	// Use up all tokens
	for i := 0; i < 5; i++ {
		assert.True(t, limiter.Allow())
	}

	// Next request should be denied
	assert.False(t, limiter.Allow())
}

func TestRateLimiter_Allow_TokenRefill(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	limiter := newRateLimiterWithClock(5, clock.Now)

	// Use up all tokens
	for i := 0; i < 5; i++ {
		assert.True(t, limiter.Allow())
	}

	// Should be denied immediately
	assert.False(t, limiter.Allow())

	// Advance enough time for one token refill.
	clock.Advance(1200 * time.Millisecond)

	// Should allow at least one more request
	assert.True(t, limiter.Allow())
}

func TestRateLimiter_Allow_Concurrent(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	limiter := newRateLimiterWithClock(100, clock.Now)

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

	// With a frozen clock there is no refill during the burst, so exactly the
	// initial bucket should be allowed.
	assert.Equal(t, 100, allowed)
}

func TestRateLimiter_Allow_ZeroRate(t *testing.T) {
	clock := newManualClock(time.Unix(0, 0))
	limiter := newRateLimiterWithClock(0, clock.Now)

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
	requireLocalizedMessageID(t, err, localization.MessageMultidbMaxConnections)
}

func requireLocalizedMessageID(t *testing.T, err error, messageID localization.MessageID) {
	t.Helper()
	var localizedErr *nornicerrors.Localized
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
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

func TestConnectionTracker_TryIncrementConnectionNoLimit(t *testing.T) {
	manager, dbName := setupTestManager(t)
	tracker := NewConnectionTracker()

	require.NoError(t, tracker.TryIncrementConnection(manager, dbName))
	assert.Equal(t, 1, tracker.GetConnectionCount(dbName))
	require.NoError(t, tracker.TryIncrementConnection(manager, dbName))
	assert.Equal(t, 2, tracker.GetConnectionCount(dbName))
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

// ============================================================================
// sizeTrackingEngine – CRUD with incremental size tracking
// ============================================================================

func TestSizeTrackingEngine_NodeWrites(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		update   bool
	}{
		{name: "create", id: "st-node-1"}, {name: "update", id: "st-upd-1", update: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, dbName := setupTestManager(t)
			store, err := manager.GetStorage(dbName)
			require.NoError(t, err)
			node := &storage.Node{ID: storage.NodeID(tc.id), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
			id, err := store.CreateNode(node)
			require.NoError(t, err)
			if tc.update {
				node.ID = id
				node.Properties["name"] = "Alice Updated"
				node.Properties["age"] = 30
				require.NoError(t, store.UpdateNode(node))
			} else {
				assert.NotEmpty(t, id)
			}
			got, err := store.GetNode(id)
			require.NoError(t, err)
			if tc.update {
				assert.Equal(t, "Alice Updated", got.Properties["name"])
				assert.EqualValues(t, 30, got.Properties["age"])
			} else {
				assert.Equal(t, "Alice", got.Properties["name"])
			}
		})
	}
}

func TestSizeTrackingEngine_DeleteNode(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	node := &storage.Node{
		ID:         storage.NodeID("st-del-1"),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	id, err := store.CreateNode(node)
	require.NoError(t, err)

	err = store.DeleteNode(id)
	require.NoError(t, err)

	_, err = store.GetNode(id)
	assert.Error(t, err)
}

func TestSizeTrackingEngine_CreateEdge(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	idA, err := store.CreateNode(&storage.Node{ID: "st-ce-a", Labels: []string{"Person"}})
	require.NoError(t, err)
	idB, err := store.CreateNode(&storage.Node{ID: "st-ce-b", Labels: []string{"Person"}})
	require.NoError(t, err)

	edge := &storage.Edge{
		ID:         storage.EdgeID("st-edge-1"),
		StartNode:  idA,
		EndNode:    idB,
		Type:       "KNOWS",
		Properties: map[string]interface{}{"since": 2020},
	}
	err = store.CreateEdge(edge)
	require.NoError(t, err)
}

func TestSizeTrackingEngine_UpdateEdge(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	idA, err := store.CreateNode(&storage.Node{ID: "st-ue-a", Labels: []string{"Person"}})
	require.NoError(t, err)
	idB, err := store.CreateNode(&storage.Node{ID: "st-ue-b", Labels: []string{"Person"}})
	require.NoError(t, err)

	edge := &storage.Edge{
		ID:         storage.EdgeID("st-ue-edge-1"),
		StartNode:  idA,
		EndNode:    idB,
		Type:       "KNOWS",
		Properties: map[string]interface{}{"since": 2020},
	}
	err = store.CreateEdge(edge)
	require.NoError(t, err)

	outgoing, err := store.GetOutgoingEdges(idA)
	require.NoError(t, err)
	require.NotEmpty(t, outgoing)

	outgoing[0].Properties["since"] = 2021
	err = store.UpdateEdge(outgoing[0])
	require.NoError(t, err)
}

func TestSizeTrackingEngine_DeleteEdge(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	idA, err := store.CreateNode(&storage.Node{ID: "st-de-a", Labels: []string{"Person"}})
	require.NoError(t, err)
	idB, err := store.CreateNode(&storage.Node{ID: "st-de-b", Labels: []string{"Person"}})
	require.NoError(t, err)

	edge := &storage.Edge{ID: "st-de-edge-1", StartNode: idA, EndNode: idB, Type: "KNOWS"}
	err = store.CreateEdge(edge)
	require.NoError(t, err)

	outgoing, err := store.GetOutgoingEdges(idA)
	require.NoError(t, err)
	require.NotEmpty(t, outgoing)

	err = store.DeleteEdge(outgoing[0].ID)
	require.NoError(t, err)

	outgoing, err = store.GetOutgoingEdges(idA)
	require.NoError(t, err)
	assert.Empty(t, outgoing)
}

func TestSizeTrackingEngine_BulkCreateNodes(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	nodes := []*storage.Node{
		{ID: "st-bulk-n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: "st-bulk-n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
		{ID: "st-bulk-n3", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol"}},
	}
	err = store.BulkCreateNodes(nodes)
	require.NoError(t, err)

	allNodes, err := store.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(allNodes), 3)
}

func TestSizeTrackingEngine_BulkCreateEdges(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	idA, err := store.CreateNode(&storage.Node{ID: "st-bce-a", Labels: []string{"Person"}})
	require.NoError(t, err)
	idB, err := store.CreateNode(&storage.Node{ID: "st-bce-b", Labels: []string{"Person"}})
	require.NoError(t, err)
	idC, err := store.CreateNode(&storage.Node{ID: "st-bce-c", Labels: []string{"Person"}})
	require.NoError(t, err)

	edges := []*storage.Edge{
		{ID: "st-bce-e1", StartNode: idA, EndNode: idB, Type: "KNOWS"},
		{ID: "st-bce-e2", StartNode: idB, EndNode: idC, Type: "KNOWS"},
	}
	err = store.BulkCreateEdges(edges)
	require.NoError(t, err)
}

func TestSizeTrackingEngine_BulkDeleteNodes(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	id1, err := store.CreateNode(&storage.Node{ID: "st-bdn-1", Labels: []string{"Temp"}})
	require.NoError(t, err)
	id2, err := store.CreateNode(&storage.Node{ID: "st-bdn-2", Labels: []string{"Temp"}})
	require.NoError(t, err)

	err = store.BulkDeleteNodes([]storage.NodeID{id1, id2})
	require.NoError(t, err)

	_, err = store.GetNode(id1)
	assert.Error(t, err)
	_, err = store.GetNode(id2)
	assert.Error(t, err)
}

func TestSizeTrackingEngine_BulkDeleteEdges(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	idA, err := store.CreateNode(&storage.Node{ID: "st-bde-a", Labels: []string{"Person"}})
	require.NoError(t, err)
	idB, err := store.CreateNode(&storage.Node{ID: "st-bde-b", Labels: []string{"Person"}})
	require.NoError(t, err)

	err = store.CreateEdge(&storage.Edge{ID: "st-bde-e1", StartNode: idA, EndNode: idB, Type: "KNOWS"})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "st-bde-e2", StartNode: idB, EndNode: idA, Type: "LIKES"})
	require.NoError(t, err)

	outgoing, err := store.GetOutgoingEdges(idA)
	require.NoError(t, err)
	incoming, err := store.GetIncomingEdges(idA)
	require.NoError(t, err)

	var edgeIDs []storage.EdgeID
	for _, e := range outgoing {
		edgeIDs = append(edgeIDs, e.ID)
	}
	for _, e := range incoming {
		edgeIDs = append(edgeIDs, e.ID)
	}

	err = store.BulkDeleteEdges(edgeIDs)
	require.NoError(t, err)
}

func TestSizeTrackingEngine_DeleteNodeWithEdges(t *testing.T) {
	// Deleting a node should account for connected edge bytes in the size delta
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	idA, err := store.CreateNode(&storage.Node{ID: "st-dne-a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)
	idB, err := store.CreateNode(&storage.Node{ID: "st-dne-b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)

	err = store.CreateEdge(&storage.Edge{ID: "st-dne-e1", StartNode: idA, EndNode: idB, Type: "KNOWS", Properties: map[string]interface{}{"since": 2020}})
	require.NoError(t, err)

	err = store.DeleteNode(idA)
	require.NoError(t, err)

	_, err = store.GetNode(idA)
	assert.Error(t, err)
}

func TestSizeTrackingEngine_GetInnerEngine(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)

	if ste, ok := store.(*sizeTrackingEngine); ok {
		inner := ste.GetInnerEngine()
		assert.NotNil(t, inner)
	}
}

// ============================================================================
// calculateCurrentStorageSize
// ============================================================================

func TestCalculateCurrentStorageSize(t *testing.T) {
	for _, populated := range []bool{false, true} {
		name := "empty"
		if populated {
			name = "populated"
		}
		t.Run(name, func(t *testing.T) {
			manager, dbName := setupTestManager(t)
			store, err := manager.GetStorage(dbName)
			require.NoError(t, err)
			if populated {
				id1, err := store.CreateNode(&storage.Node{ID: "css-1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
				require.NoError(t, err)
				id2, err := store.CreateNode(&storage.Node{ID: "css-2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
				require.NoError(t, err)
				err = store.CreateEdge(&storage.Edge{ID: "css-e1", StartNode: id1, EndNode: id2, Type: "KNOWS"})
				require.NoError(t, err)

			}
			nodeSize, edgeSize, calcErr := manager.calculateStorageSizeFromEngine(store)
			require.NoError(t, calcErr)
			if populated {
				assert.Greater(t, nodeSize, int64(0), "node size must be > 0")
				assert.Greater(t, edgeSize, int64(0), "edge size must be > 0")
			} else {
				assert.Equal(t, int64(0), nodeSize)
				assert.Equal(t, int64(0), edgeSize)
			}
		})
	}
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
