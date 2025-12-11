package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBug_ConcurrentCreateAndCount simulates real server behavior:
// - One goroutine rapidly creates nodes
// - Another goroutine repeatedly queries count
// - Count should always reflect actual nodes
func TestBug_ConcurrentCreateAndCount(t *testing.T) {
	tmpDir := t.TempDir()
	config := &Config{
		AsyncWritesEnabled: true,
		AsyncFlushInterval: 50 * time.Millisecond,
	}

	db, err := Open(tmpDir, config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Clear first
	_, err = db.ExecuteCypher(ctx, "MATCH (n) DETACH DELETE n", nil)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	nodeCount := 100
	countErrors := make([]string, 0)
	var mu sync.Mutex

	// Goroutine 1: Create nodes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < nodeCount; i++ {
			query := fmt.Sprintf("CREATE (n:Concurrent {id: %d})", i)
			_, err := db.ExecuteCypher(ctx, query, nil)
			if err != nil {
				t.Logf("CREATE error: %v", err)
			}
		}
	}()

	// Goroutine 2: Query count repeatedly
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			// Test the fast path
			result, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
			if err != nil {
				continue
			}
			fastCount := result.Rows[0][0].(int64)

			// Test the label path
			result, err = db.ExecuteCypher(ctx, "MATCH (n:Concurrent) RETURN count(n) as cnt", nil)
			if err != nil {
				continue
			}
			labelCount := result.Rows[0][0].(int64)

			// They should be equal (or fast should be >= label since it includes all nodes)
			if fastCount < labelCount {
				mu.Lock()
				countErrors = append(countErrors, fmt.Sprintf("MISMATCH: MATCH (n) count=%d, MATCH (n:Concurrent) count=%d", fastCount, labelCount))
				mu.Unlock()
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Wait for final flush
	time.Sleep(200 * time.Millisecond)

	// Final check
	result, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	finalFastCount := result.Rows[0][0].(int64)
	t.Logf("Final MATCH (n) RETURN count(n): %d", finalFastCount)

	result, err = db.ExecuteCypher(ctx, "MATCH (n:Concurrent) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	finalLabelCount := result.Rows[0][0].(int64)
	t.Logf("Final MATCH (n:Concurrent) RETURN count(n): %d", finalLabelCount)

	storageCount, _ := db.storage.NodeCount()
	t.Logf("Final storage.NodeCount(): %d", storageCount)

	// Log any mismatches found during concurrent access
	if len(countErrors) > 0 {
		for _, e := range countErrors {
			t.Logf("RACE CONDITION: %s", e)
		}
	}

	// Final counts should match
	assert.Equal(t, int64(nodeCount), finalLabelCount, "Label count should be %d", nodeCount)
	assert.Equal(t, int64(nodeCount), finalFastCount, "Fast path count should be %d", nodeCount)
	assert.Equal(t, int64(nodeCount), storageCount, "Storage count should be %d", nodeCount)
}

// TestBug_CountDuringFlush tests count accuracy during flush operations
func TestBug_CountDuringFlush(t *testing.T) {
	tmpDir := t.TempDir()
	config := &Config{
		AsyncWritesEnabled: true,
		AsyncFlushInterval: 10 * time.Millisecond, // Fast flush
	}

	db, err := Open(tmpDir, config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create nodes rapidly
	for i := 0; i < 50; i++ {
		_, err := db.ExecuteCypher(ctx, fmt.Sprintf("CREATE (n:FlushTest {id: %d})", i), nil)
		require.NoError(t, err)

		// Query count immediately after each create
		result, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
		require.NoError(t, err)
		count := result.Rows[0][0].(int64)

		// Count should be at least i+1 (might be higher if previous flushes completed)
		if count == 0 && i > 5 {
			t.Logf("WARNING at iteration %d: count is 0 when it should be >= %d", i, i+1)
		}
	}

	// Wait for final flush
	time.Sleep(100 * time.Millisecond)

	// Final verification
	result, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	finalCount := result.Rows[0][0].(int64)
	t.Logf("Final count: %d", finalCount)

	assert.Equal(t, int64(50), finalCount, "Should have 50 nodes")
}
