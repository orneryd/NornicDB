//go:build localllm

package embed

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mockModel implements a test double for localllm.Model
// that can be configured to panic, error, or succeed.
type mockModel struct {
	embedFunc  func(ctx context.Context, text string) ([]float32, error)
	dimensions int
	closeCount atomic.Int32
	embedCount atomic.Int32
}

func (m *mockModel) Embed(ctx context.Context, text string) ([]float32, error) {
	m.embedCount.Add(1)
	if m.embedFunc != nil {
		return m.embedFunc(ctx, text)
	}
	return make([]float32, m.dimensions), nil
}

func (m *mockModel) Dimensions() int {
	return m.dimensions
}

func (m *mockModel) Close() error {
	m.closeCount.Add(1)
	return nil
}

// --- Helper to create a testable embedder with mock ---

// testableEmbedder wraps LocalGGUFEmbedder with injectable mock behavior
type testableEmbedder struct {
	*LocalGGUFEmbedder
	mock *mockModel
}

func newTestableEmbedder(dimensions int) *testableEmbedder {
	mock := &mockModel{dimensions: dimensions}

	embedder := &LocalGGUFEmbedder{
		modelName:     "test-model",
		modelPath:     "/test/path/model.gguf",
		stopWarmup:    make(chan struct{}),
		warmupStopped: make(chan struct{}),
	}

	// Close the warmupStopped channel since we're not running warmup in tests
	close(embedder.warmupStopped)

	return &testableEmbedder{
		LocalGGUFEmbedder: embedder,
		mock:              mock,
	}
}

// embedWithMock uses the mock model instead of the real one
func (t *testableEmbedder) embedWithMock(ctx context.Context, text string) (embedding []float32, err error) {
	// This mimics embedWithRecovery but uses our mock
	defer func() {
		if r := recover(); r != nil {
			t.panicCount.Add(1)
			err = &panicError{recovered: r}
		}
	}()

	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil, errClosed
	}
	t.mu.RUnlock()

	embedding, err = t.mock.Embed(ctx, text)
	if err != nil {
		t.errorCount.Add(1)
	}
	return embedding, err
}

// Custom error types for testing
type panicError struct {
	recovered interface{}
}

func (e *panicError) Error() string {
	return "panic recovered"
}

var errClosed = &closedError{}

type closedError struct{}

func (e *closedError) Error() string {
	return "embedder is closed"
}

// --- Tests ---

func TestContainsGPUError(t *testing.T) {
	tests := []struct {
		name     string
		errStr   string
		expected bool
	}{
		{"CUDA error", "CUDA error: out of memory", true},
		{"cuda lowercase", "cuda allocation failed", true},
		{"GPU uppercase", "GPU memory exhausted", true},
		{"gpu lowercase", "gpu device not found", true},
		{"Metal error", "Metal: command buffer error", true},
		{"metal lowercase", "metal shader compilation failed", true},
		{"OOM", "out of memory", true},
		{"device error", "device driver version mismatch", true},
		{"cublas error", "cublas initialization failed", true},
		{"cudnn error", "cudnn: invalid handle", true},
		{"allocation", "allocation failed for tensor", true},
		{"driver error", "driver not loaded", true},
		{"regular error", "file not found", false},
		{"network error", "connection refused", false},
		{"timeout", "context deadline exceeded", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containsGPUError(tt.errStr)
			if result != tt.expected {
				t.Errorf("containsGPUError(%q) = %v, want %v", tt.errStr, result, tt.expected)
			}
		})
	}
}

func TestContainsHelper(t *testing.T) {
	tests := []struct {
		s      string
		substr string
		want   bool
	}{
		{"hello world", "world", true},
		{"hello world", "hello", true},
		{"hello world", "lo wo", true},
		{"hello world", "foo", false},
		{"hello", "hello world", false},
		{"", "foo", false},
		{"foo", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.s+"_"+tt.substr, func(t *testing.T) {
			got := contains(tt.s, tt.substr)
			if got != tt.want {
				t.Errorf("contains(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.want)
			}
		})
	}
}

func TestEmbedderStats(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	// Initial stats should be zero
	stats := embedder.Stats()
	if stats.EmbedCount != 0 {
		t.Errorf("initial EmbedCount = %d, want 0", stats.EmbedCount)
	}
	if stats.ErrorCount != 0 {
		t.Errorf("initial ErrorCount = %d, want 0", stats.ErrorCount)
	}
	if stats.PanicCount != 0 {
		t.Errorf("initial PanicCount = %d, want 0", stats.PanicCount)
	}
	if stats.ModelName != "test-model" {
		t.Errorf("ModelName = %q, want %q", stats.ModelName, "test-model")
	}
	if stats.ModelPath != "/test/path/model.gguf" {
		t.Errorf("ModelPath = %q, want %q", stats.ModelPath, "/test/path/model.gguf")
	}
	if !stats.LastEmbedTime.IsZero() {
		t.Errorf("initial LastEmbedTime should be zero, got %v", stats.LastEmbedTime)
	}

	// Simulate some activity
	embedder.embedCount.Add(5)
	embedder.errorCount.Add(2)
	embedder.panicCount.Add(1)
	embedder.lastEmbedTime.Store(time.Now().Unix())

	stats = embedder.Stats()
	if stats.EmbedCount != 5 {
		t.Errorf("EmbedCount = %d, want 5", stats.EmbedCount)
	}
	if stats.ErrorCount != 2 {
		t.Errorf("ErrorCount = %d, want 2", stats.ErrorCount)
	}
	if stats.PanicCount != 1 {
		t.Errorf("PanicCount = %d, want 1", stats.PanicCount)
	}
	if stats.LastEmbedTime.IsZero() {
		t.Error("LastEmbedTime should not be zero after activity")
	}
}

func TestPanicRecovery(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	// Configure mock to panic
	embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
		panic("simulated CGO crash")
	}

	// Embed should recover from panic and return error
	_, err := embedder.embedWithMock(context.Background(), "test input")

	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}

	if _, ok := err.(*panicError); !ok {
		t.Errorf("expected panicError, got %T: %v", err, err)
	}

	// Panic count should be incremented
	if embedder.panicCount.Load() != 1 {
		t.Errorf("panicCount = %d, want 1", embedder.panicCount.Load())
	}
}

func TestPanicRecoveryMultiple(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	panicCount := 0
	embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
		panicCount++
		if panicCount <= 3 {
			panic("simulated crash #" + string(rune('0'+panicCount)))
		}
		return make([]float32, 1024), nil
	}

	// First 3 calls should panic and recover
	for i := 0; i < 3; i++ {
		_, err := embedder.embedWithMock(context.Background(), "test")
		if err == nil {
			t.Errorf("call %d: expected error, got nil", i+1)
		}
	}

	// Fourth call should succeed
	result, err := embedder.embedWithMock(context.Background(), "test")
	if err != nil {
		t.Errorf("call 4: expected success, got error: %v", err)
	}
	if len(result) != 1024 {
		t.Errorf("call 4: expected 1024 dimensions, got %d", len(result))
	}

	if embedder.panicCount.Load() != 3 {
		t.Errorf("panicCount = %d, want 3", embedder.panicCount.Load())
	}
}

func TestErrorCounting(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	// Configure mock to return error
	testErr := &testError{msg: "model inference failed"}
	embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
		return nil, testErr
	}

	_, err := embedder.embedWithMock(context.Background(), "test")
	if err != testErr {
		t.Errorf("expected testErr, got %v", err)
	}

	if embedder.errorCount.Load() != 1 {
		t.Errorf("errorCount = %d, want 1", embedder.errorCount.Load())
	}

	// Panic count should not be affected
	if embedder.panicCount.Load() != 0 {
		t.Errorf("panicCount = %d, want 0", embedder.panicCount.Load())
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestClosedEmbedder(t *testing.T) {
	embedder := newTestableEmbedder(1024)

	// Close the embedder
	embedder.mu.Lock()
	embedder.closed = true
	embedder.mu.Unlock()

	// Embed should return closed error
	_, err := embedder.embedWithMock(context.Background(), "test")
	if err != errClosed {
		t.Errorf("expected errClosed, got %v", err)
	}
}

func TestConcurrentEmbedding(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	// Configure mock for concurrent access
	var callCount atomic.Int32
	embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
		callCount.Add(1)
		time.Sleep(10 * time.Millisecond) // Simulate work
		return make([]float32, 1024), nil
	}

	// Run concurrent embeddings
	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errors := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			_, err := embedder.embedWithMock(context.Background(), "concurrent test")
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent embedding failed: %v", err)
	}

	if callCount.Load() != goroutines {
		t.Errorf("callCount = %d, want %d", callCount.Load(), goroutines)
	}
}

func TestConcurrentPanicRecovery(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	// Configure mock to panic on some calls
	var callCount atomic.Int32
	embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
		count := callCount.Add(1)
		if count%3 == 0 {
			panic("periodic panic")
		}
		return make([]float32, 1024), nil
	}

	// Run concurrent embeddings - some will panic
	const goroutines = 30
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			// Don't check error - we expect some panics
			embedder.embedWithMock(context.Background(), "test")
		}()
	}

	wg.Wait()

	// Should have approximately 10 panics (every 3rd call)
	panics := embedder.panicCount.Load()
	if panics < 5 || panics > 15 {
		t.Errorf("panicCount = %d, expected ~10 (range 5-15)", panics)
	}
}

func TestGPUErrorDetection(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	gpuErrors := []string{
		"CUDA error: out of memory",
		"Metal: command buffer execution failed",
		"GPU memory allocation failed",
		"cublas: invalid handle",
	}

	for _, errMsg := range gpuErrors {
		t.Run(errMsg, func(t *testing.T) {
			embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
				return nil, &testError{msg: errMsg}
			}

			_, err := embedder.embedWithMock(context.Background(), "test")
			if err == nil {
				t.Error("expected error")
			}

			// Verify the error contains the GPU-related message
			if !strings.Contains(err.Error(), errMsg[:4]) {
				t.Errorf("error should contain GPU message, got: %v", err)
			}
		})
	}
}

// EmbedBatchWithMock overrides EmbedBatch to use the mock model
func (t *testableEmbedder) EmbedBatchWithMock(ctx context.Context, texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	var firstErr error

	for i, text := range texts {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		emb, err := t.embedWithMock(ctx, text)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("text %d: %w", i, err)
			}
			// Continue processing other texts even if one fails
			continue
		}
		results[i] = emb
	}

	// Return first error if any, but still return partial results
	return results, firstErr
}

func TestEmbedBatchPartialFailure(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	t.Run("all_succeed", func(t *testing.T) {
		// All embeddings should succeed
		texts := []string{"text1", "text2", "text3"}
		results, err := embedder.EmbedBatchWithMock(context.Background(), texts)

		require.NoError(t, err, "Should not return error when all succeed")
		require.Equal(t, len(texts), len(results), "Should return results for all texts")

		for i, result := range results {
			require.NotNil(t, result, "Result %d should not be nil", i)
			require.Equal(t, 1024, len(result), "Result %d should have 1024 dimensions", i)
		}
	})

	t.Run("first_fails", func(t *testing.T) {
		// First embedding fails, others succeed
		callCount := 0
		testErr := &testError{msg: "first embedding failed"}
		embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
			callCount++
			if callCount == 1 {
				return nil, testErr
			}
			return make([]float32, 1024), nil
		}

		texts := []string{"text1", "text2", "text3"}
		results, err := embedder.EmbedBatchWithMock(context.Background(), texts)

		require.Error(t, err, "Should return error when first fails")
		require.Contains(t, err.Error(), "text 0", "Error should mention failed text index")
		require.Equal(t, len(texts), len(results), "Should return results array for all texts")

		require.Nil(t, results[0], "First result should be nil (failed)")
		require.NotNil(t, results[1], "Second result should not be nil")
		require.NotNil(t, results[2], "Third result should not be nil")
		require.Equal(t, 1024, len(results[1]), "Second result should have 1024 dimensions")
		require.Equal(t, 1024, len(results[2]), "Third result should have 1024 dimensions")
	})

	t.Run("middle_fails", func(t *testing.T) {
		// Middle embedding fails, others succeed
		callCount := 0
		testErr := &testError{msg: "middle embedding failed"}
		embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
			callCount++
			if callCount == 2 {
				return nil, testErr
			}
			return make([]float32, 1024), nil
		}

		texts := []string{"text1", "text2", "text3"}
		results, err := embedder.EmbedBatchWithMock(context.Background(), texts)

		require.Error(t, err, "Should return error when middle fails")
		require.Contains(t, err.Error(), "text 1", "Error should mention failed text index")
		require.Equal(t, len(texts), len(results), "Should return results array for all texts")

		require.NotNil(t, results[0], "First result should not be nil")
		require.Nil(t, results[1], "Second result should be nil (failed)")
		require.NotNil(t, results[2], "Third result should not be nil")
		require.Equal(t, 1024, len(results[0]), "First result should have 1024 dimensions")
		require.Equal(t, 1024, len(results[2]), "Third result should have 1024 dimensions")
	})

	t.Run("last_fails", func(t *testing.T) {
		// Last embedding fails, others succeed
		callCount := 0
		testErr := &testError{msg: "last embedding failed"}
		embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
			callCount++
			if callCount == 3 {
				return nil, testErr
			}
			return make([]float32, 1024), nil
		}

		texts := []string{"text1", "text2", "text3"}
		results, err := embedder.EmbedBatchWithMock(context.Background(), texts)

		require.Error(t, err, "Should return error when last fails")
		require.Contains(t, err.Error(), "text 2", "Error should mention failed text index")
		require.Equal(t, len(texts), len(results), "Should return results array for all texts")

		require.NotNil(t, results[0], "First result should not be nil")
		require.NotNil(t, results[1], "Second result should not be nil")
		require.Nil(t, results[2], "Third result should be nil (failed)")
		require.Equal(t, 1024, len(results[0]), "First result should have 1024 dimensions")
		require.Equal(t, 1024, len(results[1]), "Second result should have 1024 dimensions")
	})

	t.Run("multiple_failures", func(t *testing.T) {
		// Multiple embeddings fail, but processing continues
		callCount := 0
		testErr := &testError{msg: "embedding failed"}
		embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
			callCount++
			if callCount == 1 || callCount == 3 {
				return nil, testErr
			}
			return make([]float32, 1024), nil
		}

		texts := []string{"text1", "text2", "text3", "text4"}
		results, err := embedder.EmbedBatchWithMock(context.Background(), texts)

		require.Error(t, err, "Should return error when some fail")
		require.Contains(t, err.Error(), "text 0", "Error should mention first failed text index")
		require.Equal(t, len(texts), len(results), "Should return results array for all texts")

		require.Nil(t, results[0], "First result should be nil (failed)")
		require.NotNil(t, results[1], "Second result should not be nil")
		require.Nil(t, results[2], "Third result should be nil (failed)")
		require.NotNil(t, results[3], "Fourth result should not be nil")
		require.Equal(t, 1024, len(results[1]), "Second result should have 1024 dimensions")
		require.Equal(t, 1024, len(results[3]), "Fourth result should have 1024 dimensions")
	})

	t.Run("all_fail", func(t *testing.T) {
		// All embeddings fail
		testErr := &testError{msg: "all embeddings failed"}
		embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
			return nil, testErr
		}

		texts := []string{"text1", "text2", "text3"}
		results, err := embedder.EmbedBatchWithMock(context.Background(), texts)

		require.Error(t, err, "Should return error when all fail")
		require.Contains(t, err.Error(), "text 0", "Error should mention first failed text index")
		require.Equal(t, len(texts), len(results), "Should return results array for all texts")

		require.Nil(t, results[0], "All results should be nil")
		require.Nil(t, results[1], "All results should be nil")
		require.Nil(t, results[2], "All results should be nil")
	})

	t.Run("panic_recovery", func(t *testing.T) {
		// One embedding panics, others succeed
		callCount := 0
		embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
			callCount++
			if callCount == 2 {
				panic("simulated CGO crash")
			}
			return make([]float32, 1024), nil
		}

		texts := []string{"text1", "text2", "text3"}
		results, err := embedder.EmbedBatchWithMock(context.Background(), texts)

		require.Error(t, err, "Should return error when panic occurs")
		require.Contains(t, err.Error(), "text 1", "Error should mention failed text index")
		require.Equal(t, len(texts), len(results), "Should return results array for all texts")

		require.NotNil(t, results[0], "First result should not be nil")
		require.Nil(t, results[1], "Second result should be nil (panic)")
		require.NotNil(t, results[2], "Third result should not be nil")
		require.Equal(t, 1, int(embedder.panicCount.Load()), "Panic count should be 1")
	})

	t.Run("context_cancellation", func(t *testing.T) {
		// Context cancellation should stop processing
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		texts := []string{"text1", "text2", "text3"}
		results, err := embedder.EmbedBatchWithMock(ctx, texts)

		require.Error(t, err, "Should return error on context cancellation")
		require.Equal(t, context.Canceled, err, "Error should be context.Canceled")
		require.Nil(t, results, "Results should be nil on context cancellation")
	})

	t.Run("empty_batch", func(t *testing.T) {
		// Empty batch should return empty results
		texts := []string{}
		results, err := embedder.EmbedBatchWithMock(context.Background(), texts)

		require.NoError(t, err, "Should not return error for empty batch")
		require.Equal(t, 0, len(results), "Should return empty results")
	})

	t.Run("single_text", func(t *testing.T) {
		// Single text should work correctly
		texts := []string{"single text"}
		results, err := embedder.EmbedBatchWithMock(context.Background(), texts)

		require.NoError(t, err, "Should not return error for single text")
		require.Equal(t, 1, len(results), "Should return one result")
		require.NotNil(t, results[0], "Result should not be nil")
		require.Equal(t, 1024, len(results[0]), "Result should have 1024 dimensions")
	})
}

func TestWarmupSkipsRecentActivity(t *testing.T) {
	embedder := newTestableEmbedder(1024)

	// Set recent activity
	embedder.lastEmbedTime.Store(time.Now().Unix())

	// Verify the warmup logic would skip (we can't easily test the goroutine)
	lastEmbed := time.Unix(embedder.lastEmbedTime.Load(), 0)
	interval := 5 * time.Minute

	if time.Since(lastEmbed) >= interval/2 {
		t.Error("lastEmbed should be recent, warmup should skip")
	}

	embedder.Close()
}

func TestCloseStopsWarmup(t *testing.T) {
	// Create embedder with warmup channels properly set up
	embedder := &LocalGGUFEmbedder{
		warmupStarted: true,
		modelName:     "test-model",
		modelPath:     "/test/path/model.gguf",
		stopWarmup:    make(chan struct{}),
		warmupStopped: make(chan struct{}),
	}

	// Simulate warmup goroutine
	go func() {
		<-embedder.stopWarmup
		close(embedder.warmupStopped)
	}()

	// Close should complete without hanging
	done := make(chan struct{})
	go func() {
		embedder.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Close() timed out - warmup stop may be stuck")
	}
}

func TestCloseIdempotent(t *testing.T) {
	embedder := newTestableEmbedder(1024)

	// First close
	err1 := embedder.Close()
	if err1 != nil {
		t.Errorf("first Close() error: %v", err1)
	}

	// Second close should be no-op
	err2 := embedder.Close()
	if err2 != nil {
		t.Errorf("second Close() error: %v", err2)
	}
}

func TestStatsConcurrentAccess(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				embedder.embedCount.Add(1)
				embedder.errorCount.Add(1)
				embedder.panicCount.Add(1)
				embedder.lastEmbedTime.Store(time.Now().Unix())
			}
		}()
	}

	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = embedder.Stats()
			}
		}()
	}

	wg.Wait()

	// Verify final counts
	stats := embedder.Stats()
	if stats.EmbedCount != 500 {
		t.Errorf("EmbedCount = %d, want 500", stats.EmbedCount)
	}
	if stats.ErrorCount != 500 {
		t.Errorf("ErrorCount = %d, want 500", stats.ErrorCount)
	}
	if stats.PanicCount != 500 {
		t.Errorf("PanicCount = %d, want 500", stats.PanicCount)
	}
}

func TestPanicWithDifferentTypes(t *testing.T) {
	embedder := newTestableEmbedder(1024)
	defer embedder.Close()

	panicValues := []interface{}{
		"string panic",
		42,
		struct{ msg string }{"struct panic"},
		&testError{msg: "error panic"},
	}

	for i, panicVal := range panicValues {
		t.Run("panic_type_"+string(rune('0'+i)), func(t *testing.T) {
			embedder.mock.embedFunc = func(ctx context.Context, text string) ([]float32, error) {
				panic(panicVal)
			}

			_, err := embedder.embedWithMock(context.Background(), "test")
			if err == nil {
				t.Error("expected error from panic recovery")
			}
		})
	}

	if embedder.panicCount.Load() != int64(len(panicValues)) {
		t.Errorf("panicCount = %d, want %d", embedder.panicCount.Load(), len(panicValues))
	}
}
