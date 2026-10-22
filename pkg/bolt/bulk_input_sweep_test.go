package bolt

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// bulkInputExecutor reports when the server starts the next statement and
// when it has ended it.
type bulkInputExecutor struct {
	executor *cypher.StorageExecutor
	mu       sync.Mutex
	started  func()
	done     chan struct{}
}

func (b *bulkInputExecutor) arm(started func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.started, b.done = started, make(chan struct{})
}

// wait returns once the armed statement has ended on the server (at once
// when the server never started it).
func (b *bulkInputExecutor) wait() {
	b.mu.Lock()
	unstarted, done := b.started != nil, b.done
	b.started = nil
	b.mu.Unlock()
	if !unstarted {
		<-done
	}
}

func (b *bulkInputExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	b.mu.Lock()
	started, done := b.started, b.done
	b.started = nil
	b.mu.Unlock()
	if started != nil {
		started()
		defer close(done)
	}
	result, err := b.executor.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return &QueryResult{Columns: result.Columns, Rows: result.Rows, Metadata: result.Metadata}, nil
}

// The generated bulk-input sweep over Bolt: a client's RESET, Bolt's
// cancellation, ends the server's running statement promptly for every
// registered function and procedure that takes a list, a map or any value.
func TestBoltBulkInputCancellationSweep(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = store.Close() })
	executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "test"))
	size := testutil.DefaultBulkInputSize
	rebuild := func(t *testing.T) {
		for _, statement := range testutil.BulkInputFixture(size) {
			_, err := executor.Execute(context.Background(), statement, nil)
			require.NoError(t, err)
		}
	}
	rebuild(t)
	calls := testutil.BulkInputCalls(t, executor, size)
	require.NotEmpty(t, calls)

	tracked := &bulkInputExecutor{executor: executor}
	server := New(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10}, tracked)
	port := serveBoltIntegrationServer(t, server)

	testutil.RunBulkInputSweep(t, calls, func(ctx context.Context, query string, params map[string]interface{}, started func()) error {
		tracked.arm(started)
		defer tracked.wait()
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return err
		}
		defer conn.Close()
		if err := PerformHandshakeWithTesting(t, conn); err != nil {
			return err
		}
		if err := SendHello(t, conn, nil); err != nil {
			return err
		}
		if err := ReadSuccess(t, conn); err != nil {
			return err
		}
		if err := SendMessage(conn, BuildRunMessage(query, params, nil)); err != nil {
			return err
		}
		response := make(chan error, 1)
		go func() {
			_, _, err := ReadMessage(conn)
			response <- err
		}()
		select {
		case err := <-response:
			return err
		case <-ctx.Done():
			if err := SendMessage(conn, BuildResetMessage()); err != nil {
				return err
			}
			return ctx.Err()
		}
	}, testutil.DefaultBulkInputBudget, rebuild)
}
