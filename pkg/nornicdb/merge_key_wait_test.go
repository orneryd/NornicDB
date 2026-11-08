package nornicdb

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestInMemoryDatabaseBoundsMergeKeyWaits (NOR-284, review round 1): a
// database opened in memory takes the same engine-wide policy as a
// persistent one, so a MERGE waiting for a node key another transaction
// holds ends at the configured transaction timeout, auto-commit or explicit.
func TestInMemoryDatabaseBoundsMergeKeyWaits(t *testing.T) {
	const timeout = 100 * time.Millisecond
	config := DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.TransactionTimeout = timeout
	db, err := Open("", config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecuteCypher(t.Context(), "CREATE CONSTRAINT u_k FOR (n:U) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	holder := cypher.NewStorageExecutor(db.GetStorage())
	_, err = holder.Execute(t.Context(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
	_, err = holder.Execute(t.Context(), "MERGE (:U {k: 1})", nil)
	require.NoError(t, err)
	for _, explicit := range []bool{false, true} {
		waiter := cypher.NewStorageExecutor(db.GetStorage())
		if explicit {
			_, err = waiter.Execute(t.Context(), "BEGIN", nil)
			require.NoError(t, err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		started := time.Now()
		_, err = waiter.Execute(ctx, "MERGE (:U {k: 1})", nil)
		elapsed := time.Since(started)
		cancel()
		if explicit {
			_, _ = waiter.Execute(context.Background(), "ROLLBACK", nil)
		}
		require.ErrorIs(t, err, storage.ErrLockWaitTimeout, "explicit=%v after %v", explicit, elapsed)
		require.Less(t, elapsed, 4*time.Second, "explicit=%v", explicit)
	}
}

// TestZeroTransactionTimeoutLeavesMergeWaitsUnbounded (NOR-284, review round
// 2): NORNICDB_TRANSACTION_TIMEOUT=0 disables the transaction timeout, over a
// shorter configured value and the legacy variable, so a MERGE waiting for a
// node key another transaction holds keeps waiting until the holder ends and
// then proceeds, in a persistent and an in-memory database.
func TestZeroTransactionTimeoutLeavesMergeWaitsUnbounded(t *testing.T) {
	t.Setenv("NORNICDB_TRANSACTION_TIMEOUT", "0s")
	t.Setenv("NEO4J_dbms_transaction_timeout", "100ms")
	for _, dataDir := range []string{t.TempDir(), ""} {
		cfg := DefaultConfig()
		cfg.Memory.DecayEnabled = false
		cfg.Memory.AutoLinksEnabled = false
		cfg.Database.AsyncWritesEnabled = false
		cfg.Database.TransactionTimeout = 100 * time.Millisecond // the file's value, which the environment overrides
		require.NoError(t, config.ApplyEnvVars(cfg))
		db, err := Open(dataDir, cfg)
		require.NoError(t, err)
		ctx := context.Background()
		_, err = db.ExecuteCypher(ctx, "CREATE CONSTRAINT zero_k FOR (n:Zero) REQUIRE n.k IS UNIQUE", nil)
		require.NoError(t, err)
		holder := cypher.NewStorageExecutor(db.GetStorage())
		_, err = holder.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = holder.Execute(ctx, "MERGE (:Zero {k: 1})", nil)
		require.NoError(t, err)
		waits := make(chan struct{}, 1)
		restore := storage.SetUniqueKeyLockWaitHookForTesting(func() {
			select {
			case waits <- struct{}{}:
			default:
			}
		})
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		done := make(chan error, 1)
		go func() {
			_, err := db.ExecuteCypher(waitCtx, "MERGE (:Zero {k: 1})", nil)
			done <- err
		}()
		select {
		case <-waits:
		case err := <-done:
			t.Fatalf("dataDir=%q: the MERGE ended before waiting for the key: %v", dataDir, err)
		}
		time.Sleep(300 * time.Millisecond) // longer than the overridden 100ms
		_, err = holder.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
		require.NoError(t, <-done, "dataDir=%q: NORNICDB_TRANSACTION_TIMEOUT=0 must leave the wait unbounded (resolved timeout %v)", dataDir, cfg.Database.TransactionTimeout)
		require.Zero(t, cfg.Database.TransactionTimeout)
		cancel()
		restore()
		require.NoError(t, db.Close())
	}
}

// lockWaitWarnings collects a database's JSON log and counts its prolonged
// MERGE-wait warnings.
type lockWaitWarnings struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockWaitWarnings) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockWaitWarnings) count(t *testing.T) int {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	scanner := bufio.NewScanner(bytes.NewReader(w.buf.Bytes()))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["event_id"] == "storage.transaction.lock_wait_prolonged" {
			n++
		}
	}
	return n
}

// TestZeroSlowQueryThresholdSilencesMergeWaitWarnings (NOR-284, maintainer
// review of #1756): NORNICDB_SLOW_QUERY_THRESHOLD=0 disables the prolonged
// MERGE-wait warning over a shorter configured threshold, while the
// transaction timeout still ends the wait; with the threshold left on, the
// same wait logs its one warning. Persistent and in-memory databases.
func TestZeroSlowQueryThresholdSilencesMergeWaitWarnings(t *testing.T) {
	t.Setenv("NORNICDB_TRANSACTION_TIMEOUT", "")
	t.Setenv("NEO4J_dbms_transaction_timeout", "")
	for _, envThreshold := range []string{"0s", ""} {
		t.Setenv("NORNICDB_SLOW_QUERY_THRESHOLD", envThreshold)
		for _, dataDir := range []string{t.TempDir(), ""} {
			warnings := &lockWaitWarnings{}
			cfg := DefaultConfig()
			cfg.Memory.DecayEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Database.AsyncWritesEnabled = false
			cfg.Database.TransactionTimeout = 400 * time.Millisecond
			cfg.Logging.SlowQueryThreshold = 50 * time.Millisecond // the file's value
			cfg.Logger = slog.New(slog.NewJSONHandler(warnings, &slog.HandlerOptions{Level: slog.LevelDebug}))
			require.NoError(t, config.ApplyEnvVars(cfg))
			db, err := Open(dataDir, cfg)
			require.NoError(t, err)
			ctx := context.Background()
			_, err = db.ExecuteCypher(ctx, "CREATE CONSTRAINT quiet_k FOR (n:Quiet) REQUIRE n.k IS UNIQUE", nil)
			require.NoError(t, err)
			holder := cypher.NewStorageExecutor(db.GetStorage())
			_, err = holder.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = holder.Execute(ctx, "MERGE (:Quiet {k: 1})", nil)
			require.NoError(t, err)
			waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			started := time.Now()
			_, err = db.ExecuteCypher(waitCtx, "MERGE (:Quiet {k: 1})", nil)
			elapsed := time.Since(started)
			cancel()
			require.ErrorIs(t, err, storage.ErrLockWaitTimeout, "env=%q dataDir=%q: the transaction timeout still ends the wait", envThreshold, dataDir)
			require.GreaterOrEqual(t, elapsed, 400*time.Millisecond)
			_, err = holder.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
			want := 1
			if envThreshold != "" {
				want = 0
			}
			require.Equal(t, want, warnings.count(t), "env=%q dataDir=%q: prolonged-wait warnings (resolved threshold %v)", envThreshold, dataDir, cfg.Logging.SlowQueryThreshold)
			require.NoError(t, db.Close())
		}
	}
}
