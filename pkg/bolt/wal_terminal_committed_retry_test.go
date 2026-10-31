// Author: Claude, NOR-180 committed-outcome retry contract, 30 October 2026.
// Uses the outcome matrix's fixtures, so it runs on release as a comparison.
package bolt

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A transaction whose work committed natively, and whose post-commit action
// then fails with a transient storage failure, must not be retried by the
// real driver's managed transaction: a retry repeats work that is stored.
// The driver decides on the status code alone, so the reply's code must not
// be retriable, while the reply still carries the primary and, when the
// COMMIT record failed too, the committed WAL diagnostic.
func TestBoltWALTerminalCommittedOutcomeIsNotRetried(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%v", closed), func(t *testing.T) {
			wal, err := storage.NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			engine := &boltMatrixEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), wal: wal}
			if closed {
				// Close the logical WAL right after the native commit, before
				// the post-commit action and the COMMIT record.
				engine.commit = func(ctx context.Context, tx *storage.BadgerTransaction) error {
					err := tx.CommitContext(ctx)
					if err == nil {
						_ = wal.Close()
					}
					return err
				}
			}
			store := storage.NewNamespacedEngine(engine, "nornic")
			_, err = store.CreateNode(&storage.Node{ID: "committed-retry", Labels: []string{"WalOutcome"}, Properties: map[string]any{"writes": int64(0)}})
			require.NoError(t, err)
			port := startTransactionLifecycleServerWithStore(t, &boltMatrixSuppression{Engine: store, cause: storage.ErrStorageRestoring})

			ctx := context.Background()
			driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth(),
				func(config *neo4jdriver.Config) { config.MaxTransactionRetryTime = 3 * time.Second })
			require.NoError(t, err)
			t.Cleanup(func() { _ = driver.Close(context.Background()) })
			session := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: "nornic"})
			t.Cleanup(func() { _ = session.Close(context.Background()) })

			attempts := 0
			_, err = session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
				attempts++
				for _, query := range []string{
					"MATCH (n:WalOutcome) SET n.writes = n.writes + 1",
					"MATCH (n:WalOutcome) CALL kp.suppress(n)",
				} {
					result, runErr := tx.Run(ctx, query, nil)
					if runErr != nil {
						return nil, runErr
					}
					if _, runErr = result.Consume(ctx); runErr != nil {
						return nil, runErr
					}
				}
				return nil, nil
			})
			require.Error(t, err, "the post-commit action failed")
			// The server may close the connection after an unknown COMMIT
			// outcome; the driver then reports the FAILURE inside its own
			// commit-on-dead-connection error, which it never retries either.
			var reply *neo4jdriver.Neo4jError
			if errors.As(err, &reply) {
				require.False(t, reply.IsRetriable(), "the driver must not retry committed work: %s", reply.Code)
			}
			require.Equal(t, 1, attempts, "the managed transaction ran its work once: %v", err)
			require.Contains(t, err.Error(), storage.ErrStorageRestoring.Error(), "the reply keeps the primary")
			if closed {
				require.Contains(t, err.Error(), "committed, but failed to write WAL tx commit")
			}

			// Native truth: the work is stored exactly once.
			rows, err := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine.MemoryEngine, "nornic")).Execute(ctx, "MATCH (n:WalOutcome) RETURN n.writes", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, rows.Rows)
		})
	}
}
