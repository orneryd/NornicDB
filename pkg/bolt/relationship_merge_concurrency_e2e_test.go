package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	neo4jconfig "github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBoltConcurrentBareRelationshipMergeConverges reproduces issue #357:
// two managed transactions begin before either runs the same relationship
// MERGE between bound nodes. Both managed writes must succeed, with any
// transient conflict retried by the driver, and the final graph must contain
// one relationship.
func TestBoltConcurrentBareRelationshipMergeConverges(t *testing.T) {
	baseStore := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = baseStore.Close() })
	store := storage.NewNamespacedEngine(baseStore, "neo4j")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
		func(config *neo4jconfig.Config) {
			config.MaxTransactionRetryTime = 5 * time.Second
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	require.NoError(t, driver.VerifyConnectivity(ctx))

	setup := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
	result, err := setup.Run(ctx, `CREATE (:Workload {id: 'source'}), (:Workload {id: 'target'})`, nil)
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, setup.Close(ctx))

	const query = `MATCH (source:Workload {id: $s})
MATCH (target:Workload {id: $t})
MERGE (source)-[rel:DEPENDS_ON]->(target)
SET rel.confidence = 0.9`
	params := map[string]any{"s": "source", "t": "target"}

	type writerResult struct {
		writer int
		err    error
	}
	ready := make(chan int, 2)
	start := make(chan struct{})
	staged := make(chan int, 2)
	commit := make(chan struct{})
	done := make(chan writerResult, 2)
	var attempts [2]atomic.Int32

	for writer := range 2 {
		go func() {
			session := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
			defer func() { _ = session.Close(ctx) }()

			_, executeErr := session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
				firstAttempt := attempts[writer].Add(1) == 1
				if firstAttempt {
					pinned, pinErr := tx.Run(ctx, `MATCH (:Workload {id: $s}), (:Workload {id: $t}) RETURN count(*)`, params)
					if pinErr != nil {
						return nil, pinErr
					}
					if _, pinErr = pinned.Consume(ctx); pinErr != nil {
						return nil, pinErr
					}
					ready <- writer
					select {
					case <-start:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				result, runErr := tx.Run(ctx, query, params)
				if runErr != nil {
					return nil, runErr
				}
				_, consumeErr := result.Consume(ctx)
				if consumeErr != nil {
					return nil, consumeErr
				}
				if firstAttempt {
					staged <- writer
					select {
					case <-commit:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return nil, nil
			})
			done <- writerResult{writer: writer, err: executeErr}
		}()
	}

	for range 2 {
		select {
		case <-ready:
		case early := <-done:
			close(start)
			t.Fatalf("writer %d completed before the concurrency barrier: %v", early.writer, early.err)
		case <-ctx.Done():
			close(start)
			t.Fatal(ctx.Err())
		}
	}
	close(start)
	for range 2 {
		select {
		case <-staged:
		case early := <-done:
			close(commit)
			t.Fatalf("writer %d completed before both MERGEs were staged: %v", early.writer, early.err)
		case <-ctx.Done():
			close(commit)
			t.Fatal(ctx.Err())
		}
	}
	close(commit)
	for range 2 {
		select {
		case writer := <-done:
			require.NoErrorf(t, writer.err, "writer %d", writer.writer)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	require.GreaterOrEqual(t, attempts[0].Load()+attempts[1].Load(), int32(2))

	check := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer func() { _ = check.Close(ctx) }()
	result, err = check.Run(ctx, `MATCH (:Workload {id: 'source'})-[rel:DEPENDS_ON]->(:Workload {id: 'target'})
RETURN count(rel)`, nil)
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, record.Values[0])
}
