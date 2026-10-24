package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestConcurrentTransactionsCreatingSameLabelCommitIndependently(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	const rounds = 16
	for round := 0; round < rounds; round++ {
		transactions := make([]*BadgerTransaction, 2)
		for writer := range transactions {
			transactions[writer], err = engine.BeginTransaction()
			require.NoError(t, err)
			_, err = transactions[writer].CreateNode(&Node{
				ID:     NodeID(fmt.Sprintf("db1:concurrent-%d-%d", round, writer)),
				Labels: []string{"Work"},
			})
			require.NoError(t, err)
		}

		for _, commitErr := range commitTransactionsConcurrently(transactions) {
			require.NoError(t, commitErr)
		}
	}

	count, err := engine.NodeCountByLabelInNamespace("db1", "Work")
	require.NoError(t, err)
	require.Equal(t, int64(rounds*2), count)
}

func TestConcurrentTransactionsChangingSameLabelsCommitIndependently(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	for i := 0; i < 2; i++ {
		_, err = engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("db1:update-%d", i)), Labels: []string{"Pending"}})
		require.NoError(t, err)
	}

	transactions := make([]*BadgerTransaction, 2)
	for writer := range transactions {
		transactions[writer], err = engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, transactions[writer].UpdateNode(&Node{
			ID:     NodeID(fmt.Sprintf("db1:update-%d", writer)),
			Labels: []string{"Complete"},
		}))
	}
	for _, commitErr := range commitTransactionsConcurrently(transactions) {
		require.NoError(t, commitErr)
	}

	pending, err := engine.NodeCountByLabelInNamespace("db1", "Pending")
	require.NoError(t, err)
	require.Zero(t, pending)
	complete, err := engine.NodeCountByLabelInNamespace("db1", "Complete")
	require.NoError(t, err)
	require.Equal(t, int64(2), complete)
}

func TestConflictingTransactionDoesNotPublishLabelCountDelta(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, err = engine.CreateNode(&Node{ID: "db1:same-node", Labels: []string{"Pending"}})
	require.NoError(t, err)

	transactions := make([]*BadgerTransaction, 2)
	for writer := range transactions {
		transactions[writer], err = engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, transactions[writer].UpdateNode(&Node{ID: "db1:same-node", Labels: []string{"Complete"}}))
	}

	commitErrors := commitTransactionsConcurrently(transactions)
	successes := 0
	for _, commitErr := range commitErrors {
		if commitErr == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)

	count, err := engine.NodeCountByLabelInNamespace("db1", "Pending")
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = engine.NodeCountByLabelInNamespace("db1", "Complete")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestTransactionRepairsMalformedDerivedLabelCountAfterCommit(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		malformed := make([]byte, 4)
		binary.BigEndian.PutUint32(malformed, 99)
		return txn.Set(labelCountKey("db1", "Work"), malformed)
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "db1:repair-count", Labels: []string{"Work"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	count, err := engine.NodeCountByLabelInNamespace("db1", "Work")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestRolledBackTransactionDoesNotPublishLabelCountDelta(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "db1:rolled-back", Labels: []string{"Work"}})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	count, err := engine.NodeCountByLabelInNamespace("db1", "Work")
	require.NoError(t, err)
	require.Zero(t, count)
}

func commitTransactionsConcurrently(transactions []*BadgerTransaction) []error {
	start := make(chan struct{})
	errors := make([]error, len(transactions))
	var commits sync.WaitGroup
	for writer, tx := range transactions {
		commits.Add(1)
		go func() {
			defer commits.Done()
			<-start
			errors[writer] = tx.Commit()
		}()
	}
	close(start)
	commits.Wait()
	return errors
}

func BenchmarkConcurrentTransactionsCreatingSameLabel(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, engine.Close()) })

	var nextID atomic.Uint64
	var conflicts atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx, beginErr := engine.BeginTransaction()
			if beginErr != nil {
				b.Error(beginErr)
				continue
			}
			id := nextID.Add(1)
			if _, createErr := tx.CreateNode(&Node{
				ID:     NodeID(fmt.Sprintf("db1:benchmark-%d", id)),
				Labels: []string{"Work"},
			}); createErr != nil {
				b.Error(createErr)
				_ = tx.Rollback()
				continue
			}
			if commitErr := tx.Commit(); commitErr != nil {
				conflicts.Add(1)
			}
		}
	})
	b.ReportMetric(100*float64(conflicts.Load())/float64(b.N), "conflicts_%")
}
