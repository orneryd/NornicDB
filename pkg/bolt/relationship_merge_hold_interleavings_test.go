package bolt

import (
	"context"
	"fmt"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBoltRelationshipMergeHeldTransactionInterleavings records the result of
// a held relationship MERGE against the 16 combinations of session-2 mode,
// session creation/begin order, MERGE order, and whether session 1 commits
// before session 2 starts its MERGE. The existing concurrent-MERGE test only
// races managed transactions and does not cover this explicit held transaction.
func TestBoltRelationshipMergeHeldTransactionInterleavings(t *testing.T) {
	for _, explicit2 := range []bool{false, true} {
		for _, session2First := range []bool{false, true} {
			for _, merge2First := range []bool{false, true} {
				for _, commit1BeforeMerge2 := range []bool{false, true} {
					name := fmt.Sprintf("s2=%s/session2-first=%t/merge2-first=%t/commit1-before-merge2=%t",
						map[bool]string{false: "autocommit", true: "explicit"}[explicit2],
						session2First, merge2First, commit1BeforeMerge2)
					t.Run(name, func(t *testing.T) {
						runBoltRelationshipMergeHeldSchedule(t, explicit2, session2First, merge2First, commit1BeforeMerge2)
					})
				}
			}
		}
	}
}

func runBoltRelationshipMergeHeldSchedule(t *testing.T, explicit2, session2First, merge2First, commit1BeforeMerge2 bool) {
	base := testfixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "neo4j")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	require.NoError(t, driver.VerifyConnectivity(ctx))

	caseID := t.Name()
	query := `MATCH (a:HoldMerge {id: $a}), (b:HoldMerge {id: $b}) MERGE (a)-[:R]->(b)`
	params := map[string]any{"a": caseID + "-a", "b": caseID + "-b"}
	setup := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
	seed, err := setup.Run(ctx, `CREATE (:HoldMerge {id: $a}), (:HoldMerge {id: $b})`, params)
	require.NoError(t, err)
	_, err = seed.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, setup.Close(ctx))

	s1 := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
	s2 := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
	t.Cleanup(func() { _ = s1.Close(context.Background()); _ = s2.Close(context.Background()) })
	var tx1, tx2 neo4jdriver.ExplicitTransaction
	openTx := func(session neo4jdriver.SessionWithContext) neo4jdriver.ExplicitTransaction {
		tx, beginErr := session.BeginTransaction(ctx)
		require.NoError(t, beginErr)
		return tx
	}
	if session2First && explicit2 {
		tx2 = openTx(s2)
	}
	tx1 = openTx(s1)
	if !session2First && explicit2 {
		tx2 = openTx(s2)
	}

	runExplicit := func(tx neo4jdriver.ExplicitTransaction) error {
		result, runErr := tx.Run(ctx, query, params)
		if runErr != nil {
			return runErr
		}
		_, runErr = result.Consume(ctx)
		return runErr
	}
	runSecond := func() (string, error) {
		if explicit2 {
			err := runExplicit(tx2)
			if err == nil {
				err = tx2.Commit(ctx)
			}
			return "explicit", err
		}
		result, err := s2.Run(ctx, query, params)
		if err == nil {
			_, err = result.Consume(ctx)
		}
		return "autocommit", err
	}
	firstSession, secondSession := "session1", "session2"
	if session2First {
		firstSession, secondSession = secondSession, firstSession
	}

	var secondErr error
	secondOutcome := ""
	if merge2First {
		secondOutcome, secondErr = runSecond()
		if err := runExplicit(tx1); err != nil {
			t.Logf("session1 merge error after session2: %v", err)
		}
		if err := tx1.Commit(ctx); err != nil {
			t.Logf("session1 commit error: %v", err)
		}
	} else {
		if err := runExplicit(tx1); err != nil {
			t.Logf("session1 merge error: %v", err)
		}
		if commit1BeforeMerge2 {
			if err := tx1.Commit(ctx); err != nil {
				t.Logf("session1 commit error before session2: %v", err)
			}
			secondOutcome, secondErr = runSecond()
		} else {
			secondDone := make(chan struct{})
			go func() {
				secondOutcome, secondErr = runSecond()
				close(secondDone)
			}()
			select {
			case <-secondDone:
				t.Log("session2 MERGE returned while session1 was uncommitted")
			case <-time.After(100 * time.Millisecond):
				t.Log("session2 MERGE waited for session1")
			}
			if err := tx1.Commit(ctx); err != nil {
				t.Logf("session1 commit error: %v", err)
			}
			select {
			case <-secondDone:
			case <-ctx.Done():
				t.Logf("session2 did not finish after session1 commit: %v", ctx.Err())
			}
		}
	}
	if secondErr != nil {
		t.Logf("session2 %s error: %v", secondOutcome, secondErr)
	} else {
		t.Logf("session2 %s succeeded", secondOutcome)
	}

	check := driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer check.Close(context.Background())
	result, err := check.Run(ctx, `MATCH (a:HoldMerge {id: $a})-[r:R]->(b:HoldMerge {id: $b}) RETURN count(r)`, params)
	if err != nil {
		t.Logf("final count query error: %v", err)
		return
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Logf("final count result error: %v", err)
		return
	}
	count := record.Values[0]
	t.Logf("INTERLEAVE sessions=%s/%s explicit2=%t merge2First=%t commit1BeforeMerge2=%t relationship_count=%v", firstSession, secondSession, explicit2, merge2First, commit1BeforeMerge2, count)
}
