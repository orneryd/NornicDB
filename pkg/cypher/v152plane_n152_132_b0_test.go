package cypher

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	testfixture "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-371 reproductions: an overlapping MERGE transaction's loser, and the
// parameters the replay-eligibility decision reads at COMMIT. These are
// bounded, deterministic tests on the current head: no unsynchronized mutation
// of a caller's parameter map, no timing dependence, and no production change.
//
// TestNOR371ParameterMapReuseAfterFailedExecuteKeepsLoserOutcome pins the
// failed-admission shape: the peer commits first, so Execute returns the
// transient conflict and stages nothing. TestNOR371ParameterMapReuseAfter
// SuccessfulExecuteReadsAtCommit is the real producer: Execute succeeds and
// stages the write, the peer commits afterwards, the loser's COMMIT raises the
// actual storage UNIQUE violation, and the classifier's answer depends only on
// the map content after Execute returned. The remaining decision tests are
// characterizations with constructed inputs. Nothing here decides whether the
// reported difference is a defect or a deferred replay-eligibility feature; the
// hard ConstraintValidationFailed policy for non-MERGE work is asserted as the
// current approved behavior, not relaxed.
const (
	nor371MergeStatement = "MERGE (n:Doc {id: $key}) SET n.id = $value"
	nor371PeerMerge      = "MERGE (n:Doc {id: 'Y'}) ON CREATE SET n.owner = 'peer'"
	nor371ConstraintCode = "Neo.ClientError.Schema.ConstraintValidationFailed"
	nor371TxFailedCode   = "Neo.ClientError.Transaction.TransactionMarkedAsFailed"
)

// nor371Fixture returns a shared transactional store with a UNIQUE constraint
// on (:Doc.id), plus the observer executor used to read committed state. Each
// session gets its own executor, as in merge_node_write_race_test.go.
func nor371Fixture(t *testing.T) (storage.Engine, *StorageExecutor) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor371_replay")
	observer := NewStorageExecutor(store)
	_, err := observer.Execute(context.Background(), "CREATE CONSTRAINT nor371_doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	return store, observer
}

func nor371Begin(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	_, err := exec.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
}

// nor371Status requires err and returns its Neo4j status code.
func nor371Status(t *testing.T, err error) string {
	t.Helper()
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	return code
}

// nor371Docs returns the committed (:Doc) state a retry would see.
func nor371Docs(t *testing.T, observer *StorageExecutor) [][]interface{} {
	t.Helper()
	result, err := observer.Execute(context.Background(), "MATCH (n:Doc) RETURN n.id, n.owner ORDER BY n.id", nil)
	require.NoError(t, err)
	return result.Rows
}

func nor371Count(t *testing.T, observer *StorageExecutor, label string) int64 {
	t.Helper()
	result, err := observer.Execute(context.Background(), "MATCH (n:"+label+") RETURN count(n)", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)
	return result.Rows[0][0].(int64)
}

// TestNOR371OverlappingMergeLoserOutcome runs the reported overlap: a loser
// transaction has already staged work when a peer commits the MERGE key's
// node, so the loser cannot catch its snapshot up. The loser is transient, is
// not a uniqueness failure, and commits nothing; a loser that has observed
// nothing yet advances its snapshot and matches instead.
func TestNOR371OverlappingMergeLoserOutcome(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("observed loser is transient and commits nothing", func(t *testing.T) {
			store, observer := nor371Fixture(t)
			loser := NewStorageExecutor(store)
			nor371Begin(t, loser)
			_, err := loser.Execute(context.Background(), "CREATE (:Other {tag: 'loser'})", nil)
			require.NoError(t, err)

			// The peer commits the key after the loser's transaction began
			// and after it observed, so the loser's snapshot cannot move.
			_, err = NewStorageExecutor(store).Execute(context.Background(), nor371PeerMerge, nil)
			require.NoError(t, err)

			_, err = loser.Execute(context.Background(), nor371MergeStatement,
				map[string]interface{}{"key": "Y", "value": "Y"})
			require.Equal(t, nornicerrors.TransientOutdated, nor371Status(t, err))
			require.True(t, stderrors.Is(err, storage.ErrConflict), "%v", err)
			var violation *storage.ConstraintViolationError
			require.False(t, stderrors.As(err, &violation),
				"an uncommitted loser of an overlapping MERGE stays retryable, not a uniqueness failure: %v", err)

			_, err = loser.Execute(context.Background(), "COMMIT", nil)
			require.Equal(t, nor371TxFailedCode, nor371Status(t, err))
			require.Equal(t, [][]interface{}{{"Y", "peer"}}, nor371Docs(t, observer))
			require.Zero(t, nor371Count(t, observer, "Other"), "the loser's writes were not committed")
		})

		t.Run("unobserved loser matches the peer's node", func(t *testing.T) {
			store, observer := nor371Fixture(t)
			loser := NewStorageExecutor(store)
			nor371Begin(t, loser)

			_, err := NewStorageExecutor(store).Execute(context.Background(), nor371PeerMerge, nil)
			require.NoError(t, err)

			_, err = loser.Execute(context.Background(), "MERGE (n:Doc {id: $key}) ON CREATE SET n.owner = 'loser'",
				map[string]interface{}{"key": "Y"})
			require.NoError(t, err)
			_, err = loser.Execute(context.Background(), "COMMIT", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"Y", "peer"}}, nor371Docs(t, observer),
				"the loser matched the committed node and did not run ON CREATE")
		})
	})
}

// TestNOR371ParameterMapReuseAfterFailedExecuteKeepsLoserOutcome pins the
// failed-admission shape: the peer has already committed the MERGE key, so the
// loser's Execute is refused at admission with the transient conflict and
// stages nothing. Only after that error returned does the embedded caller reuse
// its map for the next statement. Reuse must not change the returned statement
// or COMMIT outcome, or the committed graph; the reuse is sequential, so
// nothing here mutates a map while Execute is running. The successful-staging
// producer is TestNOR371ParameterMapReuseAfterSuccessfulExecuteReadsAtCommit.
func TestNOR371ParameterMapReuseAfterFailedExecuteKeepsLoserOutcome(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		run := func(reuse bool) (statement, commit string) {
			store, observer := nor371Fixture(t)
			loser := NewStorageExecutor(store)
			nor371Begin(t, loser)
			_, err := loser.Execute(context.Background(), "CREATE (:Other {tag: 'loser'})", nil)
			require.NoError(t, err)
			_, err = NewStorageExecutor(store).Execute(context.Background(), nor371PeerMerge, nil)
			require.NoError(t, err)

			// The peer already committed key Y, so this MERGE is refused at
			// admission and stages nothing. The caller then reuses the map
			// for its next statement.
			params := map[string]interface{}{"key": "Y", "value": "Y"}
			_, err = loser.Execute(context.Background(), nor371MergeStatement, params)
			statement = nor371Status(t, err)
			if reuse {
				params["key"] = "X"
			}
			_, err = loser.Execute(context.Background(), "COMMIT", nil)
			commit = nor371Status(t, err)
			require.Equal(t, [][]interface{}{{"Y", "peer"}}, nor371Docs(t, observer))
			require.Zero(t, nor371Count(t, observer, "Other"), "the loser's writes were not committed")
			return statement, commit
		}

		controlStatement, controlCommit := run(false)
		reusedStatement, reusedCommit := run(true)
		require.Equal(t, nornicerrors.TransientOutdated, controlStatement)
		require.Equal(t, nor371TxFailedCode, controlCommit)
		require.Equal(t, controlStatement, reusedStatement, "parameter reuse changed the uncommitted loser's statement outcome")
		require.Equal(t, controlCommit, reusedCommit, "parameter reuse changed the uncommitted loser's COMMIT outcome")
	})
}

// TestNOR371ParameterMapReuseAfterSuccessfulExecuteReadsAtCommit is the real
// producer for the reported direction, not the failed-admission case: the
// loser's MERGE (key X, SET id Y) executes successfully and stages the write,
// because the peer has not committed Y yet. Only after Execute returned does
// the embedded caller reuse the map for key Y / value Y. The peer then commits
// id Y. The loser reserved only its MERGE key X, so the peer's commit is not
// blocked; the loser's COMMIT raises the actual storage UNIQUE violation. The
// classifier reads the recorded map at COMMIT: the reused key Y / value Y is
// reported retry-safe although the staged write was MERGE key X / SET id Y,
// which the retained map correctly reports as unsafe.
//
// The no-reuse control runs the same interleaving with the original map and
// observes the same COMMIT error and committed graph, so the only difference is
// the classifier's input after Execute returned. This test uses the actual
// storage error, not a constructed one, and the key-wait hook records that the
// peer committed without waiting on a reservation.
//
// Reachability limit: the only production caller of
// MergeUniqueConflictIsRetrySafe is Session.canRetryMergeCommitConflict in
// pkg/bolt/session_messages.go, and it records the RUN params map by reference.
// A Bolt client's RUN params are decoded into a server-owned map per message,
// and no production path mutates that map after RUN; the public Bolt path
// therefore cannot reuse the map and keeps the hard status (see
// pkg/bolt/nor371_parameter_reuse_public_test.go). This divergence is reachable
// only by calling the exported classifier with a caller-mutated map.
func TestNOR371ParameterMapReuseAfterSuccessfulExecuteReadsAtCommit(t *testing.T) {
	require.True(t, IsRetrySafeMergeCommitQuery(t.Context(), analyzeQuery(nor371MergeStatement)),
		"the MERGE statement's write shape is retry-safe; only the parameters vary")

	forEachParser(t, func(t *testing.T) {
		waits := testfixture.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 64)
		type runResult struct {
			commitCode     string
			conflict       bool
			classification bool
		}
		run := func(t *testing.T, reuse bool) runResult {
			t.Helper()
			store, observer := nor371Fixture(t)
			loser := NewStorageExecutor(store)
			nor371Begin(t, loser)

			params := map[string]interface{}{"key": "X", "value": "Y"}
			_, err := loser.Execute(context.Background(), nor371MergeStatement, params)
			require.NoError(t, err, "the loser stages MERGE key X / SET id Y before any peer commits Y")

			if reuse {
				params["key"] = "Y"
				params["value"] = "Y"
			}

			// The peer commits the id the loser staged. The loser holds only
			// the MERGE key X reservation, so Y is not reserved by it.
			peerCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			drainMergeKeyWaits(waits)
			_, err = NewStorageExecutor(store).Execute(peerCtx, nor371PeerMerge, nil)
			require.NoError(t, err, "the peer can commit id Y: the loser reserved only MERGE key X")
			select {
			case <-waits:
				t.Fatal("the peer waited on a key the loser did not reserve")
			default:
			}

			_, err = loser.Execute(peerCtx, "COMMIT", nil)
			require.Error(t, err, "the loser's COMMIT hits the peer's id Y")
			var violation *storage.ConstraintViolationError
			require.True(t, stderrors.As(err, &violation), "%v", err)
			require.Equal(t, "Doc", violation.Label)
			require.Equal(t, []string{"id"}, violation.Properties)

			code := nor371Status(t, err)
			classification := MergeUniqueConflictIsRetrySafe(peerCtx,
				[]CommitStatement{{Query: nor371MergeStatement, Params: params}}, err)
			require.Equal(t, [][]interface{}{{"Y", "peer"}}, nor371Docs(t, observer),
				"the loser's staged node did not commit; only the peer's Y is stored")
			t.Logf("reuse=%v peer=committed-without-wait commitCode=%s conflict=%v classification=%v",
				reuse, code, stderrors.Is(err, storage.ErrConflict), classification)
			return runResult{
				commitCode:     code,
				conflict:       stderrors.Is(err, storage.ErrConflict),
				classification: classification,
			}
		}

		control := run(t, false)
		reused := run(t, true)
		require.Equal(t, nor371ConstraintCode, control.commitCode)
		require.False(t, control.classification,
			"the retained merge key X and SET value Y are not retry-safe")
		require.Equal(t, control.commitCode, reused.commitCode,
			"the actual COMMIT error does not depend on the caller's later reuse")
		require.Equal(t, control.conflict, reused.conflict)
		require.True(t, reused.classification,
			"after reuse the classifier reads key Y / value Y and reports the actual commit failure retry-safe")
	})
}

// TestNOR371PeerOfReservedMergeKeyCannotCommitWhileLoserIsActive records the
// complementary boundary: when the loser's MERGE pattern itself keys on Y, its
// transaction holds the MERGE key reservation for Y until it ends. A peer MERGE
// of Y under a bounded context enters the key wait and ends at the caller's
// deadline (no goroutine, no hang); the key-wait hook proves it waited on the
// reservation rather than failing before admission. The loser then commits its
// own node.
func TestNOR371PeerOfReservedMergeKeyCannotCommitWhileLoserIsActive(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store, observer := nor371Fixture(t)
		waits := testfixture.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 64)
		loser := NewStorageExecutor(store)
		nor371Begin(t, loser)
		_, err := loser.Execute(context.Background(), nor371MergeStatement,
			map[string]interface{}{"key": "Y", "value": "Y"})
		require.NoError(t, err, "the loser stages MERGE key Y / SET id Y before any peer commits Y")

		peerCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		drainMergeKeyWaits(waits)
		_, err = NewStorageExecutor(store).Execute(peerCtx, nor371PeerMerge, nil)
		require.Error(t, err, "the peer cannot commit Y while the loser holds the MERGE key Y reservation")
		require.True(t, stderrors.Is(err, context.DeadlineExceeded), "%v", err)
		select {
		case <-waits:
			t.Log("peer entered the MERGE key wait for the loser's reservation")
		default:
			t.Fatal("the peer did not wait on the loser's MERGE key reservation")
		}
		code, _ := nornicerrors.Neo4jStatus(err)
		t.Logf("peer of the reserved MERGE key ended at the bounded context: code=%q error=%v", code, err)

		_, err = loser.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err, "the loser still owns the key and commits")
		require.Equal(t, [][]interface{}{{"Y", nil}}, nor371Docs(t, observer),
			"only the loser's node is stored after the peer's bounded wait")
	})
}

// TestNOR371IndependentWritesKeepHardConstraintPolicy pins the approved hard
// policy for write shapes that are not a MERGE race: a direct CREATE that
// clashes at COMMIT is ConstraintValidationFailed, and a transaction whose
// writes are not MERGE-only is ineligible for the transient classification.
// The MERGE statement itself stays retry-safe, so the refusal comes from the
// direct write, not from the MERGE shape.
func TestNOR371IndependentWritesKeepHardConstraintPolicy(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("direct create overlap", func(t *testing.T) {
			store, observer := nor371Fixture(t)
			loser := NewStorageExecutor(store)
			nor371Begin(t, loser)
			_, err := loser.Execute(context.Background(), "CREATE (:Doc {id: 'Z'})", nil)
			require.NoError(t, err)
			_, err = NewStorageExecutor(store).Execute(context.Background(), "CREATE (:Doc {id: 'Z'})", nil)
			require.NoError(t, err)

			_, err = loser.Execute(context.Background(), "COMMIT", nil)
			require.Equal(t, nor371ConstraintCode, nor371Status(t, err))
			var violation *storage.ConstraintViolationError
			require.True(t, stderrors.As(err, &violation), "%v", err)
			require.Equal(t, "Doc", violation.Label)
			require.Equal(t, []string{"id"}, violation.Properties)
			require.False(t, stderrors.Is(err, storage.ErrConflict), "%v", err)
			require.Equal(t, [][]interface{}{{"Z", nil}}, nor371Docs(t, observer))
			require.False(t, IsRetrySafeMergeCommitQuery(t.Context(), analyzeQuery("CREATE (:Doc {id: $key})")))
		})

		t.Run("merge with an independent direct write", func(t *testing.T) {
			store, observer := nor371Fixture(t)
			loser := NewStorageExecutor(store)
			nor371Begin(t, loser)
			_, err := loser.Execute(context.Background(), "CREATE (:Doc {id: 'R'})", nil)
			require.NoError(t, err)
			_, err = NewStorageExecutor(store).Execute(context.Background(), "CREATE (:Doc {id: 'R'})", nil)
			require.NoError(t, err)
			_, err = loser.Execute(context.Background(), "MERGE (n:Doc {id: 'S'}) ON CREATE SET n.owner = 'loser'", nil)
			require.NoError(t, err)
			require.True(t, IsRetrySafeMergeCommitQuery(t.Context(), analyzeQuery("MERGE (n:Doc {id: 'S'}) ON CREATE SET n.owner = 'loser'")),
				"the MERGE statement alone is retry-safe")

			_, err = loser.Execute(context.Background(), "COMMIT", nil)
			require.Equal(t, nor371ConstraintCode, nor371Status(t, err))
			var violation *storage.ConstraintViolationError
			require.True(t, stderrors.As(err, &violation), "%v", err)
			require.Equal(t, []string{"id"}, violation.Properties)
			require.Equal(t, [][]interface{}{{"R", nil}}, nor371Docs(t, observer),
				"the MERGE's staged node did not commit either; no committed loser")
		})

		t.Run("custom procedure effect", func(t *testing.T) {
			store, observer := nor371Fixture(t)
			const call = "CALL apoc.cypher.run('CREATE (n:Doc {id: $id})', {id: 'Q'}) YIELD value RETURN value"
			loser := NewStorageExecutor(store)
			nor371Begin(t, loser)
			_, err := loser.Execute(context.Background(), call, nil)
			require.NoError(t, err)
			_, err = NewStorageExecutor(store).Execute(context.Background(), "CREATE (:Doc {id: 'Q'})", nil)
			require.NoError(t, err)

			_, err = loser.Execute(context.Background(), "COMMIT", nil)
			require.Equal(t, nor371ConstraintCode, nor371Status(t, err))
			var violation *storage.ConstraintViolationError
			require.True(t, stderrors.As(err, &violation), "%v", err)
			require.Equal(t, []string{"id"}, violation.Properties)
			require.Equal(t, [][]interface{}{{"Q", nil}}, nor371Docs(t, observer))
			require.False(t, IsRetrySafeMergeCommitQuery(t.Context(), analyzeQuery(call)),
				"a statement whose writes are a procedure's effect is not a retry-safe MERGE")
		})
	})
}

// TestNOR371ReplayEligibilityReadsParametersAtCommitWindow is a constructed
// characterization of the classifier's inputs, not a producer: the
// ConstraintViolationError below is created by the test, not raised by a real
// COMMIT. It pins that the classifier reads the map it is given, so the
// reported staged map (MERGE key X / SET id Y) is unsafe while the map a caller
// reused after Execute (key Y / value Y) is reported retry-safe even though the
// staged write did not change. The real producer and actual commit error are in
// TestNOR371ParameterMapReuseAfterSuccessfulExecuteReadsAtCommit. Which answer
// is a defect and which is deferred replay-eligibility work is not decided
// here; the hard policy for non-MERGE work is asserted unchanged.
func TestNOR371ReplayEligibilityReadsParametersAtCommitWindow(t *testing.T) {
	violation := &storage.ConstraintViolationError{
		Type:       storage.ConstraintUnique,
		Label:      "Doc",
		Properties: []string{"id"},
		Message:    "Node with id=Y already exists (nodeID: nornic:peer)",
	}
	require.True(t, IsRetrySafeMergeCommitQuery(t.Context(), analyzeQuery(nor371MergeStatement)),
		"the MERGE statement's write shape is retry-safe")

	// The reported staged write: the MERGE keys on X and the SET overwrites the
	// id with Y, so a retry of the same statement cannot match the peer's Y.
	staged := map[string]interface{}{"key": "X", "value": "Y"}
	require.False(t, MergeUniqueConflictIsRetrySafe(t.Context(), []CommitStatement{{Query: nor371MergeStatement, Params: staged}}, violation),
		"the staged MERGE key X / SET id Y is not retry-safe")

	// The caller reused the same map after Execute returned: key Y / value Y
	// makes the classifier report the unchanged staged write as retry-safe.
	reused := map[string]interface{}{"key": "Y", "value": "Y"}
	require.True(t, MergeUniqueConflictIsRetrySafe(t.Context(), []CommitStatement{{Query: nor371MergeStatement, Params: reused}}, violation),
		"the reused map reads as a retry-safe MERGE key Y / SET id Y")

	// Control: when one parameter feeds both the MERGE key and the SET value,
	// reuse cannot separate them, so the decision is unchanged.
	const shared = "MERGE (n:Doc {id: $key}) SET n.id = $key"
	sharedParams := map[string]interface{}{"key": "Y"}
	require.True(t, MergeUniqueConflictIsRetrySafe(t.Context(), []CommitStatement{{Query: shared, Params: sharedParams}}, violation))
	sharedParams["key"] = "X"
	require.True(t, MergeUniqueConflictIsRetrySafe(t.Context(), []CommitStatement{{Query: shared, Params: sharedParams}}, violation))

	// The approved hard answers are unaffected: a computed value, a direct
	// CREATE and a procedure effect are never reported as a retryable race.
	require.False(t, MergeUniqueConflictIsRetrySafe(t.Context(), []CommitStatement{{
		Query:  "MERGE (n:Doc {id: $key}) SET n.id = toUpper($value)",
		Params: map[string]interface{}{"key": "Y", "value": "y"},
	}}, violation))
	for _, query := range []string{
		"CREATE (:Doc {id: $key})",
		"CALL apoc.cypher.run('MERGE (n:Doc {id: $key})', {key: $key}) YIELD value RETURN value",
	} {
		require.False(t, IsRetrySafeMergeCommitQuery(t.Context(), analyzeQuery(query)), query)
	}
}
