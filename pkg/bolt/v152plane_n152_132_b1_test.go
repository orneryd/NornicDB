package bolt

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-371 synthetic characterization of the Bolt consumer's decision inputs and
// a test-only status helper. It constructs a commit-time UNIQUE failure; it runs
// no storage COMMIT, no wire transaction and no producer. The production COMMIT
// path uses Session.canRetryMergeCommitConflict and
// nornicerrors.MarkMergeCommitTimeUniqueConflict + RenderStatus;
// mapBoltCommitError has no production caller and is exercised only here. The
// real producer and actual commit error are in pkg/cypher
// TestNOR371ParameterMapReuseAfterSuccessfulExecuteReadsAtCommit, and the
// public wire result is in
// TestNOR371WireMergeStagedBeforePeerCommitKeepsRetainedParameters.
//
// The code pins only that the consumer's recorded statement params are read by
// reference: a caller that mutates that map after RUN changes the synthetic
// classification, while non-MERGE shapes keep the hard answer. No public
// reachable defect is established here.
func TestNOR371SyntheticCommitStatusForParametersRecordedAtRun(t *testing.T) {
	const (
		mergeStatement = "MERGE (n:Doc {id: $key}) SET n.id = $value"
		directCreate   = "CREATE (:Doc {id: $key})"
		procedureCall  = "CALL apoc.cypher.run('CREATE (n:Doc {id: $key})', {key: $key}) YIELD value RETURN value"
		transient      = "Neo.TransientError.Transaction.Outdated"
		hard           = "Neo.ClientError.Schema.ConstraintValidationFailed"
	)

	// The commit failure a peer's committed id=Y produces for a transaction
	// that staged the same value: the pinned consumer shape, constructed here
	// rather than raised by storage.
	syntheticCommitFailure := func() error {
		return fmt.Errorf("commit failed: constraint violation: %w", &storage.ConstraintViolationError{
			Type:       storage.ConstraintUnique,
			Label:      "Doc",
			Properties: []string{"id"},
			Message:    "Node with id=Y already exists (nodeID: nornic:peer)",
		})
	}
	record := func(t *testing.T, session *Session, query string, params map[string]any) {
		t.Helper()
		session.inTransaction = true
		session.recordExplicitTransactionWrite(t.Context(), query, params, true)
	}
	commitCode := func(t *testing.T, session *Session) string {
		t.Helper()
		failure := syntheticCommitFailure()
		code, _ := mapBoltCommitError(failure, session.canRetryMergeCommitConflict(t.Context(), failure))
		return code
	}
	stagedParams := func() map[string]any {
		return map[string]any{"key": "Y", "value": "Y"}
	}
	clone := func(params map[string]any) map[string]any {
		copied := make(map[string]any, len(params))
		for key, value := range params {
			copied[key] = value
		}
		return copied
	}

	t.Run("as staged", func(t *testing.T) {
		session := &Session{server: &Server{config: DefaultConfig()}}
		record(t, session, mergeStatement, stagedParams())
		require.Equal(t, transient, commitCode(t, session))
	})

	t.Run("caller reuses the map after RUN", func(t *testing.T) {
		session := &Session{server: &Server{config: DefaultConfig()}}
		params := stagedParams()
		record(t, session, mergeStatement, params)
		params["key"] = "X" // sequential reuse for the caller's next statement
		require.Equal(t, hard, commitCode(t, session))
	})

	t.Run("parameters retained at RUN", func(t *testing.T) {
		session := &Session{server: &Server{config: DefaultConfig()}}
		params := stagedParams()
		record(t, session, mergeStatement, clone(params))
		params["key"] = "X"
		require.Equal(t, transient, commitCode(t, session))
	})

	t.Run("direct create", func(t *testing.T) {
		session := &Session{server: &Server{config: DefaultConfig()}}
		record(t, session, directCreate, stagedParams())
		require.Equal(t, hard, commitCode(t, session))
	})

	t.Run("merge with an independent direct write", func(t *testing.T) {
		session := &Session{server: &Server{config: DefaultConfig()}}
		record(t, session, mergeStatement, stagedParams())
		record(t, session, directCreate, stagedParams())
		require.Equal(t, hard, commitCode(t, session))
	})

	t.Run("custom procedure effect", func(t *testing.T) {
		session := &Session{server: &Server{config: DefaultConfig()}}
		record(t, session, procedureCall, stagedParams())
		require.Equal(t, hard, commitCode(t, session))
	})
}
