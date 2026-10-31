package cypher

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The permission classifier stages its source once per request. Raw client text
// and already projected text must reach the same verdict, and the accepted pass
// must never expand a projected escape a second time.
func TestPermissionClassificationStagesAcceptedTextOnce(t *testing.T) {
	raw := `CALL db.labels() YIELD label RETURN label AS l`
	source := cyphertext.NewSource(t.Context(), raw, RawSource)
	require.NoError(t, source.Err)
	require.Equal(t, raw, source.Text, "plain ASCII text must not be rewritten")

	for name, query := range map[string]string{
		"raw":      raw,
		"accepted": source.Text,
	} {
		t.Run(name, func(t *testing.T) {
			requirements := QueryPermissionRequirements(t.Context(), query, AcceptedSource)
			require.True(t, requirements.Read)
			require.False(t, requirements.Write)
			require.False(t, requirements.Schema)
			require.False(t, requirements.Admin)

			procedures := RegisteredProceduresForCalls(t.Context(), query, AcceptedSource)
			require.Len(t, procedures, 1)
			require.Equal(t, "db.labels", procedures[0].Name)
		})
	}
}

// Accepted text keeps its single backslash: the accepted pass owns no Unicode
// expansion, so a projected literal is never decoded twice. Submitting that same
// spelling as raw text decodes twice, which is exactly the difference the
// source stage exists to preserve.
func TestPermissionClassificationPreservesAcceptedUnicodeBoundaries(t *testing.T) {
	const accepted = `RETURN '\u0061' AS value`
	const raw = `RETURN '\\u0061' AS value`

	projected := cyphertext.NewSource(t.Context(), raw, RawSource)
	require.NoError(t, projected.Err)
	require.Equal(t, raw, projected.Original)
	require.Equal(t, raw, projected.Text, "raw normalization keeps this spelling byte for byte")
	require.Equal(t, `RETURN 'a' AS value`, cyphertext.NewSource(t.Context(), accepted, RawSource).Text,
		"re-submitting accepted text as raw decodes its escape yet again")

	require.Equal(t, PermissionRequirements{Read: true}, QueryPermissionRequirements(t.Context(), accepted, AcceptedSource))
	require.Empty(t, RegisteredProceduresForCalls(t.Context(), accepted, AcceptedSource))

	// The raw spelling reaches the same verdict after its one projection, and
	// no CALL is invented inside a projected literal.
	require.Equal(t, PermissionRequirements{Read: true}, QueryPermissionRequirements(t.Context(), raw, RawSource))
	require.Empty(t, RegisteredProceduresForCalls(t.Context(), raw, RawSource))

	callRaw := `CALL db.labels() YIELD label RETURN '\u0061' AS value`
	callAccepted := cyphertext.NewSource(t.Context(), callRaw, RawSource).Text
	require.NotEqual(t, callRaw, callAccepted)
	for stage, query := range map[SourceStage]string{RawSource: callRaw, AcceptedSource: callAccepted} {
		procedures := RegisteredProceduresForCalls(t.Context(), query, stage)
		require.Len(t, procedures, 1)
		require.Equal(t, "db.labels", procedures[0].Name)
		requirements := QueryPermissionRequirements(t.Context(), query, stage)
		require.False(t, requirements.Schema)
		require.False(t, requirements.Write)
		require.True(t, requirements.Read)
	}
}

// A cancellation that lands before the grammar read must not let the reader
// report "no calls": the registry owner refuses cancelled text.
func TestRegisteredProceduresForCallsHonoursCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Empty(t, RegisteredProceduresForCalls(ctx, "CALL db.labels()", RawSource))

	requirements := QueryPermissionRequirements(ctx, "CALL db.labels()", RawSource)
	require.True(t, requirements.Read)
	require.False(t, requirements.Admin)
	require.Empty(t, requirements.BoostedProcedures)
}

// Procedure contracts come from the live registry on every request, including
// calls nested in a subquery, so reusing a normalized view cannot freeze a
// declaration the registry replaced.
func TestPermissionClassificationResolvesLiveProcedureRegistry(t *testing.T) {
	names := []string{"reuseContract.read", "reuseContract.write"}
	for i, name := range names {
		mode := ProcedureModeRead
		if i == 1 {
			mode = ProcedureModeWrite
		}
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: mode}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{}, nil
		}))
	}
	t.Cleanup(func() {
		globalProcedureRegistry.mu.Lock()
		defer globalProcedureRegistry.mu.Unlock()
		for _, name := range names {
			delete(globalProcedureRegistry.user, name)
		}
	})

	const query = `CALL { CALL reuseContract.read() RETURN 1 AS one } RETURN one`
	for i := 0; i < 3; i++ {
		procedures := RegisteredProceduresForCalls(t.Context(), query, AcceptedSource)
		require.Len(t, procedures, 1)
		require.Equal(t, names[0], procedures[0].Name)
		require.False(t, QueryPermissionRequirements(t.Context(), query, AcceptedSource).Write)
	}

	const writing = `CALL { CALL reuseContract.write() RETURN 1 AS one } RETURN one`
	requirements := QueryPermissionRequirements(t.Context(), writing, AcceptedSource)
	require.True(t, requirements.Write, "a registered WRITE procedure makes the statement a write")
	require.False(t, requirements.Schema)
	require.False(t, requirements.Admin)
}

type cancellingPublicationHandler struct {
	cancel    context.CancelFunc
	cancelled bool
}

func (h *cancellingPublicationHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *cancellingPublicationHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message == "slow query" && !h.cancelled {
		h.cancelled = true
		h.cancel()
	}
	return nil
}
func (h *cancellingPublicationHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *cancellingPublicationHandler) WithGroup(string) slog.Handler      { return h }

func TestDirectStatementPublicationPreservesCommittedWrites(t *testing.T) {
	for name, testCase := range map[string]struct {
		query     string
		committed bool
	}{
		"create":              {query: "CREATE (:PublishedDirect)", committed: true},
		"create with literal": {query: "CREATE (:PublishedDirect {word: 'DROP INDEX'})", committed: true},
		"schema":              {query: "CREATE INDEX published_direct_idx FOR (n:PublishedDirect) ON (n.x)", committed: true},
		"read":                {query: "RETURN 1 AS one"},
		"literal read":        {query: "RETURN 'CREATE' AS word"},
		"labels procedure":    {query: "CALL db.labels() YIELD label RETURN label"},
	} {
		t.Run(name, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx, cancel := context.WithCancel(t.Context())
			statement := exec.newStatementExecutionContext(ctx, time.Now().UTC(), nil)
			cancel()
			exec.publishDirectStatementCommit(statement, testCase.query)
			require.Equal(t, testCase.committed, statement.outcome.committed())
		})
	}
}

// A real committed write reaches storage through the direct path and still
// reports success when the statement's context is cancelled as it completes.
func TestDirectPublicationReportsStoredWriteAcrossCancellation(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "published-direct")
	exec := NewStorageExecutor(store)
	h := &cancellingPublicationHandler{}
	cancelCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.cancel = cancel
	exec.SetLogger(slog.New(h))
	forceSlowQueryLogging(t, exec)

	result, err := exec.Execute(cancelCtx, "CREATE (:PublishedDirect)", nil)
	require.NoError(t, err, "a stored write must not invite a retry")
	require.NotNil(t, result)
	require.True(t, h.cancelled)
	require.ErrorIs(t, cancelCtx.Err(), context.Canceled)

	stored, err := NewStorageExecutor(store).Execute(t.Context(), "MATCH (n:PublishedDirect) RETURN count(n) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
}
