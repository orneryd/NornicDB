package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestPublicMetadataExplicitSourceStages(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "source-stage"))
	for _, tc := range []struct {
		stage       SourceStage
		query, name string
	}{
		{RawSource, `RETURN 1 AS a\u0062`, "ab"},
		{RawSource, "RETURN 1 AS `a\\u005cu0062`", `a\u0062`},
		{AcceptedSource, "RETURN 1 AS `a\\u0062`", `a\u0062`},
	} {
		columns, err := executor.ResultColumns(t.Context(), tc.query, resultmetadata.KnownScope(nil), false, tc.stage)
		require.NoError(t, err)
		require.Equal(t, []string{tc.name}, columns)
		columns, err = executor.StatementResultColumnsContext(t.Context(), tc.query, tc.stage)
		require.NoError(t, err)
		require.Equal(t, []string{tc.name}, columns)
		executor.StatementFacts(tc.query, resultmetadata.KnownScope(nil), false, tc.stage).Visit(func(s resultmetadata.Statement) {
			s.Columns.Visit(func(names []string) { require.Equal(t, []string{tc.name}, names) }, func() { t.Fatal("unknown header") })
		}, func() { t.Fatal("declined header") }, func(r resultmetadata.Unresolved) { t.Fatalf("unresolved: %v", r.Err) })
	}
	for _, query := range []string{`\u0043REATE (n)`, `CREATE (n)`} {
		require.True(t, QueryPermissionRequirements(t.Context(), query, RawSource).Write)
	}
	require.False(t, QueryPermissionRequirements(t.Context(), `RETURN 1`, RawSource).Write)
}

func TestPublicFactsRawNestedCoordinates(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "source-coordinates"))
	raw := `/* \u0061 */ CALL () { RETURN 1 AS a\u0062 UNION RETURN 2 AS a\u0062 } RETURN ab`
	accepted := cyphertext.NewSource(t.Context(), raw, cyphertext.RawSource)
	require.NoError(t, accepted.Err)
	var original, projected resultmetadata.Statement
	executor.StatementFacts(accepted.Text, resultmetadata.KnownScope(nil), false, AcceptedSource).Visit(func(s resultmetadata.Statement) { original = s }, func() { t.Fatal("declined") }, func(r resultmetadata.Unresolved) { t.Fatal(r.Err) })
	executor.StatementFacts(raw, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(s resultmetadata.Statement) { projected = s }, func() { t.Fatal("declined") }, func(r resultmetadata.Unresolved) { t.Fatal(r.Err) })
	var check func(resultmetadata.Statement, resultmetadata.Statement)
	check = func(a, b resultmetadata.Statement) {
		extent, ok := accepted.OriginalSpan(cyphertext.Span{Start: a.Start, End: a.End}, cyphertext.SourceExtent)
		require.True(t, ok)
		require.Equal(t, extent.Start, b.Start)
		require.Equal(t, extent.End, b.End)
		require.Len(t, b.Clauses, len(a.Clauses))
		for i, c := range a.Clauses {
			mapped, ok := accepted.OriginalSpan(cyphertext.Span{Start: c.Start, End: c.End}, cyphertext.SourceExtent)
			require.True(t, ok)
			require.Equal(t, mapped.Start, b.Clauses[i].Start)
			require.Equal(t, mapped.End, b.Clauses[i].End)
			if c.Call != nil {
				check(*c.Call.Statement, *b.Clauses[i].Call.Statement)
			}
		}
		require.Len(t, b.Branches, len(a.Branches))
		for i := range a.Branches {
			check(a.Branches[i], b.Branches[i])
		}
	}
	check(original, projected)
	// A raw projection must not mutate the accepted cached product.
	executor.StatementFacts(accepted.Text, resultmetadata.KnownScope(nil), false, AcceptedSource).Visit(func(s resultmetadata.Statement) { require.Equal(t, original, s) }, func() { t.Fatal("declined") }, func(r resultmetadata.Unresolved) { t.Fatal(r.Err) })
}

func TestPublicFactsRawParticipantObligation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "source-obligations"))
	query := `/* \u0061 */ USE comp.remote CALL custom.participantOnly(outer)`
	calls := 0
	facts := executor.StatementFactsWith(query, resultmetadata.KnownScope([]string{"outer"}), false, func(o resultmetadata.SchemaObligation) (resultmetadata.Columns, error) {
		calls++
		require.Equal(t, "CALL custom.participantOnly(outer)", query[o.Start:o.End])
		require.Equal(t, "USE comp.remote", strings.TrimSpace(query[o.UseStart:o.UseEnd]))
		return resultmetadata.KnownColumns([]string{"value"}), nil
	}, RawSource)
	facts.Visit(func(resultmetadata.Statement) {}, func() { t.Fatal("declined") }, func(r resultmetadata.Unresolved) { t.Fatal(r.Err) })
	require.Equal(t, 1, calls)
}

func TestPublicFactsUnresolvedOriginAndConcurrentViews(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "source-errors"))
	raw := `/* \u0061 */ RETURN missing`
	source := cyphertext.NewSource(t.Context(), raw, cyphertext.RawSource)
	var accepted, original resultmetadata.Unresolved
	executor.StatementFacts(source.Text, resultmetadata.KnownScope(nil), false, AcceptedSource).Visit(func(resultmetadata.Statement) { t.Fatal("undefined variable accepted") }, func() { t.Fatal("declined") }, func(r resultmetadata.Unresolved) { accepted = r })
	executor.StatementFacts(raw, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) { t.Fatal("undefined variable accepted") }, func() { t.Fatal("declined") }, func(r resultmetadata.Unresolved) { original = r })
	require.Equal(t, accepted.Reason, original.Reason)
	if accepted.Position >= 0 {
		position, ok := source.OriginalOffset(accepted.Position, true, cyphertext.SourceExtent)
		require.True(t, ok)
		require.Equal(t, position, original.Position)
	} else {
		require.Equal(t, accepted.Position, original.Position)
	}
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			for j := 0; j < 8; j++ {
				executor.StatementFacts(raw, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) { t.Error("undefined variable accepted") }, func() { t.Error("declined") }, func(r resultmetadata.Unresolved) {
					require.Equal(t, original.Position, r.Position)
					require.Equal(t, original.Reason, r.Reason)
				})
				executor.StatementFacts(source.Text, resultmetadata.KnownScope(nil), false, AcceptedSource).Visit(func(resultmetadata.Statement) { t.Error("undefined variable accepted") }, func() { t.Error("declined") }, func(r resultmetadata.Unresolved) {
					require.Equal(t, accepted.Position, r.Position)
					require.Equal(t, accepted.Reason, r.Reason)
				})
			}
		})
	}
}
