package fabric

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// readDeclaredEffects reports exactly the statements a fixture declares as
// writes as writes and every other statement as a read. Fixtures declare their
// effects so no planned fragment depends on a classifier guessing them.
func readDeclaredEffects(writes ...string) StatementEffectReader {
	declared := make(map[string]struct{}, len(writes))
	for _, statement := range writes {
		declared[strings.TrimSpace(statement)] = struct{}{}
	}
	return func(_ context.Context, statement string) (bool, error) {
		_, isWrite := declared[strings.TrimSpace(statement)]
		return isWrite, nil
	}
}

// TestPlannerResolvesFragmentEffect pins the canonical effect boundary: a
// FragmentExec's IsWrite is the statement effect reader's answer for that
// fragment's own statement, whether the statement reads, writes, calls an
// unresolved procedure, or only quotes a write keyword.
func TestPlannerResolvesFragmentEffect(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})

	for _, tc := range []struct {
		name, query string
		resolved    bool
		want        bool
	}{
		{name: "read statement", query: "MATCH (n:Doc) RETURN n", resolved: false, want: false},
		{name: "write statement", query: "CREATE (n:Doc)", resolved: true, want: true},
		{name: "unknown procedure", query: "CALL missing.unknown() YIELD value RETURN value", resolved: true, want: true},
		{name: "quoted keyword", query: "RETURN 'CREATE (n:Doc)' AS text", resolved: false, want: false},
		{name: "reader clears a write statement", query: "CREATE (n:Doc)", resolved: false, want: false},
		{name: "reader flags a read statement", query: "MATCH (n:Doc) RETURN n", resolved: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			effects := readDeclaredEffects()
			if tc.resolved {
				effects = readDeclaredEffects(tc.query)
			}
			fragment, err := NewFabricPlanner(catalog, effects).Plan(t.Context(), tc.query, "db", cyphertext.RawSource)
			require.NoError(t, err)
			exec, ok := fragment.(*FragmentExec)
			require.True(t, ok, "expected FragmentExec, got %T", fragment)
			require.Equal(t, tc.want, exec.IsWrite)
			require.Equal(t, tc.query, exec.Query, "the fragment keeps its accepted statement text")
			require.Equal(t, "db", exec.GraphName)
		})
	}
}

// TestPlannerResolvesEffectPerUnionBranch proves each branch's effect is
// resolved from that branch's own statement while the UNION structure and
// branch text stay unchanged.
func TestPlannerResolvesEffectPerUnionBranch(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("g1", &LocationLocal{DBName: "a"})
	catalog.Register("g2", &LocationLocal{DBName: "b"})

	var asked []string
	planner := NewFabricPlanner(catalog, func(_ context.Context, statement string) (bool, error) {
		asked = append(asked, statement)
		return strings.HasPrefix(statement, "CREATE "), nil
	})

	fragment, err := planner.Plan(t.Context(),
		"USE g1 CREATE (n:Doc) RETURN n UNION ALL USE g2 MATCH (n:Doc) RETURN n", "g1", cyphertext.RawSource)
	require.NoError(t, err)
	union, ok := fragment.(*FragmentUnion)
	require.True(t, ok, "expected FragmentUnion, got %T", fragment)
	require.False(t, union.Distinct)

	lhs, ok := union.LHS.(*FragmentExec)
	require.True(t, ok, "expected lhs FragmentExec, got %T", union.LHS)
	require.Equal(t, "CREATE (n:Doc) RETURN n", lhs.Query)
	require.Equal(t, "g1", lhs.GraphName)
	require.True(t, lhs.IsWrite)

	rhs, ok := union.RHS.(*FragmentExec)
	require.True(t, ok, "expected rhs FragmentExec, got %T", union.RHS)
	require.Equal(t, "MATCH (n:Doc) RETURN n", rhs.Query)
	require.Equal(t, "g2", rhs.GraphName)
	require.False(t, rhs.IsWrite)

	require.ElementsMatch(t, []string{"CREATE (n:Doc) RETURN n", "MATCH (n:Doc) RETURN n"}, asked,
		"the reader is asked about each branch's own statement")
}

// TestPlannerResolvesCallSubqueryFragmentEffect proves the effects of a CALL
// body, its outer prefix and its trailing statement are resolved separately
// while the Apply structure keeps each statement as written.
func TestPlannerResolvesCallSubqueryFragmentEffect(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})

	planner := NewFabricPlanner(catalog, readDeclaredEffects("CREATE (m:Doc) RETURN m"))
	fragment, err := planner.Plan(t.Context(),
		"MATCH (n:Doc) CALL { USE db.shard CREATE (m:Doc) RETURN m } RETURN n", "db", cyphertext.RawSource)
	require.NoError(t, err)

	var planned []*FragmentExec
	var walk func(Fragment)
	walk = func(current Fragment) {
		switch f := current.(type) {
		case *FragmentExec:
			planned = append(planned, f)
		case *FragmentApply:
			walk(f.Input)
			walk(f.Inner)
		case *FragmentUnion:
			walk(f.LHS)
			walk(f.RHS)
		}
	}
	walk(fragment)

	require.Len(t, planned, 3)
	require.Equal(t, "MATCH (n:Doc) RETURN *", planned[0].Query)
	require.False(t, planned[0].IsWrite)
	require.Equal(t, "db", planned[0].GraphName)
	require.Equal(t, "CREATE (m:Doc) RETURN m", planned[1].Query)
	require.True(t, planned[1].IsWrite)
	require.Equal(t, "db.shard", planned[1].GraphName)
	require.Equal(t, "RETURN n", planned[2].Query)
	require.False(t, planned[2].IsWrite)
	require.Equal(t, "db", planned[2].GraphName)
}

// TestPlannerPropagatesStatementEffectError proves a failing reader fails the
// whole plan, including when an earlier branch resolved.
func TestPlannerPropagatesStatementEffectError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("g1", &LocationLocal{DBName: "a"})
	catalog.Register("g2", &LocationLocal{DBName: "b"})
	cause := errors.New("statement effects unavailable")

	t.Run("single fragment", func(t *testing.T) {
		planner := NewFabricPlanner(catalog, func(context.Context, string) (bool, error) { return false, cause })
		fragment, err := planner.Plan(t.Context(), "MATCH (n:Doc) RETURN n", "g1", cyphertext.RawSource)
		require.Nil(t, fragment)
		require.ErrorIs(t, err, cause)
	})

	t.Run("later branch", func(t *testing.T) {
		planner := NewFabricPlanner(catalog, func(_ context.Context, statement string) (bool, error) {
			if strings.HasPrefix(statement, "MATCH ") {
				return false, cause
			}
			return true, nil
		})
		fragment, err := planner.Plan(t.Context(),
			"USE g1 CREATE (n:Doc) RETURN n UNION ALL USE g2 MATCH (n:Doc) RETURN n", "g1", cyphertext.RawSource)
		require.Nil(t, fragment, "no partially resolved plan is returned")
		require.ErrorIs(t, err, cause)
	})
}

// TestPlannerStatementEffectCancellation proves a canceled plan reports
// cancellation whether it arrives before the first statement, through the
// reader's error, or only after the reader returns.
func TestPlannerStatementEffectCancellation(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})

	t.Run("canceled before planning", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		fragment, err := NewFabricPlanner(catalog, readDeclaredEffects()).
			Plan(ctx, "MATCH (n:Doc) RETURN n", "db", cyphertext.RawSource)
		require.Nil(t, fragment)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("reader returns the cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		planner := NewFabricPlanner(catalog, func(callCtx context.Context, _ string) (bool, error) {
			cancel()
			return false, callCtx.Err()
		})
		fragment, err := planner.Plan(ctx, "MATCH (n:Doc) RETURN n", "db", cyphertext.RawSource)
		require.Nil(t, fragment)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("reader cancels without an error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		planner := NewFabricPlanner(catalog, func(context.Context, string) (bool, error) {
			cancel()
			return true, nil
		})
		fragment, err := planner.Plan(ctx, "MATCH (n:Doc) RETURN n", "db", cyphertext.RawSource)
		require.Nil(t, fragment)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// TestPlannerRequiresStatementEffectReader proves the planner has no default
// statement classifier: without a reader, planning fails.
func TestPlannerRequiresStatementEffectReader(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})

	fragment, err := NewFabricPlanner(catalog, nil).Plan(t.Context(), "MATCH (n:Doc) RETURN n", "db", cyphertext.RawSource)
	require.Nil(t, fragment)
	require.ErrorIs(t, err, errStatementEffectReaderMissing)
}
