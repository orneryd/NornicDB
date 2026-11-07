package fabric

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

func TestPreparationKeywordCallbackContextCancellationAndRetry(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	for _, projection := range []bool{false, true} {
		t.Run(map[bool]string{false: "Plan", true: "SingleGraphStatement"}[projection], func(t *testing.T) {
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			planner := NewFabricPlanner(catalog, readDeclaredEffects())
			var observed []context.Context
			planner.SetClauseKeywordNameReader(func(ctx context.Context, text string, pos, end int, keyword string) bool {
				require.Equal(t, "UNION", keyword)
				require.Equal(t, text[pos:end], keyword)
				observed = append(observed, ctx)
				if ctx == parent {
					cancel()
				}
				return false
			})
			query := "RETURN 1 AS x UNION RETURN 2 AS x"
			if projection {
				statement, err := planner.SingleGraphStatement(parent, query, cyphertext.AcceptedSource)
				require.Same(t, context.Canceled, err)
				require.Empty(t, statement)
				healthy, err := planner.SingleGraphStatement(t.Context(), query, cyphertext.AcceptedSource)
				require.NoError(t, err)
				require.Contains(t, healthy, "UNION")
			} else {
				fragment, err := planner.Plan(parent, query, "db", cyphertext.AcceptedSource)
				require.Same(t, context.Canceled, err)
				require.Nil(t, fragment)
				healthy, err := planner.Plan(t.Context(), query, "db", cyphertext.AcceptedSource)
				require.NoError(t, err)
				require.IsType(t, &FragmentUnion{}, healthy)
			}
			require.GreaterOrEqual(t, len(observed), 2)
			require.Same(t, parent, observed[0])
			require.Same(t, t.Context(), observed[len(observed)-1])
		})
	}
}

func TestPreparationImportCallbackContextErrorsAndRetry(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	planner := NewFabricPlanner(catalog, readDeclaredEffects())
	query := "USE db CALL { USE db.shard WITH myVar RETURN myVar } RETURN myVar"
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	var observed []context.Context
	sentinel := errors.New("import preparation failed")
	planner.SetCallImportReader(func(ctx context.Context, scope string, scoped bool, body string) ([]string, error) {
		observed = append(observed, ctx)
		if ctx == parent {
			cancel()
			return nil, ctx.Err()
		}
		return nil, sentinel
	})
	fragment, err := planner.Plan(parent, query, "db", cyphertext.AcceptedSource)
	require.Same(t, context.Canceled, err)
	require.Nil(t, fragment)
	fragment, err = planner.Plan(t.Context(), query, "db", cyphertext.AcceptedSource)
	require.Same(t, sentinel, err)
	require.Nil(t, fragment)
	require.Equal(t, []context.Context{parent, t.Context()}, observed)
	planner.SetCallImportReader(func(ctx context.Context, scope string, scoped bool, body string) ([]string, error) {
		require.Same(t, t.Context(), ctx)
		return []string{"myVar"}, nil
	})
	fragment, err = planner.Plan(t.Context(), query, "db", cyphertext.AcceptedSource)
	require.NoError(t, err)
	require.NotNil(t, fragment)
}
