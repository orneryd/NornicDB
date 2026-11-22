package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A backtick-quoted REMOVE label whose decoded name is a single space is a
// label like any other: the label-chain reader unquotes it and the clause
// owner admits it, so neither the public builder nor the cached analysis may
// trim it away. Every case carries a neighbouring label as the identity
// control: trimming a decoded name into an empty one drops the space label and
// would also report the wrong chain, while a neighbour keeps ordering visible.
func TestRemoveDecodedLabelIdentity(t *testing.T) {
	for _, test := range []struct {
		name, query string
		labels      []string
	}{
		{"single space", "MATCH (n) REMOVE n:` ` RETURN n", []string{" "}},
		{"space then neighbour", "MATCH (n) REMOVE n:` `:`b` RETURN n", []string{" ", "b"}},
		{"neighbour then space", "MATCH (n) REMOVE n:`b`:` ` RETURN n", []string{"b", " "}},
		{"plain control", "MATCH (n) REMOVE n:Old RETURN n", []string{"Old"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			built, err := NewASTBuilder().Build(ctx, test.query)
			require.NoError(t, err)
			info := mustAnalyze(t, NewQueryAnalyzer(4), ctx, test.query)
			cached, err := info.GetAST(ctx)
			require.NoError(t, err)
			// The public builder and the cached analysis are the two AST
			// products a consumer reads; both must expose the same item.
			for _, ast := range []*AST{built, cached} {
				require.NotNil(t, ast)
				require.Len(t, ast.Clauses, 3)
				require.NotNil(t, ast.Clauses[1].Remove)
				require.Len(t, ast.Clauses[1].Remove.Items, 1)
				item := ast.Clauses[1].Remove.Items[0]
				require.Equal(t, "n", item.Variable)
				require.Equal(t, test.labels, item.Labels)
				require.Empty(t, item.Property)
			}
		})
	}
}

// The same decoded label must reach the execution owner intact: the public
// Execute path reads the label chain again (setLabelChainItems), so a label
// decoded as a single space removes exactly that label and leaves its
// neighbour alone. LabelsRemoved pins the count, and the reloaded node pins
// the stored name.
func TestRemoveDecodedLabelIdentityPublicExecute(t *testing.T) {
	ctx := context.Background()

	t.Run("space removed with neighbour preserved", func(t *testing.T) {
		exec, store := newTestExecutor(t)
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:         "space-neighbour",
			Labels:     []string{" ", "keep"},
			Properties: map[string]interface{}{"name": "x"},
		})
		require.NoError(t, err)

		result, err := exec.Execute(ctx, "MATCH (n:keep) REMOVE n:` ` RETURN n", nil)
		require.NoError(t, err)
		require.Equal(t, 1, result.Stats.LabelsRemoved)

		updated, err := store.GetNode(t.Context(), "space-neighbour")
		require.NoError(t, err)
		require.Equal(t, []string{"keep"}, updated.Labels)
	})

	t.Run("space and neighbour removed in one chain", func(t *testing.T) {
		exec, store := newTestExecutor(t)
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:         "space-chain",
			Labels:     []string{" ", "b"},
			Properties: map[string]interface{}{"name": "y"},
		})
		require.NoError(t, err)

		result, err := exec.Execute(ctx, "MATCH (n:b) REMOVE n:` `:`b` RETURN n", nil)
		require.NoError(t, err)
		require.Equal(t, 2, result.Stats.LabelsRemoved)

		updated, err := store.GetNode(t.Context(), "space-chain")
		require.NoError(t, err)
		require.Empty(t, updated.Labels)
	})
}
