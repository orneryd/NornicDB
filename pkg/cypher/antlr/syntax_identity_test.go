package antlr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A node's identity is its parsed product's: one read of one source in one
// entry and dialect. Two reads of the same text through entries the owner
// aliases share it; a product read for another entry never does. Consumers key
// retained facts (columns, effects, projections, clause splits) by it, so it
// must be exact and comparable, and an absent node must match nothing.
func TestSyntaxIdentityIsOnePerRetainedProduct(t *testing.T) {
	ctx := context.Background()
	source := "RETURN 1 AS x"
	statement, err := ParseSyntax(ctx, source, StatementEntry, false, AcceptedSource)
	require.NoError(t, err)
	require.True(t, statement.Valid())

	fragment, err := ParseSyntax(ctx, source, FragmentEntry, false, AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, statement.Identity(), fragment.Identity(), "the same text read for a statement and a fragment is one product")

	root := fragment.Root()
	require.Equal(t, root.Identity(), root.Statement().Identity())
	require.Equal(t, source, root.Statement().Source())

	clause, err := ParseSyntax(ctx, source, ClauseEntry, false, AcceptedSource)
	require.NoError(t, err)
	require.True(t, clause.Valid())
	require.NotEqual(t, statement.Identity(), clause.Identity(), "another entry reading the same text is another product")
	require.NotEqual(t, root.ID(), -1)

	var absent SyntaxNode
	require.Equal(t, SyntaxIdentity{}, absent.Identity())
	require.False(t, absent.Statement().Valid())
	require.NotEqual(t, SyntaxIdentity{}, statement.Identity())
}
