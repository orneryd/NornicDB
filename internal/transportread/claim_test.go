package transportread

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTakeConsumesTheClaimOnce covers the capability's one rule: the claim a
// transport sets belongs to one statement, and the context it returns carries no
// claim for the statements that statement runs.
func TestTakeConsumesTheClaimOnce(t *testing.T) {
	statement, claimed := Take(Claim(context.Background()))
	require.True(t, claimed, "a claimed context carries the claim")

	nested, again := Take(statement)
	require.False(t, again, "the claim is taken once, so a statement-run statement has none")
	require.NotNil(t, nested)

	unclaimed, claimed := Take(context.Background())
	require.False(t, claimed, "an unclaimed context carries no claim")
	require.NotNil(t, unclaimed)
}
