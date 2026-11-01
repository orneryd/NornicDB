package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// LOAD CSV expands a source row stream even when its destination is remote.
// The effect owner must describe that boundary without fetching the resource.
func TestLoadCSVEffectExpandsRows(t *testing.T) {
	effects, err := pipelineClauseEffectsFor(context.Background(), "LOAD CSV FROM 'file:///unused' AS row")
	require.NoError(t, err)
	require.True(t, effects.expands, "a resource row stream expands rows")
	require.False(t, effects.barrier, "a read clause does not become a write barrier")
	require.Nil(t, effects.batch)
}
