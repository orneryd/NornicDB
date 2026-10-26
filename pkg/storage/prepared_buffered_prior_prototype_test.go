package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPreparedPriorSurvivesAcknowledgedBufferRetirement(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	seed, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, seed.SetImplicit(true))
	_, err = seed.CreateNode(&Node{ID: "test:prior", Properties: map[string]any{"value": "old"}})
	require.NoError(t, err)
	require.NoError(t, seed.Commit())
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:prior", Properties: map[string]any{"value": "new"}}, ClientNodeEdits))
	// The canonical coherent-usage initializer drains before preparation. The
	// transaction began after this ACK and must retain its actual prior image.
	require.NoError(t, b.FlushWriteBehind())
	prepared, err := tx.PrepareEntityCommit(context.Background(), "logical-after-drain")
	require.NoError(t, err)
	defer prepared.Abort()
	changes := prepared.Changes()
	require.Len(t, changes, 1)
	require.NotNil(t, changes[0].BeforeNode)
	require.Equal(t, "old", changes[0].BeforeNode.Properties["value"])
	require.Equal(t, "new", changes[0].AfterNode.Properties["value"])
}
