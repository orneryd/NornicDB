package nornicdb

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// A direct node update must move pending relationships to the new label's
// count bucket, independently of JSON restore.
func TestDirectNodeRelabelKeepsAcknowledgedRelationshipCounts(t *testing.T) {
	db, base, _, y := openBufferedRelationshipRelabelFixture(t)
	store := db.GetStorage()
	held, err := store.GetNode(t.Context(), storage.NodeID(y.ID))
	require.NoError(t, err)
	require.Equal(t, []string{"Held"}, held.Labels)
	held.Labels = []string{"After"}
	require.NoError(t, store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: held}))
	require.NoError(t, base.FlushWriteBehind(t.Context()))
	requireRelabelledRelationshipCounts(t, db)
}
