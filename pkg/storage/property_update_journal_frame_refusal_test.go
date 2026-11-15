//go:build wal_large_vectors

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The complete first anchor can span frames. The client update remains bounded
// under both policies; ordinary full-image capacity tests remain separate.
const propertyUpdateFrameRefusalChunks = 17000

func TestPropertyOnlyUpdateAboveOneFrameRecordPolicy(t *testing.T) {
	if testing.Short() {
		t.Skip("explicit large-vector qualification")
	}
	for _, tc := range []struct {
		name   string
		policy WALRecordPolicy
	}{
		{"standard", WALRecordsStandard}, {"large", WALRecordsLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, wrapped := newPropertyUpdateVectorEngine(t, tc.policy)
			const id NodeID = "test:large-property-update"
			seedPropertyUpdateVectorNode(t, engine, wrapped, id, propertyUpdateFrameRefusalChunks)
			wal := wrapped.GetWAL()
			node, err := wrapped.GetNode(t.Context(), id)
			require.NoError(t, err)
			node.Properties["flag"] = true
			before := wal.Sequence()
			require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
			record := propertyUpdateJournalUpdate(t, wal, before)
			require.LessOrEqual(t, record.Bytes, propertyUpdateJournalByteBound)
			require.Equal(t, 1, record.References)
			entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
			require.NoError(t, err)
			anchors := 0
			for _, entry := range entries {
				if entry.Sequence > before && entry.Operation == opManagedVectorAnchor {
					anchors++
					require.Greater(t, len(entry.Data), int(walMaxEntrySize))
					require.Equal(t, walFormatVersionSpanned, entry.framingVersion)
				}
			}
			require.Equal(t, 1, anchors)
			stored, err := wrapped.GetNode(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, true, stored.Properties["flag"])
			requirePropertyUpdateVectors(t, stored, propertyUpdateFrameRefusalChunks)
		})
	}
}
