package enginecontract

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// StoredModes crosses each registered real Engine with all three public stored
// consumers. Expectations come from the independent fixture, never dispatch.
func StoredModes(t *testing.T, f Fixture, disposition StoredDisposition) {
	t.Helper()
	require.True(t, disposition >= StoredNative && disposition <= StoredRefused, "missing independent stored disposition")
	declaration, ok := f.Engine.(storage.StoredReadDeclaration)
	require.True(t, ok, "real view must explicitly declare its stored contract")
	expected := storage.NativeStoredReadPolicy()
	if disposition == StoredRefused {
		expected = storage.RefusedStoredReadPolicy()
	}
	if disposition == StoredNodesOnly {
		expected.Edges = storage.BindingRefused
		expected.Adjacency = storage.BindingRefused
	}
	require.Equal(t, expected, declaration.StoredReadPolicy())
	ids := make([]storage.NodeID, len(f.IDs))
	for i, id := range f.IDs {
		ids[i] = storage.NodeID(id)
	}
	if len(ids) == 0 {
		ids = []storage.NodeID{"sentinel"}
	}
	for _, mode := range []storage.StoredReadMode{storage.StoredMetadataOnly, storage.StoredSelectedProperties, storage.StoredFullMaterialization} {
		result, err := storage.ReadStored(context.Background(), f.Engine, storage.StoredReadRequest{Mode: mode, Nodes: ids, NodeProperties: []string{}, Physical: true})
		if disposition == StoredRefused {
			require.ErrorIs(t, err, storage.ErrNotImplemented)
			require.Nil(t, result)
			continue
		}
		require.NoError(t, err)
		facts := result.StoredFacts()
		var got []string
		for _, fact := range facts.Nodes {
			if fact.Exists {
				_, _, local, qualified := storage.ParseElementID(string(fact.ID))
				if qualified {
					got = append(got, local)
				} else {
					got = append(got, string(fact.ID))
				}
			}
		}
		want := append([]string{}, f.IDs...)
		if disposition == StoredEmpty {
			want = []string{}
		}
		sort.Strings(got)
		sort.Strings(want)
		require.Equal(t, want, append([]string{}, got...), "stored view leaked or lost a seeded member")
		switch mode {
		case storage.StoredMetadataOnly:
			_, ok := result.(storage.StoredMetadata)
			require.True(t, ok, "metadata has no payload carrier")
		case storage.StoredSelectedProperties:
			value, ok := result.(storage.StoredProjection)
			require.True(t, ok)
			for _, node := range value.Nodes {
				require.Empty(t, node.Properties, "empty explicit projection may not copy the property map")
			}
		case storage.StoredFullMaterialization:
			_, ok := result.(storage.StoredFullRead)
			require.True(t, ok)
		}
	}
}
