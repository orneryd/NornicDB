package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func identityCaseManagerFixture(t testing.TB, refs []ConstituentRef) *DatabaseManager {
	t.Helper()
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	fixtureowner.Own(t, manager)
	for _, name := range []string{"identity_one", "identity_two"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: name}))
	}
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "identity_cmp", refs))
	return manager
}

func TestIdentityManagerRuntimeTargetsPreserveCodeUnits(t *testing.T) {
	high, low, literal := "Route_\xed\xa0\x80", "Route_\xed\xb0\x80", `Route_\ud800`
	manager := identityCaseManagerFixture(t, []ConstituentRef{{Alias: high, DatabaseName: "identity_one", Type: "local", AccessMode: "read"}, {Alias: low, DatabaseName: "identity_two", Type: "local", AccessMode: "read_write"}, {Alias: literal, DatabaseName: "identity_one", Type: "local", AccessMode: "read"}})
	for _, name := range []string{"identity_one", "identity_two"} {
		engine, err := manager.GetStorage(name)
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: "marker", Labels: []string{"IdentityMarker"}, Properties: map[string]any{"target": name}})
		require.NoError(t, err)
	}
	for _, tc := range []struct{ name, target string }{{high, "identity_one"}, {low, "identity_two"}, {literal, "identity_one"}, {"ROUTE_\xed\xa0\x80", "identity_one"}, {"ROUTE_\xed\xb0\x80", "identity_two"}} {
		t.Run(tc.name, func(t *testing.T) {
			graph := "identity_cmp." + tc.name
			resolved, err := manager.ResolveConnectionLimitOwner(graph)
			require.NoError(t, err)
			require.Equal(t, tc.target, resolved)
			engine, err := manager.GetStorage(graph)
			require.NoError(t, err)
			marker, err := engine.GetNode("marker")
			require.NoError(t, err)
			require.Equal(t, tc.target, marker.Properties["target"])
		})
	}
	refs, err := manager.GetCompositeConstituents("identity_cmp")
	require.NoError(t, err)
	aliases := make([]string, 0, len(refs))
	for _, ref := range refs {
		aliases = append(aliases, ref.Alias)
	}
	require.ElementsMatch(t, []string{high, low, literal}, aliases)
}
