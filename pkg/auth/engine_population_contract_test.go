package auth

import (
	"github.com/orneryd/nornicdb/internal/enginecontract"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

var enginePopulationAdapters = map[string]enginecontract.Adapter{
	"github.com/orneryd/nornicdb/pkg/auth.storeTransaction": {Factory: func(t *testing.T) enginecontract.Fixture {
		base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { require.NoError(t, base.Close()) })
		tx, err := base.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		_, err = tx.CreateNode(&storage.Node{ID: "system:pending", Labels: []string{"L"}})
		require.NoError(t, err)
		view := &storeTransaction{Engine: base, tx: tx}
		binding := storage.MembershipBinding{Owner: tx, Adapter: view, Enforcement: base}
		return enginecontract.Fixture{Binding: &binding, End: func() { require.NoError(t, tx.Rollback()) }, Engine: view, IDs: []string{"system:pending"}, Membership: storage.MembershipBound, Owner: tx}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNodesOnly},
}

func TestEnginePopulationAdapters(t *testing.T) {
	for name, adapter := range enginePopulationAdapters {
		t.Run(name, func(t *testing.T) { enginecontract.Check(t, name, adapter) })
	}
}
