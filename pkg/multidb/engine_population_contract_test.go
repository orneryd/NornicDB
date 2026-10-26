package multidb

import (
	"github.com/orneryd/nornicdb/internal/enginecontract"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type enginePopulationMember struct {
	storage.Engine
	binding storage.MembershipBinding
}

func (p *enginePopulationMember) StorageMembership() storage.MembershipBinding { return p.binding }
func (p *enginePopulationMember) ReadCapabilitySource() (storage.Engine, bool) { return p.Engine, true }
func enginePopulationMemberSeed(base storage.Engine) (*enginePopulationMember, storage.MembershipBinding, func()) {
	token := new(int)
	expected := storage.MembershipBinding{Status: storage.MembershipBound, Owner: token, Adapter: token, Controller: token, PendingActions: token, SchemaPublication: token, ReadLease: token, StatementMode: token, Enforcement: token}
	member := &enginePopulationMember{Engine: base, binding: expected}
	return member, expected, func() { member.binding.Status = storage.MembershipEnded }
}

var enginePopulationAdapters = map[string]enginecontract.Adapter{
	"github.com/orneryd/nornicdb/pkg/multidb.sizeTrackingEngine": {Factory: func(t *testing.T) enginecontract.Fixture {
		manager, db := setupTestManager(t)
		view, err := manager.GetStorage(db)
		require.NoError(t, err)
		_, err = view.CreateNode(&storage.Node{ID: "n", Labels: []string{"L"}})
		require.NoError(t, err)
		managed := view.(*sizeTrackingEngine)
		member, binding, end := enginePopulationMemberSeed(managed.Engine)
		managed.Engine = member
		return enginecontract.Fixture{Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, Engine: view, IDs: []string{"n"}, Namespace: db}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNative},
}

func TestEnginePopulationAdapters(t *testing.T) {
	for name, adapter := range enginePopulationAdapters {
		t.Run(name, func(t *testing.T) { enginecontract.Check(t, name, adapter) })
	}
}
