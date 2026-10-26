package replication

import (
	"github.com/orneryd/nornicdb/internal/enginecontract"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
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
	"github.com/orneryd/nornicdb/pkg/replication.ReplicatedEngine": {Factory: func(t *testing.T) enginecontract.Fixture {
		base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { require.NoError(t, base.Close()) })
		_, err := base.CreateNode(&storage.Node{ID: "tenant:n", Labels: []string{"L"}})
		require.NoError(t, err)
		member, binding, end := enginePopulationMemberSeed(base)
		return enginecontract.Fixture{Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, Engine: NewReplicatedEngine(member, nil, 0), IDs: []string{"tenant:n"}}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNative},
}

func TestEnginePopulationAdapters(t *testing.T) {
	for name, adapter := range enginePopulationAdapters {
		t.Run(name, func(t *testing.T) { enginecontract.Check(t, name, adapter) })
	}
}
