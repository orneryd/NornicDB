package cypher

import (
	"github.com/orneryd/nornicdb/internal/enginecontract"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func enginePopulationBase(t *testing.T) *storage.MemoryEngine {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	_, err := base.CreateNode(&storage.Node{ID: "tenant:n", Labels: []string{"L"}})
	require.NoError(t, err)
	return base
}

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
	"github.com/orneryd/nornicdb/pkg/cypher.transactionStorageWrapper": {Factory: func(t *testing.T) enginecontract.Fixture {
		base := enginePopulationBase(t)
		tx, err := base.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		_, err = tx.CreateNode(&storage.Node{ID: "tenant:pending", Labels: []string{"L"}})
		require.NoError(t, err)
		view := &transactionStorageWrapper{tx: tx, underlying: base, namespace: "tenant", separator: ":"}
		binding := storage.MembershipBinding{Owner: view, Adapter: view, Controller: (*StorageExecutor)(nil), PendingActions: view, SchemaPublication: view, ReadLease: view, StatementMode: view, Enforcement: base}
		return enginecontract.Fixture{Binding: &binding, End: func() { require.NoError(t, tx.Rollback()) }, Engine: view, IDs: []string{"n", "pending"}, Membership: storage.MembershipBound, Owner: view}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNative},
	"github.com/orneryd/nornicdb/pkg/cypher.algorithmRelationshipReader": {Factory: func(t *testing.T) enginecontract.Fixture {
		member, binding, end := enginePopulationMemberSeed(enginePopulationBase(t))
		return enginecontract.Fixture{Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, Engine: &algorithmRelationshipReader{Engine: member, types: map[string]struct{}{"R": {}}}, IDs: []string{"tenant:n"}}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredRefused},
	"github.com/orneryd/nornicdb/pkg/cypher.graphIdentityStorage": {Factory: func(t *testing.T) enginecontract.Fixture {
		member, binding, end := enginePopulationMemberSeed(enginePopulationBase(t))
		return enginecontract.Fixture{Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, Engine: &graphIdentityStorage{Engine: member}, IDs: []string{"tenant:n"}}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredRefused},
	"github.com/orneryd/nornicdb/pkg/cypher.systemGraphView": {Factory: func(t *testing.T) enginecontract.Fixture {
		member, binding, end := enginePopulationMemberSeed(enginePopulationBase(t))
		return enginecontract.Fixture{Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, Engine: systemGraphView{Engine: member}, IDs: []string{}, AbsentIDs: []string{"tenant:n"}, Namespace: "system"}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredEmpty},
}

func TestEnginePopulationAdapters(t *testing.T) {
	for name, adapter := range enginePopulationAdapters {
		t.Run(name, func(t *testing.T) { enginecontract.Check(t, name, adapter) })
	}
}
