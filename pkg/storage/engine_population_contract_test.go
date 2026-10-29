package storage_test

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/enginecontract"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	remoteFixture "github.com/orneryd/nornicdb/internal/testfixture/remotequery"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func populationBase(t *testing.T) *storage.MemoryEngine {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for _, id := range []storage.NodeID{"tenant:n", "foreign:sentinel"} {
		_, err := base.CreateNode(&storage.Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	return base
}

type populationMember struct {
	storage.Engine
	binding storage.MembershipBinding
}

func (p *populationMember) StorageMembership() storage.MembershipBinding { return p.binding }
func (p *populationMember) ReadCapabilitySource() (storage.Engine, bool) { return p.Engine, true }
func populationMemberSeed(base storage.Engine) (*populationMember, storage.MembershipBinding, func()) {
	token := new(int)
	expected := storage.MembershipBinding{Status: storage.MembershipBound, Owner: token, Adapter: token, Controller: token, PendingActions: token, SchemaPublication: token, ReadLease: token, StatementMode: token, Enforcement: token}
	member := &populationMember{Engine: base, binding: expected}
	return member, expected, func() { member.binding.Status = storage.MembershipEnded }
}

var enginePopulationAdapters = map[string]enginecontract.Adapter{
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault.ReadFailureEngine": {Factory: func(t *testing.T) enginecontract.Fixture {
		member, binding, end := populationMemberSeed(populationBase(t))
		reader, ok := storage.StoredReader(member)
		require.True(t, ok)
		return enginecontract.Fixture{Engine: storagefault.NewReadFailureEngine(storage.NewWALEngine(member, nil), reader, fmt.Errorf("injected read failure")), Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, IDs: []string{"tenant:n", "foreign:sentinel"}}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNative},

	"github.com/orneryd/nornicdb/pkg/storage.MemoryEngine": {Factory: func(t *testing.T) enginecontract.Fixture {
		return enginecontract.Fixture{Engine: populationBase(t), IDs: []string{"tenant:n", "foreign:sentinel"}}
	}, Oracle: func(t *testing.T, fixture enginecontract.Fixture) {
		enginecontract.Rows(t, fixture)
		native := fixture.Engine.(*storage.MemoryEngine)
		require.Same(t, native.BadgerEngine, storage.UnwrapEngine(native), "native logical leaf still declares its physical wrapper identity")
	}, Stored: enginecontract.StoredNative},
	"github.com/orneryd/nornicdb/pkg/storage.BadgerEngine": {Factory: func(t *testing.T) enginecontract.Fixture {
		return enginecontract.Fixture{Engine: populationBase(t).BadgerEngine, IDs: []string{"tenant:n", "foreign:sentinel"}}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNative},
	"github.com/orneryd/nornicdb/pkg/storage.TracedEngine": {Factory: func(t *testing.T) enginecontract.Fixture {
		member, binding, end := populationMemberSeed(populationBase(t))
		return enginecontract.Fixture{Engine: storage.NewTracedEngine(member), Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, IDs: []string{"tenant:n", "foreign:sentinel"}}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNative},
	"github.com/orneryd/nornicdb/pkg/storage.WALEngine": {Factory: func(t *testing.T) enginecontract.Fixture {
		member, binding, end := populationMemberSeed(populationBase(t))
		return enginecontract.Fixture{Engine: storage.NewWALEngine(member, nil), Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, IDs: []string{"tenant:n", "foreign:sentinel"}}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNative},
	"github.com/orneryd/nornicdb/pkg/storage.NamespacedEngine": {Factory: func(t *testing.T) enginecontract.Fixture {
		member, binding, end := populationMemberSeed(populationBase(t))
		return enginecontract.Fixture{Engine: storage.NewNamespacedEngine(member, "tenant"), Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, IDs: []string{"n"}, Namespace: "tenant"}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNative},
	"github.com/orneryd/nornicdb/pkg/storage.CompositeEngine": {Factory: func(t *testing.T) enginecontract.Fixture {
		n := storage.NewNamespacedEngine(populationBase(t), "tenant")
		member, binding, end := populationMemberSeed(nil)
		return enginecontract.Fixture{Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, Engine: storage.NewCompositeEngine(map[string]storage.Engine{"a": n, "alias": n}, map[string]string{"a": "tenant", "alias": "tenant"}, map[string]string{"a": "read", "alias": "read"}).WithMembership(member), IDs: []string{"n"}}
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredNative},
	"github.com/orneryd/nornicdb/pkg/storage.RemoteEngine": {Factory: populationRemoteFixture, Oracle: enginecontract.Rows, Stored: enginecontract.StoredRefused},
	"github.com/orneryd/nornicdb/internal/testfixture/remotequery.Observer": {Factory: func(t *testing.T) enginecontract.Fixture {
		f := populationRemoteFixture(t)
		f.Engine = &remoteFixture.Observer{RemoteEngine: f.Engine.(*storage.RemoteEngine)}
		return f
	}, Oracle: enginecontract.Rows, Stored: enginecontract.StoredRefused},
}

func TestEnginePopulationAdapters(t *testing.T) {
	for name, adapter := range enginePopulationAdapters {
		t.Run(name, func(t *testing.T) { enginecontract.Check(t, name, adapter) })
	}
}

func populationRemoteFixture(t *testing.T) enginecontract.Fixture {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"results":[{"columns":["n"],"data":[{"row":[{"id":"remote","labels":["L"],"properties":{}}]}]}],"errors":[]}`)
	}))
	t.Cleanup(server.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remote.Close()) })
	member, binding, end := populationMemberSeed(nil)
	return enginecontract.Fixture{Membership: storage.MembershipBound, Owner: binding.Owner, Binding: &binding, End: end, Engine: remote.WithMembership(member), IDs: []string{"remote"}}
}
