package storage_test

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type currentNodeReadCapability interface {
	IsCurrentTemporalNode(*storage.Node, time.Time) (bool, error)
}

type accessReadCapability interface {
	RecordMaterializedAccess(string)
}

type labelCountReadCapability interface {
	NodeCountByLabel(string) (int64, error)
}

type decayReadCapability interface{ IsDecayEnabled() bool }
type suppressionReadCapability interface{ HasManualSuppressions() bool }

func checkReadCapability[T any](t *testing.T, wrapped, view storage.Engine) {
	t.Helper()
	expected, supported := view.(T)
	actual, found := capabilityForTest[T](wrapped)
	require.Equal(t, supported, found)
	if supported {
		require.Equal(t, expected, actual)
	}
}

// The population is the optional interfaces asserted by MATCH, node hydration
// and traversal, including the storage label-ID helper they call.
func TestReadCapabilityForEnginePreservesMatcherCapabilities(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	view := storage.NewNamespacedEngine(base, "tenant")
	for name, engine := range map[string]storage.Engine{
		"direct":     view,
		"traced":     storage.NewTracedEngine(view),
		"wal":        storage.NewWALEngine(view, nil),
		"wal-traced": storage.NewWALEngine(storage.NewTracedEngine(view), nil),
		"traced-wal": storage.NewTracedEngine(storage.NewWALEngine(view, nil)),
	} {
		t.Run(name, func(t *testing.T) {
			checkReadCapability[storage.ProjectedLabelNodeReader](t, engine, view)
			checkReadCapability[storage.ProjectedNodeReader](t, engine, view)
			checkReadCapability[storage.StreamingEngine](t, engine, view)
			checkReadCapability[storage.QueryNodeReader](t, engine, view)
			checkReadCapability[storage.EdgeHeaderReader](t, engine, view)
			checkReadCapability[storage.LabelIndexEngine](t, engine, view)
			checkReadCapability[storage.LabelNodeIDLookupEngine](t, engine, view)
			checkReadCapability[storage.AdjacentEdgesEngine](t, engine, view)
			checkReadCapability[storage.GraphMutationVersionProvider](t, engine, view)
			checkReadCapability[currentNodeReadCapability](t, engine, view)
			checkReadCapability[accessReadCapability](t, engine, view)
			checkReadCapability[labelCountReadCapability](t, engine, view)
			checkReadCapability[decayReadCapability](t, engine, view)
			checkReadCapability[suppressionReadCapability](t, engine, view)
		})
	}
}

type opaqueReadView struct{ storage.Engine }

func (v *opaqueReadView) GetInnerEngine() storage.Engine { return v.Engine }

type delegatedReadView struct{ storage.Engine }

func (v *delegatedReadView) ReadCapabilitySource() (storage.Engine, bool) {
	return v.Engine, true
}

func (v *delegatedReadView) CapabilityOwnerName() string { return "delegated-owner" }

// A new decorator participates by declaring the contract; the resolver does
// not need another concrete-type arm or a collection of forwarding methods.
func TestReadCapabilityForEngineUsesDeclaredSource(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	view := storage.NewNamespacedEngine(base, "tenant")
	delegated := &delegatedReadView{Engine: view}
	reader, ok := capabilityForTest[storage.ProjectedLabelNodeReader](delegated)
	require.True(t, ok)
	require.Same(t, view, reader)
	_, ok = storage.OwnedCapability[storage.ProjectedLabelNodeReader](delegated)
	require.False(t, ok, "operation ownership must not silently unwrap a decorator")
	_, ok = storage.OwnedCapability[storage.ProjectedLabelNodeReader](nil)
	require.False(t, ok)
	_, transparent := view.ReadCapabilitySource()
	require.False(t, transparent)
	composite := storage.NewCompositeEngine(nil, nil, nil)
	_, transparent = composite.ReadCapabilitySource()
	require.False(t, transparent)
}

func TestMetadataCapabilityRetainsWrapperOwner(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	view := storage.NewNamespacedEngine(base, "tenant")
	owner := &delegatedReadView{Engine: view}
	outer := storage.NewTracedEngine(owner)
	type metadata interface{ CapabilityOwnerName() string }
	actual, found := storage.MetadataCapabilityForEngine[metadata](outer)
	require.True(t, found)
	require.Same(t, owner, actual)
	require.Equal(t, "delegated-owner", actual.CapabilityOwnerName())
	_, found = capabilityForTest[metadata](outer)
	require.False(t, found, "graph reads belong to the declared inner view")
	_, found = storage.OwnedCapability[metadata](outer)
	require.False(t, found, "operation discovery must retain the exact wrapper")
	_, found = storage.MetadataCapabilityForEngine[metadata](&opaqueReadView{Engine: owner})
	require.False(t, found, "metadata discovery must stop at an undeclared view")
	_, found = storage.MetadataCapabilityForEngine[metadata](view)
	require.False(t, found)
}

func TestReadCapabilityForEngineStopsAtReadView(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	view := &opaqueReadView{Engine: base}
	_, found := capabilityForTest[storage.ProjectedLabelNodeReader](storage.NewWALEngine(storage.NewTracedEngine(view), nil))
	require.False(t, found, "a view without a capability must retain its fallback")
	_, found = capabilityForTest[storage.ProjectedLabelNodeReader](nil)
	require.False(t, found)
	_, found = capabilityForTest[storage.ProjectedLabelNodeReader](storage.NewWALEngine(nil, nil))
	require.False(t, found)
}

func TestReadCapabilityForEngineStopsAtSelfSource(t *testing.T) {
	self := &delegatedReadView{}
	self.Engine = self
	for name, engine := range map[string]storage.Engine{
		"self":      self,
		"decorated": storage.NewWALEngine(storage.NewTracedEngine(self), nil),
	} {
		t.Run(name, func(t *testing.T) {
			_, found := capabilityForTest[storage.ProjectedLabelNodeReader](engine)
			require.False(t, found, "a terminal delegating source has no underlying read view")
			type missingMetadata interface{ ReadCapabilityTestMetadata() }
			_, found = storage.MetadataCapabilityForEngine[missingMetadata](engine)
			require.False(t, found)
			type metadata interface{ CapabilityOwnerName() string }
			owner, found := storage.MetadataCapabilityForEngine[metadata](engine)
			require.True(t, found)
			require.Same(t, self, owner, "wrapper-owned metadata remains available")
		})
	}
}

func TestReadCapabilityIdentityKeepsEmptyReadBoundary(t *testing.T) {
	inner := storage.NewNamespacedEngine(nil, "tenant")
	outer := storage.NewNamespacedEngine(inner, "")
	engine := storage.NewTracedEngine(outer)
	// Access metadata retains the existing first nonempty physical namespace.
	require.Equal(t, "tenant:a:b", storage.QualifiedEntityID(engine, "a:b"))
	// Graph reads and wrapper metadata still belong to the outer view.
	type namespace interface{ Namespace() string }
	view, found := capabilityForTest[namespace](engine)
	require.True(t, found)
	require.Same(t, outer, view)
	require.Empty(t, view.Namespace())
	view, found = storage.MetadataCapabilityForEngine[namespace](engine)
	require.True(t, found)
	require.Same(t, outer, view)
	require.Empty(t, storage.DatabaseOf(engine), "logical identity retains the empty outer view")
}

// Entity provenance uses the same view owner as query metadata, even when a
// boundary offers only core reads. An EngineUnwrapper alone grants no access.
func TestReadMetadataEntityProvenanceUsesViewOwner(t *testing.T) {
	namespace := storage.NewNamespacedEngine(nil, "tenant")
	composite := storage.NewCompositeEngine(nil, nil, nil)
	composite.SetCompositeName("comp")
	for name, test := range map[string]struct {
		owner     storage.Engine
		namespace string
		composite string
	}{
		"namespace": {namespace, "tenant", ""},
		"composite": {composite, "", "comp"},
	} {
		t.Run(name, func(t *testing.T) {
			wrapped := storage.NewTracedEngine(storage.NewWALEngine(test.owner, nil))
			require.Equal(t, test.namespace, storage.DatabaseOf(wrapped))
			require.Equal(t, test.composite, storage.CompositeDatabaseOf(wrapped))
			boundary := storage.NewTracedEngine(&opaqueReadView{Engine: test.owner})
			require.Empty(t, storage.DatabaseOf(boundary))
			require.Empty(t, storage.CompositeDatabaseOf(boundary))
		})
	}
}
