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
	actual, found := storage.ReadCapabilityForEngine[T](wrapped)
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

func TestReadCapabilityForEngineStopsAtReadView(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	view := &opaqueReadView{Engine: base}
	_, found := storage.ReadCapabilityForEngine[storage.ProjectedLabelNodeReader](storage.NewWALEngine(storage.NewTracedEngine(view), nil))
	require.False(t, found, "a view without a capability must retain its fallback")
	_, found = storage.ReadCapabilityForEngine[storage.ProjectedLabelNodeReader](nil)
	require.False(t, found)
	_, found = storage.ReadCapabilityForEngine[storage.ProjectedLabelNodeReader](storage.NewWALEngine(nil, nil))
	require.False(t, found)
}
