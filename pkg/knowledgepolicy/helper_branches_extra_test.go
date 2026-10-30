package knowledgepolicy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOnAccessNumericHelpersCoverSupportedTypes(t *testing.T) {
	intCases := []struct {
		value interface{}
		want  int64
	}{
		{int(1), 1},
		{int32(2), 2},
		{int64(3), 3},
		{float32(4.8), 4},
		{float64(5.9), 5},
	}
	for _, tc := range intCases {
		got, ok := toInt64(tc.value)
		require.True(t, ok)
		require.Equal(t, tc.want, got)
	}
	_, ok := toInt64("6")
	require.False(t, ok)

	floatCases := []struct {
		value interface{}
		want  float64
	}{
		{int(1), 1},
		{int32(2), 2},
		{int64(3), 3},
		{float32(4.5), 4.5},
		{float64(5.5), 5.5},
	}
	for _, tc := range floatCases {
		got, ok := toFloat64(tc.value)
		require.True(t, ok)
		require.InDelta(t, tc.want, got, 0.00001)
	}
	_, ok = toFloat64("6")
	require.False(t, ok)
}

func TestOnAccessPropertyGetAndSetBranches(t *testing.T) {
	entry := &AccessMetaEntry{
		Fixed: AccessMetaFixedFields{
			AccessCount:     1,
			LastAccessedAt:  2,
			TraversalCount:  3,
			LastTraversedAt: 4,
		},
		Overflow: map[string]interface{}{"custom": "from-entry"},
	}
	ctx := onAccessEvalContext{entry: entry, entityProps: map[string]interface{}{"custom": "from-entity", "fallback": true}}

	require.Equal(t, int64(1), getOnAccessProperty(ctx, "accessCount"))
	require.Equal(t, int64(2), getOnAccessProperty(ctx, "lastAccessedAt"))
	require.Equal(t, int64(3), getOnAccessProperty(ctx, "traversalCount"))
	require.Equal(t, int64(4), getOnAccessProperty(ctx, "lastTraversedAt"))
	require.Equal(t, "from-entry", getOnAccessProperty(ctx, "custom"))
	require.Equal(t, true, getOnAccessProperty(ctx, "fallback"))
	require.Nil(t, getOnAccessProperty(onAccessEvalContext{}, "missing"))

	require.True(t, setOnAccessProperty(entry, "accessCount", float64(10.9)))
	require.True(t, setOnAccessProperty(entry, "lastAccessedAt", int32(20)))
	require.True(t, setOnAccessProperty(entry, "traversalCount", int64(30)))
	require.True(t, setOnAccessProperty(entry, "lastTraversedAt", int(40)))
	require.Equal(t, AccessMetaFixedFields{AccessCount: 10, LastAccessedAt: 20, TraversalCount: 30, LastTraversedAt: 40}, entry.Fixed)

	require.False(t, setOnAccessProperty(entry, "custom-score", 0.75))
	require.Equal(t, 0.75, entry.Overflow["custom-score"])
	require.False(t, setOnAccessProperty(nil, "accessCount", 1))
}

func TestKnowledgePolicySmallHelpers(t *testing.T) {
	require.Equal(t, "node", scopeToEntityKind(ScopeNode))
	require.Equal(t, "edge", scopeToEntityKind(ScopeEdge))
	require.Equal(t, "property", scopeToEntityKind(ScopeProperty))
	require.Equal(t, "node", scopeToEntityKind(ScopeType("unknown")))

	require.False(t, truthy(nil))
	require.False(t, truthy(false))
	require.False(t, truthy(int64(0)))
	require.False(t, truthy(int(0)))
	require.False(t, truthy(float64(0)))
	require.False(t, truthy(""))
	require.True(t, truthy("false"))
	require.True(t, truthy(struct{}{}))
}

func TestAccessFlusherSuppressionRecheckSetter(t *testing.T) {
	accumulator := NewAccessAccumulator(true, 0)
	flusher := NewAccessFlusher(accumulator, nil, time.Second)

	var gotID string
	var gotMeta EntityMeta
	flusher.SetSuppressionRecheck(func(entityID string, meta EntityMeta) {
		gotID = entityID
		gotMeta = meta
	})
	require.NotNil(t, flusher.suppressionRecheck)

	meta := EntityMeta{Scope: ScopeNode, Labels: []string{"Memory"}, CreatedAtNanos: 11}
	flusher.suppressionRecheck("n1", meta)
	require.Equal(t, "n1", gotID)
	require.Equal(t, meta, gotMeta)

	require.Equal(t, float64(0), (*AccessFlusher)(nil).BufferFullness())
}
