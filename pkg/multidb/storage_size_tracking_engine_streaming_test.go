package multidb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sizeTrackingStreamingInner struct {
	storage.Engine
	nodes                     []*storage.Node
	streamNodesCalls          int
	streamPrefixCalls         int
	lastPrefix                string
	streamLabelProjectedCalls int
	lastProjectedLabel        string
	lastProjectedProperties   []string
}

type sizeTrackingLifecycleInner struct {
	storage.Engine
	status      map[string]interface{}
	triggered   int
	paused      int
	resumed     int
	readerCount int
	interval    time.Duration
	debtKeys    []storage.MVCCLifecycleDebtKey
}

func (e *sizeTrackingLifecycleInner) RegisterSnapshotReader(info storage.SnapshotReaderInfo) func() {
	e.readerCount++
	return func() {
		e.readerCount--
	}
}

func (e *sizeTrackingLifecycleInner) LifecycleStatus() map[string]interface{} {
	if e.status == nil {
		return map[string]interface{}{"enabled": true}
	}
	copyStatus := make(map[string]interface{}, len(e.status))
	for key, value := range e.status {
		copyStatus[key] = value
	}
	return copyStatus
}

func (e *sizeTrackingLifecycleInner) TriggerPruneNow(context.Context) error {
	e.triggered++
	return nil
}

func (e *sizeTrackingLifecycleInner) PauseLifecycle() {
	e.paused++
}

func (e *sizeTrackingLifecycleInner) ResumeLifecycle() {
	e.resumed++
}

func (e *sizeTrackingLifecycleInner) SetLifecycleSchedule(interval time.Duration) error {
	e.interval = interval
	if e.status == nil {
		e.status = make(map[string]interface{})
	}
	e.status["cycle_interval"] = interval.String()
	e.status["automatic"] = interval > 0
	return nil
}

func (e *sizeTrackingLifecycleInner) TopLifecycleDebtKeys(limit int) []storage.MVCCLifecycleDebtKey {
	if limit <= 0 || limit >= len(e.debtKeys) {
		return append([]storage.MVCCLifecycleDebtKey(nil), e.debtKeys...)
	}
	return append([]storage.MVCCLifecycleDebtKey(nil), e.debtKeys[:limit]...)
}

func (e *sizeTrackingStreamingInner) StreamNodes(_ context.Context, fn func(node *storage.Node) error) error {
	e.streamNodesCalls++
	for _, node := range e.nodes {
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *sizeTrackingStreamingInner) StreamEdges(_ context.Context, _ func(edge *storage.Edge) error) error {
	return nil
}

func (e *sizeTrackingStreamingInner) StreamNodeChunks(_ context.Context, _ int, fn func(nodes []*storage.Node) error) error {
	return fn(e.nodes)
}

func (e *sizeTrackingStreamingInner) StreamNodesByPrefix(_ context.Context, prefix string, fn func(node *storage.Node) error) error {
	e.streamPrefixCalls++
	e.lastPrefix = prefix
	for _, node := range e.nodes {
		if !strings.HasPrefix(string(node.ID), prefix) {
			continue
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func TestSizeTrackingEngine_StreamNodesByPrefix_Delegates(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	inner := &sizeTrackingStreamingInner{
		Engine: base,
		nodes: []*storage.Node{
			{ID: "tenant_a:n1"},
			{ID: "tenant_b:n2"},
			{ID: "tenant_a:n3"},
		},
	}

	wrappedEngine := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant_a")
	prefixStreamer, ok := wrappedEngine.(storage.PrefixStreamingEngine)
	require.True(t, ok, "size tracking wrapper must preserve PrefixStreamingEngine")

	var got []storage.NodeID
	err := prefixStreamer.StreamNodesByPrefix(context.Background(), "tenant_a:", func(node *storage.Node) error {
		got = append(got, node.ID)
		if len(got) == 1 {
			return storage.ErrIterationStopped
		}
		return nil
	})
	// Delegated prefix stream returns ErrIterationStopped as-is; caller handles it.
	require.ErrorIs(t, err, storage.ErrIterationStopped)
	assert.Equal(t, 1, inner.streamPrefixCalls)
	assert.Equal(t, 0, inner.streamNodesCalls)
	assert.Equal(t, "tenant_a:", inner.lastPrefix)
	assert.Equal(t, []storage.NodeID{"tenant_a:n1"}, got)
}

func (e *sizeTrackingStreamingInner) StreamNodesByLabelProjected(label string, properties []string, fn func(node *storage.Node) error) error {
	e.streamLabelProjectedCalls++
	e.lastProjectedLabel = label
	e.lastProjectedProperties = properties
	for _, node := range e.nodes {
		matched := false
		for _, l := range node.Labels {
			if l == label {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

// TestSizeTrackingEngine_StreamNodesByLabelProjected_Delegates proves the
// wrapper forwards label-scoped candidate streaming to the inner engine
// instead of silently dropping storage.ProjectedLabelNodeReader. Eshu#7014
// cause A: a wrapper that only embeds storage.Engine promotes exactly the
// methods declared on that interface, not every method the dynamic inner
// engine happens to implement, so an unforwarded optional interface makes a
// label-scoped MATCH fall through to a whole-namespace storage.StreamNodes
// scan (O(store) instead of O(label)).
func TestSizeTrackingEngine_StreamNodesByLabelProjected_Delegates(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	inner := &sizeTrackingStreamingInner{
		Engine: base,
		nodes: []*storage.Node{
			{ID: "tenant_a:n1", Labels: []string{"Repository"}},
			{ID: "tenant_a:n2", Labels: []string{"Unrelated"}},
			{ID: "tenant_a:n3", Labels: []string{"Repository"}},
		},
	}

	wrappedEngine := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant_a")
	labelReader, ok := wrappedEngine.(storage.ProjectedLabelNodeReader)
	require.True(t, ok, "size tracking wrapper must preserve ProjectedLabelNodeReader")

	var got []storage.NodeID
	err := labelReader.StreamNodesByLabelProjected("Repository", []string{"name"}, func(node *storage.Node) error {
		got = append(got, node.ID)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, inner.streamLabelProjectedCalls)
	assert.Equal(t, 0, inner.streamNodesCalls, "label-scoped MATCH must not fall back to a whole-namespace stream")
	assert.Equal(t, "Repository", inner.lastProjectedLabel)
	assert.Equal(t, []string{"name"}, inner.lastProjectedProperties)
	assert.Equal(t, []storage.NodeID{"tenant_a:n1", "tenant_a:n3"}, got)
}

func TestSizeTrackingEngine_ForEachNodeIDByLabel_Delegates(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	for i := 0; i < 5; i++ {
		_, err := base.CreateNode(&storage.Node{
			ID:     storage.NodeID("tenant_a:n-" + string(rune('0'+i))),
			Labels: []string{"Person"},
		})
		require.NoError(t, err)
	}

	wrapped := newSizeTrackingEngine(base, &DatabaseManager{}, "tenant_a")
	lookup, ok := wrapped.(storage.LabelNodeIDLookupEngine)
	require.True(t, ok, "size tracking wrapper must preserve LabelNodeIDLookupEngine")

	var count int
	err := lookup.ForEachNodeIDByLabel("Person", func(id storage.NodeID) bool {
		count++
		return count < 2
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestSizeTrackingEngine_MVCCLifecycleDelegates(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	inner := &sizeTrackingLifecycleInner{
		Engine: base,
		status: map[string]interface{}{
			"enabled":       true,
			"pressure_band": string(storage.PressureNormal),
		},
		debtKeys: []storage.MVCCLifecycleDebtKey{{LogicalKey: "tenant_a:key", DebtBytes: 42}},
	}

	wrappedEngine := newSizeTrackingEngine(inner, &DatabaseManager{}, "tenant_a")
	lifecycleEngine, ok := wrappedEngine.(storage.MVCCLifecycleEngine)
	require.True(t, ok, "size tracking wrapper must preserve MVCCLifecycleEngine")

	status := lifecycleEngine.LifecycleStatus()
	assert.Equal(t, true, status["enabled"])
	assert.Equal(t, string(storage.PressureNormal), status["pressure_band"])

	deregister := lifecycleEngine.RegisterSnapshotReader(storage.SnapshotReaderInfo{Namespace: "tenant_a"})
	assert.Equal(t, 1, inner.readerCount)
	deregister()
	assert.Equal(t, 0, inner.readerCount)

	require.NoError(t, lifecycleEngine.TriggerPruneNow(context.Background()))
	assert.Equal(t, 1, inner.triggered)

	lifecycleEngine.PauseLifecycle()
	lifecycleEngine.ResumeLifecycle()
	assert.Equal(t, 1, inner.paused)
	assert.Equal(t, 1, inner.resumed)

	scheduler, ok := wrappedEngine.(storage.MVCCLifecycleScheduleEngine)
	require.True(t, ok, "size tracking wrapper must preserve MVCCLifecycleScheduleEngine")
	require.NoError(t, scheduler.SetLifecycleSchedule(3*time.Minute))
	assert.Equal(t, 3*time.Minute, inner.interval)

	debtProvider, ok := wrappedEngine.(storage.MVCCLifecycleDebtEngine)
	require.True(t, ok, "size tracking wrapper must preserve MVCCLifecycleDebtEngine")
	debtKeys := debtProvider.TopLifecycleDebtKeys(1)
	require.Len(t, debtKeys, 1)
	assert.Equal(t, int64(42), debtKeys[0].DebtBytes)
}
