package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestExtractNamespaceFromID_Variants(t *testing.T) {
	require.Equal(t, "test", ExtractNamespaceFromID("test:n1"))
	require.Equal(t, "ns", ExtractNamespaceFromID("ns:something:nested"),
		"only the first colon delimits the namespace")
	require.Equal(t, "nornic", ExtractNamespaceFromID("noprefix"),
		"missing colon → default 'nornic' namespace")
	require.Equal(t, "nornic", ExtractNamespaceFromID(""))
}

func TestBadgerEngine_IsDecayEnabled_Toggle(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	// Default: decay is off until explicitly enabled.
	require.False(t, be.IsDecayEnabled())

	be.decayEnabled = true
	require.True(t, be.IsDecayEnabled())

	be.decayEnabled = false
	require.False(t, be.IsDecayEnabled())
}

func TestBadgerEngine_IsEmbeddingsEnabled_Toggle(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	// Default false on a fresh engine.
	require.False(t, be.IsEmbeddingsEnabled())

	be.SetEmbeddingsEnabled(true)
	require.True(t, be.IsEmbeddingsEnabled())

	be.SetEmbeddingsEnabled(false)
	require.False(t, be.IsEmbeddingsEnabled())
}

func TestBadgerEngine_DB_HandleNonNil(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })
	require.NotNil(t, be.DB(), "DB() should return the live badger handle")
}

func TestBytesMetricsSweeper_NameStable(t *testing.T) {
	s := &BytesMetricsSweeper{}
	require.Equal(t, "storage_bytes_metrics", s.Name())
}

func TestBadgerEngine_InvalidatePendingEmbeddingsIndex_Noop(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })
	require.NotPanics(t, func() {
		be.InvalidatePendingEmbeddingsIndex()
	})
}

func TestBadgerTransaction_HasPendingNodeMutations(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	tx, err := be.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Brand-new tx: no pending mutations.
	require.False(t, tx.HasPendingNodeMutations())

	// Stage a create — pending now non-empty.
	_, err = tx.CreateNode(&Node{
		ID:     "test:pending-1",
		Labels: []string{"L"},
	})
	require.NoError(t, err)
	require.True(t, tx.HasPendingNodeMutations())
}

func TestBadgerTransaction_HasPendingNodeMutations_DeletionAlsoCounts(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	// Seed a committed node so we can stage a delete in a fresh tx.
	_, err := be.CreateNode(&Node{ID: "test:committed", Labels: []string{"L"}})
	require.NoError(t, err)

	tx, err := be.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.False(t, tx.HasPendingNodeMutations())
	require.NoError(t, tx.DeleteNode("test:committed"))
	require.True(t, tx.HasPendingNodeMutations(),
		"a staged delete should also trigger HasPendingNodeMutations")
}

func TestAsyncEngine_AddSpanLink_InvalidContextDropped(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })
	ae := NewAsyncEngine(be, nil)
	t.Cleanup(func() { _ = ae.Close() })

	// Invalid SpanContext — should be silently ignored, no link recorded.
	require.NotPanics(t, func() {
		ae.AddSpanLink(trace.SpanContext{})
	})
	links := ae.drainSpanLinks()
	require.Empty(t, links, "invalid span context should not be appended")
}

func TestAsyncEngine_AddSpanLink_ValidContextRecorded(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })
	ae := NewAsyncEngine(be, nil)
	t.Cleanup(func() { _ = ae.Close() })

	tid, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	sid, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	require.True(t, sc.IsValid(), "precondition: span context is valid")

	ae.AddSpanLink(sc)
	links := ae.drainSpanLinks()
	require.Len(t, links, 1)
	require.Equal(t, sc, links[0].SpanContext)
}

func TestAsyncEngine_AddSpanLink_CapAt32(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })
	ae := NewAsyncEngine(be, nil)
	t.Cleanup(func() { _ = ae.Close() })

	tid, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)

	makeSC := func(i byte) trace.SpanContext {
		var sidBytes [8]byte
		sidBytes[7] = i + 1 // never all-zero (which would be invalid)
		return trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    tid,
			SpanID:     trace.SpanID(sidBytes),
			TraceFlags: trace.FlagsSampled,
		})
	}

	for i := 0; i < 50; i++ {
		ae.AddSpanLink(makeSC(byte(i)))
	}
	links := ae.drainSpanLinks()
	require.Len(t, links, 32, "AddSpanLink should cap at 32 to bound memory")
}

func TestReadNodeLabelsInTxn_ReturnsLabels(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	_, err := be.CreateNode(&Node{
		ID:     "test:ln",
		Labels: []string{"Alpha", "Beta"},
	})
	require.NoError(t, err)

	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		got, err := be.readNodeLabelsInTxn(txn, "test:ln")
		require.NoError(t, err)
		require.Equal(t, []string{"Alpha", "Beta"}, got)
		return nil
	}))
}

func TestReadNodeLabelsInTxn_MissingNode(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		_, err := be.readNodeLabelsInTxn(txn, "test:doesnotexist")
		require.Error(t, err)
		return nil
	}))
}
