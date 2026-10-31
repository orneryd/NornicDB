// Author: #1683 lane (Claude), 2026-10-30 (NOR-148).
package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type eventViewUnproven struct{ Engine }

// Two views share an event stream only behind the same publisher and the same
// namespace, reached through forwarders; anything unresolved proves nothing.
func TestSameEventView(t *testing.T) {
	memory := NewMemoryEngine()
	t.Cleanup(func() { _ = memory.Close() })
	other := NewMemoryEngine()
	t.Cleanup(func() { _ = other.Close() })
	base := memory.BadgerEngine

	require.True(t, SameEventView(memory, base), "MemoryEngine is its Badger publisher")
	require.True(t, SameEventView(NewNamespacedEngine(base, "a"), NewNamespacedEngine(memory, "a")), "same namespace over one publisher")
	require.True(t, SameEventView(NewNamespacedEngine(&WALEngine{engine: base}, "a"), &TracedEngine{Engine: NewNamespacedEngine(base, "a")}), "forwarders on either side")
	require.False(t, SameEventView(NewNamespacedEngine(base, "a"), NewNamespacedEngine(base, "b")), "namespaces stay separate")
	require.False(t, SameEventView(NewNamespacedEngine(base, "a"), NewNamespacedEngine(other.BadgerEngine, "a")), "different publishers")
	require.False(t, SameEventView(NewNamespacedEngine(base, "a"), base), "a namespace is not the whole store")
	unproven := &eventViewUnproven{Engine: NewNamespacedEngine(base, "a")}
	require.False(t, SameEventView(unproven, NewNamespacedEngine(base, "a")), "an unresolved wrapper proves nothing")
	require.True(t, SameEventView(unproven, unproven), "an engine is its own view")
	require.False(t, SameEventView(nil, base))
}
