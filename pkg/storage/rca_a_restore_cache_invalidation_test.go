package storage

// Root-cause diagnostic (campaign v1.5.2, lane I, group G-restore-cache-invalidation; run by lane A).
// Test-side only. Same fixture as TestNOR210UnrelatedRelationshipTypeCacheSurvivesPrefixDrop.
// The relationship-type cache is keyed by the bare type name across all
// namespaces (edgeTypeCache map[string][]*Edge). deleteByPrefixHeld replaces
// the whole map (badger_backup.go:579-581) after the drop. The test hook
// dropPrefixTailHook (badger_backup.go:425-429) runs before that point, so the
// diagnostic logs the peer entry and the invalidation generation at the hook
// and after DeleteByPrefix returns. The variant invalidates only the dropped
// database's type through the existing per-type API.

import (
	"testing"
)

func TestRCAPrefixDropClearsWholeTypeCache(t *testing.T) {
	engine := newTestEngine(t)
	seedRelationshipType(t, engine, "victim", "NOR210_VICTIM", 2)
	seedRelationshipType(t, engine, "peer", "NOR210_KEEP", 2)
	victim := NewNamespacedEngine(engine, "victim")
	peer := NewNamespacedEngine(engine, "peer")
	warmRelationshipType(t, victim, "NOR210_VICTIM", 2)
	warmRelationshipType(t, peer, "NOR210_KEEP", 2)
	t.Logf("RCA_TYPECACHE before drop: entries=%d peerIDs=%v gen=%d", relationshipTypeCacheEntryCount(engine), cachedRelationshipTypeIDs(engine, "NOR210_KEEP"), engine.edgeTypeCacheGen.current())
	engine.dropPrefixTailHook = func(prefix string) error {
		t.Logf("RCA_TYPECACHE at tail hook (prefix %q): entries=%d peerIDs=%v gen=%d", prefix, relationshipTypeCacheEntryCount(engine), cachedRelationshipTypeIDs(engine, "NOR210_KEEP"), engine.edgeTypeCacheGen.current())
		return nil
	}
	_, removed, err := engine.DeleteByPrefix("victim:")
	engine.dropPrefixTailHook = nil
	t.Logf("RCA_TYPECACHE after DeleteByPrefix(victim:) removedEdges=%d err=%v: entries=%d peerIDs=%v gen=%d", removed, err, relationshipTypeCacheEntryCount(engine), cachedRelationshipTypeIDs(engine, "NOR210_KEEP"), engine.edgeTypeCacheGen.current())

	// Variant: a targeted invalidation of the dropped database's type only.
	other := newTestEngine(t)
	seedRelationshipType(t, other, "victim", "NOR210_VICTIM", 2)
	seedRelationshipType(t, other, "peer", "NOR210_KEEP", 2)
	warmRelationshipType(t, NewNamespacedEngine(other, "victim"), "NOR210_VICTIM", 2)
	warmRelationshipType(t, NewNamespacedEngine(other, "peer"), "NOR210_KEEP", 2)
	other.InvalidateEdgeTypeCache("NOR210_VICTIM")
	t.Logf("RCA_TYPECACHE variant InvalidateEdgeTypeCache(NOR210_VICTIM): entries=%d peerIDs=%v victimIDs=%v", relationshipTypeCacheEntryCount(other), cachedRelationshipTypeIDs(other, "NOR210_KEEP"), cachedRelationshipTypeIDs(other, "NOR210_VICTIM"))
}
