package knowledgepolicy

import (
	"testing"
	"time"
)

// accessFlusherFixture is the shared setup for the access-flusher tests that
// exercise ON ACCESS mutation application and property-level suppression.
// It owns the accumulator/store/flusher triple, the compiled binding table
// (scorer + resolver), the entity metadata fake and the embed-invalidation
// recorder, so those tests assert policy behaviour instead of restating
// plumbing.
//
// Each fixture owns its scorer and store, retaining the original test identifiers.
type accessFlusherFixture struct {
	namespace string

	store   *mockStore
	acc     *AccessAccumulator
	flusher *AccessFlusher
	meta    *mockEntityMeta
	scorer  *Scorer

	// invalidated records embedInvalidate callback invocations in call order.
	invalidated []string
}

// newAccessFlusherFixture compiles a single node/edge binding plus an optional
// ON ACCESS promotion policy, and returns a fixture whose flusher has no
// property-suppression wiring yet. meta carries the labels/edge type and
// timestamps of the entity under test.
func newAccessFlusherFixture(
	t *testing.T,
	bundle *DecayProfileBundle,
	binding *DecayProfileBinding,
	policy *PromotionPolicyDef,
	meta *mockEntityMeta,
) *accessFlusherFixture {
	t.Helper()

	policies := map[string]*PromotionPolicyDef{}
	if policy != nil {
		policies[policy.Name] = policy
	}
	bt, err := BuildBindingTable(
		map[string]*DecayProfileBundle{bundle.Name: bundle},
		map[string]*DecayProfileBinding{binding.Name: binding},
		nil,
		policies,
	)
	if err != nil {
		t.Fatalf("BuildBindingTable: %v", err)
	}

	f := &accessFlusherFixture{
		namespace: "testns",
		store:     newMockStore(),
		acc:       NewAccessAccumulator(true, 0),
		meta:      meta,
		scorer:    NewScorer(NewResolver(bt, nil), true),
	}
	f.flusher = NewAccessFlusher(f.acc, f.store, time.Hour)
	return f
}

// install wires property suppression and ON ACCESS evaluation into the flusher,
// including the embed-invalidation recorder. Tests that must observe a flusher
// with no property suppression configured simply do not call install.
func (f *accessFlusherFixture) install() {
	f.flusher.SetPropertySuppression(
		func(ns string) *Scorer { return f.scorer },
		f.meta,
		func(entityID string) { f.invalidated = append(f.invalidated, entityID) },
	)
}

// entityID returns the namespace-qualified identifier for a fixture-local
// entity suffix (for example "episode-1").
func (f *accessFlusherFixture) entityID(suffix string) string {
	return f.namespace + ":" + suffix
}

// entryFor returns the persisted access meta entry for a fixture-local entity
// suffix, failing the test when the flush did not persist one.
func (f *accessFlusherFixture) entryFor(t *testing.T, suffix string) *AccessMetaEntry {
	t.Helper()
	entry := f.store.get(f.entityID(suffix))
	if entry == nil {
		t.Fatalf("expected persisted access meta entry for %s", f.entityID(suffix))
	}
	return entry
}

// flushAccesses increments the access counter for a fixture-local entity suffix
// and performs one flush.
func (f *accessFlusherFixture) flushAccesses(suffix string) {
	f.acc.IncrementAccess(f.entityID(suffix))
	f.flusher.Flush()
}

// suppressionMarker reports the overflow value and map presence of the
// persisted suppression marker for one property. Presence is reported
// separately from the value because suppression is stored as a nil-valued key.
func suppressionMarker(entry *AccessMetaEntry, property string) (value interface{}, present bool) {
	value, present = entry.Overflow["_suppress:"+property]
	return value, present
}

// suppressionMarkerKeys lists every "_suppress:" marker key currently recorded
// on the entry.
func suppressionMarkerKeys(entry *AccessMetaEntry) []string {
	const prefix = "_suppress:"
	keys := make([]string, 0, len(entry.Overflow))
	for k := range entry.Overflow {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys
}
