package knowledgepolicy

import (
	"testing"
	"time"
)

type mockEntityMeta struct {
	scope     ScopeType
	labels    []string
	edgeType  string
	propKeys  []string
	createdAt int64
	versionAt int64
}

func (m *mockEntityMeta) GetEntityMeta(entityID string) (EntityMeta, error) {
	scope := m.scope
	if scope == "" {
		scope = ScopeNode
	}
	return EntityMeta{
		Scope:          scope,
		Labels:         m.labels,
		EdgeType:       m.edgeType,
		PropertyKeys:   m.propKeys,
		CreatedAtNanos: m.createdAt,
		VersionAtNanos: m.versionAt,
	}, nil
}

// suppressionCase is one entity configuration for the property-suppression
// contract: which binding applies, how old the entity is, which properties it
// has, and how the persisted overflow/callback state must look afterwards.
type suppressionCase struct {
	// name is the subtest name, matching the original per-test suffix.
	name string
	// suffix is the fixture-local entity id the case flushes; the persisted id
	// is "testns:"+suffix, as the original per-case tests asserted.
	suffix string
	bundle *DecayProfileBundle
	bind   *DecayProfileBinding
	meta   mockEntityMeta
	// installSuppression is false for the case that exercises a flusher with no
	// property-suppression wiring at all.
	installSuppression bool
	// preSeed, when non-nil, installs a stored entry before the flush.
	preSeed func(entityID string) *AccessMetaEntry
	// verify asserts the persisted marker/callback state after the flush.
	verify func(t *testing.T, entry *AccessMetaEntry, f *accessFlusherFixture)
}

// bioMarker asserts that "bio" carries a present, nil-valued suppression
// marker. The nil value is the persisted suppression encoding, so the check
// covers both the key and its representation.
func bioMarker(t *testing.T, entry *AccessMetaEntry, context string) {
	t.Helper()

	value, ok := suppressionMarker(entry, "bio")
	if !ok {
		t.Errorf("expected _suppress:bio marker %s", context)
		return
	}
	if value != nil {
		t.Errorf("expected _suppress:bio marker to be nil-valued %s, got %#v", context, value)
	}
}

// TestFlusherPropertySuppression covers the property-level suppression contract
// of the access flusher: decayed properties get a nil-valued "_suppress:<prop>"
// marker, recent properties stay unmarked, a change in suppression state
// invokes embedInvalidate exactly once for the entity, and restoring a property
// removes its marker.
//
// Every case shares the scoring fixture (accumulator/store/flusher/binding
// table/entity meta/callback recorder) from accessFlusherFixture and differs
// only in its decay binding, entity age, property keys and expected marker
// state.
func TestFlusherPropertySuppression(t *testing.T) {
	// 30 days is past every half-life below; 5 minutes is within every one.
	oldEntity := time.Now().Add(-720 * time.Hour).UnixNano()
	recentEntity := time.Now().Add(-5 * time.Minute).UnixNano()

	// personNodeDecay builds the node-scope exponential decay bundle/binding
	// pair the cases share; profile name, binding name and property rules are
	// the only per-case inputs.
	personNodeDecay := func(profile, bindingName string, rules ...DecayProfilePropertyRule) (*DecayProfileBundle, *DecayProfileBinding) {
		bundle := &DecayProfileBundle{
			Name:                profile,
			Scope:               ScopeNode,
			Function:            DecayFunctionExponential,
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.10,
			ScoreFrom:           ScoreFromCreated,
			Enabled:             true,
			DecayEnabled:        true,
		}
		binding := &DecayProfileBinding{
			Name:          bindingName,
			ProfileRef:    profile,
			TargetLabels:  []string{"Person"},
			PropertyRules: rules,
		}
		return bundle, binding
	}
	bioHalfLife := func(seconds int64) []DecayProfilePropertyRule {
		return []DecayProfilePropertyRule{{PropertyPath: "bio", HalfLifeSeconds: seconds}}
	}

	bioDecayBundle, bioDecayBinding := personNodeDecay("bio_decay", "bind_bio", bioHalfLife(1800)...)
	recentBundle, recentBinding := personNodeDecay("recent_decay", "bind_recent", bioHalfLife(3600)...)
	embedBundle, embedBinding := personNodeDecay("embed_decay", "bind_embed", bioHalfLife(1800)...)
	restoreBundle, restoreBinding := personNodeDecay("restore_decay", "bind_restore", bioHalfLife(3600)...)

	cases := []suppressionCase{
		{
			// "bio" has a 30-min half-life and "name" falls back to the 1hr
			// parent node rule; on a 30-day-old node both are deeply decayed.
			name:               "WriteNilOnDecay",
			suffix:             "p1",
			bundle:             bioDecayBundle,
			bind:               bioDecayBinding,
			meta:               mockEntityMeta{labels: []string{"Person"}, propKeys: []string{"name", "bio"}, createdAt: oldEntity, versionAt: oldEntity},
			installSuppression: true,
			verify: func(t *testing.T, entry *AccessMetaEntry, f *accessFlusherFixture) {
				t.Helper()

				bioMarker(t, entry, "in overflow")

				value, ok := suppressionMarker(entry, "name")
				if !ok {
					t.Error("expected _suppress:name marker for old node")
				} else if value != nil {
					t.Errorf("expected _suppress:name marker to be nil-valued, got %#v", value)
				}
			},
		},
		{
			// A 5-minute-old node is inside every half-life above, so neither
			// property may be suppressed.
			name:               "NoSuppressionWhenRecent",
			suffix:             "p2",
			bundle:             recentBundle,
			bind:               recentBinding,
			meta:               mockEntityMeta{labels: []string{"Person"}, propKeys: []string{"name", "bio"}, createdAt: recentEntity, versionAt: recentEntity},
			installSuppression: true,
			verify: func(t *testing.T, entry *AccessMetaEntry, f *accessFlusherFixture) {
				t.Helper()

				if keys := suppressionMarkerKeys(entry); len(keys) != 0 {
					t.Errorf("unexpected suppression marker(s): %v", keys)
				}
			},
		},
		{
			// Newly suppressing "bio" is a suppression-state change, so the
			// re-embedding callback fires exactly once for this entity.
			name:               "EmbedInvalidateOnChange",
			suffix:             "p3",
			bundle:             embedBundle,
			bind:               embedBinding,
			meta:               mockEntityMeta{labels: []string{"Person"}, propKeys: []string{"bio"}, createdAt: oldEntity, versionAt: oldEntity},
			installSuppression: true,
			verify: func(t *testing.T, entry *AccessMetaEntry, f *accessFlusherFixture) {
				t.Helper()

				bioMarker(t, entry, "as the suppression change that triggers invalidation")
				want := f.entityID("p3")
				if len(f.invalidated) != 1 || f.invalidated[0] != want {
					t.Errorf("expected embedInvalidate called once for %s, got %v", want, f.invalidated)
				}
			},
		},
		{
			// Retained from the original family: with no scorer function wired,
			// the flusher still persists access metadata but records neither
			// suppression markers nor invalidation callbacks.
			name:   "NoCallbackWhenNoScorerFunc",
			suffix: "p4",
			bundle: &DecayProfileBundle{Name: "unused_decay", Scope: ScopeNode, Enabled: true},
			bind: &DecayProfileBinding{
				Name:         "bind_unused",
				ProfileRef:   "unused_decay",
				TargetLabels: []string{"Person"},
			},
			installSuppression: false,
			verify: func(t *testing.T, entry *AccessMetaEntry, f *accessFlusherFixture) {
				t.Helper()

				if keys := suppressionMarkerKeys(entry); len(keys) != 0 {
					t.Errorf("unexpected suppression marker without scorer: %v", keys)
				}
				if len(f.invalidated) != 0 {
					t.Errorf("expected no embedInvalidate calls without scorer, got %v", f.invalidated)
				}
			},
		},
		{
			// The stored entry arrives already suppressed; on a recent entity the
			// property is no longer eligible, so the marker must be removed and
			// the restoration must invalidate the embedding once.
			name:               "RestorationRemovesMarker",
			suffix:             "p5",
			bundle:             restoreBundle,
			bind:               restoreBinding,
			meta:               mockEntityMeta{labels: []string{"Person"}, propKeys: []string{"bio"}, createdAt: recentEntity, versionAt: recentEntity},
			installSuppression: true,
			preSeed: func(entityID string) *AccessMetaEntry {
				return &AccessMetaEntry{
					TargetID:    entityID,
					TargetScope: ScopeNode,
					Overflow:    map[string]interface{}{"_suppress:bio": nil},
				}
			},
			verify: func(t *testing.T, entry *AccessMetaEntry, f *accessFlusherFixture) {
				t.Helper()

				if _, ok := suppressionMarker(entry, "bio"); ok {
					t.Error("expected _suppress:bio marker to be removed after restoration")
				}
				want := f.entityID("p5")
				if len(f.invalidated) != 1 || f.invalidated[0] != want {
					t.Errorf("expected embedInvalidate on restoration for %s, got %v", want, f.invalidated)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f := newAccessFlusherFixture(t, tc.bundle, tc.bind, nil, &tc.meta)
			if tc.preSeed != nil {
				f.store.entries[f.entityID(tc.suffix)] = tc.preSeed(f.entityID(tc.suffix))
			}
			if tc.installSuppression {
				f.install()
			}

			f.flushAccesses(tc.suffix)
			tc.verify(t, f.entryFor(t, tc.suffix), f)
		})
	}
}
