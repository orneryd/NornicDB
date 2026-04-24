package knowledgepolicy

import (
	"testing"
	"time"
)

type mockEntityMeta struct {
	labels    []string
	propKeys  []string
	createdAt int64
	versionAt int64
}

func (m *mockEntityMeta) GetEntityMeta(entityID string) ([]string, []string, int64, int64, error) {
	return m.labels, m.propKeys, m.createdAt, m.versionAt, nil
}

type mockAccessMetaStore struct {
	entries map[string]*AccessMetaEntry
}

func (m *mockAccessMetaStore) GetAccessMeta(entityID string) (*AccessMetaEntry, error) {
	return m.entries[entityID], nil
}

func (m *mockAccessMetaStore) PutAccessMeta(entityID string, entry *AccessMetaEntry) error {
	m.entries[entityID] = entry
	return nil
}

func TestFlusherPropertySuppression_WriteNilOnDecay(t *testing.T) {
	acc := NewAccessAccumulator(true)
	store := &mockAccessMetaStore{entries: make(map[string]*AccessMetaEntry)}
	flusher := NewAccessFlusher(acc, store, time.Hour)

	bundle := &DecayProfileBundle{
		Name:                "bio_decay",
		Scope:               ScopeNode,
		Function:            DecayFunctionExponential,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		ScoreFrom:           ScoreFromCreated,
		Enabled:             true,
	}
	binding := &DecayProfileBinding{
		Name:         "bind_bio",
		ProfileRef:   "bio_decay",
		TargetLabels: []string{"Person"},
		PropertyRules: []DecayProfilePropertyRule{
			{PropertyPath: "bio", HalfLifeSeconds: 1800},
		},
	}

	bt, err := BuildBindingTable(
		map[string]*DecayProfileBundle{bundle.Name: bundle},
		map[string]*DecayProfileBinding{binding.Name: binding},
		nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	scorer := NewScorer(NewResolver(bt, nil), true)
	createdAt := time.Now().Add(-720 * time.Hour).UnixNano()
	meta := &mockEntityMeta{
		labels:    []string{"Person"},
		propKeys:  []string{"name", "bio"},
		createdAt: createdAt,
		versionAt: createdAt,
	}

	flusher.SetPropertySuppression(
		func(ns string) *Scorer { return scorer },
		meta,
		nil,
	)

	acc.IncrementAccess("testns:p1")
	flusher.Flush()

	entry := store.entries["testns:p1"]
	if entry == nil {
		t.Fatal("expected entry after flush")
	}

	// "bio" has a 30-min half-life, node is 30 days old — deeply decayed.
	if _, ok := entry.Overflow["_suppress:bio"]; !ok {
		t.Error("expected _suppress:bio marker in overflow")
	}

	// "name" uses parent node rule (1hr half-life), 30 days old — also decayed.
	if _, ok := entry.Overflow["_suppress:name"]; !ok {
		t.Error("expected _suppress:name marker for old node")
	}
}

func TestFlusherPropertySuppression_NoSuppressionWhenRecent(t *testing.T) {
	acc := NewAccessAccumulator(true)
	store := &mockAccessMetaStore{entries: make(map[string]*AccessMetaEntry)}
	flusher := NewAccessFlusher(acc, store, time.Hour)

	bundle := &DecayProfileBundle{
		Name:                "recent_decay",
		Scope:               ScopeNode,
		Function:            DecayFunctionExponential,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		ScoreFrom:           ScoreFromCreated,
		Enabled:             true,
	}
	binding := &DecayProfileBinding{
		Name:         "bind_recent",
		ProfileRef:   "recent_decay",
		TargetLabels: []string{"Person"},
		PropertyRules: []DecayProfilePropertyRule{
			{PropertyPath: "bio", HalfLifeSeconds: 3600},
		},
	}

	bt, err := BuildBindingTable(
		map[string]*DecayProfileBundle{bundle.Name: bundle},
		map[string]*DecayProfileBinding{binding.Name: binding},
		nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	scorer := NewScorer(NewResolver(bt, nil), true)
	createdAt := time.Now().Add(-5 * time.Minute).UnixNano()
	meta := &mockEntityMeta{
		labels:    []string{"Person"},
		propKeys:  []string{"name", "bio"},
		createdAt: createdAt,
		versionAt: createdAt,
	}

	flusher.SetPropertySuppression(
		func(ns string) *Scorer { return scorer },
		meta,
		nil,
	)

	acc.IncrementAccess("testns:p2")
	flusher.Flush()

	entry := store.entries["testns:p2"]
	if entry == nil {
		t.Fatal("expected entry after flush")
	}

	for k := range entry.Overflow {
		if len(k) > 10 && k[:10] == "_suppress:" {
			t.Errorf("unexpected suppression marker: %s", k)
		}
	}
}

func TestFlusherPropertySuppression_EmbedInvalidateOnChange(t *testing.T) {
	acc := NewAccessAccumulator(true)
	store := &mockAccessMetaStore{entries: make(map[string]*AccessMetaEntry)}
	flusher := NewAccessFlusher(acc, store, time.Hour)

	bundle := &DecayProfileBundle{
		Name:                "embed_decay",
		Scope:               ScopeNode,
		Function:            DecayFunctionExponential,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		ScoreFrom:           ScoreFromCreated,
		Enabled:             true,
	}
	binding := &DecayProfileBinding{
		Name:         "bind_embed",
		ProfileRef:   "embed_decay",
		TargetLabels: []string{"Person"},
		PropertyRules: []DecayProfilePropertyRule{
			{PropertyPath: "bio", HalfLifeSeconds: 1800},
		},
	}

	bt, err := BuildBindingTable(
		map[string]*DecayProfileBundle{bundle.Name: bundle},
		map[string]*DecayProfileBinding{binding.Name: binding},
		nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	scorer := NewScorer(NewResolver(bt, nil), true)
	createdAt := time.Now().Add(-720 * time.Hour).UnixNano()
	meta := &mockEntityMeta{
		labels:    []string{"Person"},
		propKeys:  []string{"bio"},
		createdAt: createdAt,
		versionAt: createdAt,
	}

	var invalidated []string
	flusher.SetPropertySuppression(
		func(ns string) *Scorer { return scorer },
		meta,
		func(entityID string) { invalidated = append(invalidated, entityID) },
	)

	acc.IncrementAccess("testns:p3")
	flusher.Flush()

	if len(invalidated) != 1 || invalidated[0] != "testns:p3" {
		t.Errorf("expected embedInvalidate called once for testns:p3, got %v", invalidated)
	}
}

func TestFlusherPropertySuppression_NoCallbackWhenNoScorerFunc(t *testing.T) {
	acc := NewAccessAccumulator(true)
	store := &mockAccessMetaStore{entries: make(map[string]*AccessMetaEntry)}
	flusher := NewAccessFlusher(acc, store, time.Hour)

	acc.IncrementAccess("testns:p4")
	flusher.Flush()

	entry := store.entries["testns:p4"]
	if entry == nil {
		t.Fatal("expected entry after flush")
	}

	for k := range entry.Overflow {
		if len(k) > 10 && k[:10] == "_suppress:" {
			t.Errorf("unexpected suppression marker without scorer: %s", k)
		}
	}
}

func TestFlusherPropertySuppression_RestorationRemovesMarker(t *testing.T) {
	acc := NewAccessAccumulator(true)
	store := &mockAccessMetaStore{entries: make(map[string]*AccessMetaEntry)}
	flusher := NewAccessFlusher(acc, store, time.Hour)

	bundle := &DecayProfileBundle{
		Name:                "restore_decay",
		Scope:               ScopeNode,
		Function:            DecayFunctionExponential,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		ScoreFrom:           ScoreFromCreated,
		Enabled:             true,
	}
	binding := &DecayProfileBinding{
		Name:         "bind_restore",
		ProfileRef:   "restore_decay",
		TargetLabels: []string{"Person"},
		PropertyRules: []DecayProfilePropertyRule{
			{PropertyPath: "bio", HalfLifeSeconds: 3600},
		},
	}

	bt, err := BuildBindingTable(
		map[string]*DecayProfileBundle{bundle.Name: bundle},
		map[string]*DecayProfileBinding{binding.Name: binding},
		nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	scorer := NewScorer(NewResolver(bt, nil), true)
	recentCreatedAt := time.Now().Add(-5 * time.Minute).UnixNano()
	meta := &mockEntityMeta{
		labels:    []string{"Person"},
		propKeys:  []string{"bio"},
		createdAt: recentCreatedAt,
		versionAt: recentCreatedAt,
	}

	var invalidated []string
	flusher.SetPropertySuppression(
		func(ns string) *Scorer { return scorer },
		meta,
		func(entityID string) { invalidated = append(invalidated, entityID) },
	)

	// Pre-seed the entry with a suppression marker.
	store.entries["testns:p5"] = &AccessMetaEntry{
		TargetID:    "testns:p5",
		TargetScope: ScopeNode,
		Overflow:    map[string]interface{}{"_suppress:bio": nil},
	}

	acc.IncrementAccess("testns:p5")
	flusher.Flush()

	entry := store.entries["testns:p5"]
	if _, ok := entry.Overflow["_suppress:bio"]; ok {
		t.Error("expected _suppress:bio marker to be removed after restoration")
	}

	if len(invalidated) != 1 || invalidated[0] != "testns:p5" {
		t.Errorf("expected embedInvalidate on restoration, got %v", invalidated)
	}
}
