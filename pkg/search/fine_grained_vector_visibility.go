package search

import (
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	// A property vector query can name a 100k-node index. Retaining its
	// admission decisions avoids rescanning every projected node for each
	// different query vector, while the smaller policy cap bounds RAM.
	vectorVisibilityMaxPolicies = 4
	vectorVisibilityMaxKeys     = 120000
)

type vectorVisibilityKey struct {
	graphVersion uint64
	fingerprint  string
	accessHash   [32]byte
	database     string
}

// vectorVisibilityCache holds only per-entry admission decisions from the
// projected graph. A key binds those decisions to a graph and policy revision.
// Missing revision support disables reuse, so source changes cannot keep a
// stale decision alive.
type vectorVisibilityCache struct {
	mu      sync.Mutex
	entries map[vectorVisibilityKey]*vectorVisibilityView
	order   []vectorVisibilityKey
}

type vectorVisibilityView struct {
	mu        sync.RWMutex
	decisions map[vectorDecisionKey]bool
}

type vectorDecisionKey struct{ id, matchID string }

func decisionKey(candidate indexResult) vectorDecisionKey {
	return vectorDecisionKey{id: candidate.ID, matchID: candidate.MatchID}
}

func (s *Service) vectorVisibilityForScope(scope *indexedSearchScope) (*vectorVisibilityView, error) {
	if scope.versions == nil || scope.principal.Fingerprint == "" || !searchVisibilityCacheSafe(s.engine) {
		return nil, nil
	}
	database := storage.DatabaseOf(s.engine)
	if database == "" {
		database = s.continuationDatabase()
	}
	id := scope.principal.DatabaseID(database)
	if id == "" {
		return nil, nil
	}
	hash, err := scope.principal.AccessHash()
	if err != nil {
		return nil, err
	}
	key := vectorVisibilityKey{graphVersion: scope.version, fingerprint: scope.principal.Fingerprint, accessHash: hash, database: id}
	cache := &s.vectorVisibility
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.entries == nil {
		cache.entries = make(map[vectorVisibilityKey]*vectorVisibilityView)
	}
	if view := cache.entries[key]; view != nil {
		for i, old := range cache.order {
			if old == key {
				cache.order = append(cache.order[:i], cache.order[i+1:]...)
				break
			}
		}
		cache.order = append(cache.order, key)
		return view, nil
	}
	if len(cache.order) == vectorVisibilityMaxPolicies {
		delete(cache.entries, cache.order[0])
		cache.order = cache.order[1:]
	}
	view := &vectorVisibilityView{decisions: make(map[vectorDecisionKey]bool)}
	cache.entries[key] = view
	cache.order = append(cache.order, key)
	return view, nil
}

func (v *vectorVisibilityView) lookup(candidates []indexResult) ([]indexResult, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	for _, candidate := range candidates {
		if _, known := v.decisions[decisionKey(candidate)]; !known {
			return nil, false
		}
	}
	admitted := candidates[:0]
	for _, candidate := range candidates {
		if v.decisions[decisionKey(candidate)] {
			admitted = append(admitted, candidate)
		}
	}
	return admitted, true
}

func (v *vectorVisibilityView) remember(keys []vectorDecisionKey, admitted []indexResult) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(keys) > vectorVisibilityMaxKeys {
		return
	}
	if len(v.decisions)+len(keys) > vectorVisibilityMaxKeys {
		clear(v.decisions)
	}
	for _, key := range keys {
		v.decisions[key] = false
	}
	for _, candidate := range admitted {
		v.decisions[decisionKey(candidate)] = true
	}
}
