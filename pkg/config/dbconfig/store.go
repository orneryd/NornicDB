// Package dbconfig provides per-database configuration override storage in the system database.
//
// Overrides are stored as _DbConfig nodes (one per database). Global config remains the default;
// per-DB overrides are merged at resolution time. See the per-database configuration overrides plan.
package dbconfig

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	dbConfigLabel   = "_DbConfig"
	dbConfigPrefix  = "db_config:"
	dbConfigSystems = "_System"
)

// Store loads and saves per-database config overrides in the system database.
// Same pattern as auth.AllowlistStore and auth.PrivilegesStore.
type Store struct {
	storage storage.Engine
	mu      sync.RWMutex
	// dbName -> overrides (key = env-style name, value = string)
	overrides map[string]map[string]string
}

// NewStore creates a store that reads/writes per-DB overrides to the given system storage.
func NewStore(systemStorage storage.Engine) *Store {
	return &Store{storage: systemStorage, overrides: make(map[string]map[string]string)}
}

// LoadWithYAMLDefaults reads existing _DbConfig nodes from storage and then
// merges yaml-declared per-DB overrides on top of those, but only for
// (dbName, key) pairs that DON'T already have a stored value. This makes
// yaml a one-time seed: an admin who PUTs a value via /admin/databases/{name}/config
// is authoritative across restarts and won't be silently overwritten by
// stale yaml on the next boot.
//
// Pass nil/empty yamlOverrides to behave identically to plain Load().
//
// The yaml-derived values are persisted into the system database the same
// way SetOverrides persists admin-API edits, so subsequent restarts find
// them in the store and skip the seed step.
func (s *Store) LoadWithYAMLDefaults(ctx context.Context, yamlOverrides map[string]map[string]string) error {
	if err := s.Load(ctx); err != nil {
		return err
	}
	if len(yamlOverrides) == 0 {
		return nil
	}
	// For each yaml-declared (dbName, key, value) tuple, persist it iff
	// there is no existing value for that key in the store.
	for dbName, kv := range yamlOverrides {
		if dbName == "" || len(kv) == 0 {
			continue
		}
		s.mu.RLock()
		existing := s.overrides[dbName]
		s.mu.RUnlock()
		merged := make(map[string]string, len(kv))
		for k, v := range existing {
			merged[k] = v
		}
		changed := false
		for k, v := range kv {
			if !IsAllowedKey(k) {
				continue
			}
			if _, set := existing[k]; set {
				continue
			}
			merged[k] = v
			changed = true
		}
		if !changed {
			continue
		}
		if err := s.SetOverrides(ctx, dbName, merged); err != nil {
			return err
		}
	}
	return nil
}

// Load reads all _DbConfig nodes from storage into memory. Call at startup and after PUT.
func (s *Store) Load(ctx context.Context) error {
	m := make(map[string]map[string]string)
	err := storage.StreamNodesWithFallback(ctx, s.storage, 1000, func(n *storage.Node) error {
		hasLabel := false
		for _, l := range n.Labels {
			if l == dbConfigLabel {
				hasLabel = true
				break
			}
		}
		if !hasLabel {
			return nil
		}
		dbName := dbNameFromNodeID(string(n.ID))
		if dbName == "" {
			return nil
		}
		overrides := overridesFromProperties(n.Properties)
		m[dbName] = overrides
		return nil
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.overrides = m
	s.mu.Unlock()
	return nil
}

// GetOverrides returns a copy of the overrides for the given database. Nil or empty = no overrides.
func (s *Store) GetOverrides(dbName string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o := s.overrides[dbName]
	if o == nil {
		return nil
	}
	out := make(map[string]string, len(o))
	for k, v := range o {
		out[k] = v
	}
	return out
}

// SetOverrides persists overrides for the given database and refreshes in-memory cache.
// Pass nil or empty map to clear all overrides for that database.
func (s *Store) SetOverrides(ctx context.Context, dbName string, overrides map[string]string) error {
	dbName = strings.TrimSpace(dbName)
	if dbName == "" {
		return nil
	}
	nodeID := storage.NodeID(dbConfigPrefix + dbName)
	if len(overrides) == 0 {
		if err := s.storage.DeleteNode(nodeID); err != nil && err != storage.ErrNotFound {
			return err
		}
		s.mu.Lock()
		delete(s.overrides, dbName)
		s.mu.Unlock()
		return nil
	}
	overridesJSON, err := json.Marshal(overrides)
	if err != nil {
		return err
	}
	node := &storage.Node{
		ID:     nodeID,
		Labels: []string{dbConfigLabel, dbConfigSystems},
		Properties: map[string]any{
			"database":  dbName,
			"overrides": string(overridesJSON),
		},
	}
	existing, err := s.storage.GetNode(nodeID)
	if err == storage.ErrNotFound {
		_, err = s.storage.CreateNode(node)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		node.CreatedAt = existing.CreatedAt
		err = s.storage.UpdateNode(node)
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	if s.overrides == nil {
		s.overrides = make(map[string]map[string]string)
	}
	s.overrides[dbName] = overrides
	s.mu.Unlock()
	return nil
}

func dbNameFromNodeID(id string) string {
	if strings.HasPrefix(id, dbConfigPrefix) {
		return id[len(dbConfigPrefix):]
	}
	return ""
}

func overridesFromProperties(p map[string]any) map[string]string {
	if p == nil {
		return nil
	}
	s, ok := p["overrides"].(string)
	if !ok {
		return nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	return out
}
