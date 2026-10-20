package multidb

import (
	"encoding/json"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A preparation is private intent and a publication receipt, never a visible
// database or a second commit decision. The storage lifecycle owns its outcome.
type catalogPreparation struct {
	mutation  storage.NamespaceCatalogMutation
	operation *storage.NamespaceDrop
	done      chan struct{}
	once      sync.Once
}

func (m *DatabaseManager) finishCatalogPreparationLocked(name string, p *catalogPreparation) {
	if m.catalogPreparations[name] == p {
		delete(m.catalogPreparations, name)
	}
	p.once.Do(func() { close(p.done) })
}

func (m *DatabaseManager) finishCatalogPreparation(name string, p *catalogPreparation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finishCatalogPreparationLocked(name, p)
}

func (m *DatabaseManager) pendingDatabaseCreatesLocked() int {
	count := 0
	for _, p := range m.catalogPreparations {
		if p.mutation.Create {
			count++
		}
	}
	return count
}

func (m *DatabaseManager) catalogAliasReservedLocked(alias string) bool {
	for _, p := range m.catalogPreparations {
		var info DatabaseInfo
		if json.Unmarshal(p.mutation.Value, &info) == nil {
			for _, pending := range info.Aliases {
				if pending == alias {
					return true
				}
			}
		}
	}
	return false
}
