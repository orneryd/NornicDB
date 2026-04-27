package storage

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// loadPersistedSchemas loads all persisted schema definitions from Badger into memory.
//
// This is run during engine startup so:
//   - schema load errors fail fast (instead of silently disabling constraints)
//   - NamespacedEngine.GetSchema() can be error-free while still returning the correct schema
func (b *BadgerEngine) loadPersistedSchemas() error {
	if err := b.ensureOpen(); err != nil {
		return err
	}

	type loaded struct {
		namespace string
		schema    *SchemaManager
	}
	var loadedSchemas []loaded

	// Phase 1: read + decode schema definitions.
	if err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixSchema}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			namespace, ok := parseSchemaNamespaceFromKey(key)
			if !ok || namespace == "" {
				return fmt.Errorf("schema: invalid schema key: %x", key)
			}

			var data []byte
			if err := item.Value(func(val []byte) error {
				data = append([]byte(nil), val...)
				return nil
			}); err != nil {
				return fmt.Errorf("schema: read %q: %w", namespace, err)
			}

			var def SchemaDefinition
			if err := json.Unmarshal(data, &def); err != nil {
				return fmt.Errorf("schema: decode %q: %w", namespace, err)
			}
			if def.Version == 0 {
				def.Version = schemaDefinitionVersion
			}

			sm := NewSchemaManager()
			if err := sm.ReplaceFromDefinition(&def); err != nil {
				return fmt.Errorf("schema: apply %q: %w", namespace, err)
			}
			sm.SetPersister(func(def *SchemaDefinition) error {
				return b.persistSchemaDefinition(namespace, def)
			})
			sm.SetKnowledgePolicyChangedHook(func() {
				_ = b.ReconcileDecaySuppression(namespace)
			})

			loadedSchemas = append(loadedSchemas, loaded{namespace: namespace, schema: sm})
		}
		return nil
	}); err != nil {
		return err
	}

	// Phase 2: install into engine map.
	if len(loadedSchemas) > 0 {
		b.schemasMu.Lock()
		for _, ls := range loadedSchemas {
			b.schemas[ls.namespace] = ls.schema
		}
		b.schemasMu.Unlock()
	}

	// Phase 3: rebuild derived unique-constraint value caches from stored nodes.
	// This keeps CreateNode() fast (in-memory uniqueness checks) and ensures constraints
	// enforce correctly immediately after restart.
	for _, ls := range loadedSchemas {
		if err := b.rebuildUniqueConstraintValues(ls.namespace, ls.schema); err != nil {
			return err
		}
	}

	return nil
}

func parseSchemaNamespaceFromKey(key []byte) (string, bool) {
	if len(key) < 2 || key[0] != prefixSchema {
		return "", false
	}
	// Format: prefixSchema + namespace + 0x00
	for i := 1; i < len(key); i++ {
		if key[i] == 0x00 {
			if i == 1 {
				return "", false
			}
			return string(key[1:i]), true
		}
	}
	return "", false
}

func (b *BadgerEngine) persistSchemaDefinition(namespace string, def *SchemaDefinition) error {
	if namespace == "" {
		return fmt.Errorf("schema: namespace is required")
	}
	if def == nil {
		return fmt.Errorf("schema: definition is required")
	}

	// Normalize version.
	if def.Version == 0 {
		def.Version = schemaDefinitionVersion
	}

	blob, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("schema: marshal %q: %w", namespace, err)
	}

	return b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(schemaKey(namespace), blob)
	})
}

func (b *BadgerEngine) rebuildUniqueConstraintValues(namespace string, sm *SchemaManager) error {
	if namespace == "" || sm == nil {
		return nil
	}

	// Fast skip: if there are no derived schema caches, there's nothing to rebuild.
	sm.mu.RLock()
	hasUnique := len(sm.uniqueConstraints) > 0
	hasPropertyIndexes := len(sm.propertyIndexes) > 0
	hasCompositeIndexes := len(sm.compositeIndexes) > 0
	uniqueConstraints := make([]*UniqueConstraint, 0, len(sm.uniqueConstraints))
	for _, uc := range sm.uniqueConstraints {
		uniqueConstraints = append(uniqueConstraints, uc)
	}
	propertyIndexes := make([]*PropertyIndex, 0, len(sm.propertyIndexes))
	for _, idx := range sm.propertyIndexes {
		propertyIndexes = append(propertyIndexes, idx)
	}
	compositeIndexes := make([]*CompositeIndex, 0, len(sm.compositeIndexes))
	for _, idx := range sm.compositeIndexes {
		compositeIndexes = append(compositeIndexes, idx)
	}
	sm.mu.RUnlock()
	if !hasUnique && !hasPropertyIndexes && !hasCompositeIndexes {
		return nil
	}

	// Reset all in-memory derived caches first.
	for _, uc := range uniqueConstraints {
		uc.mu.Lock()
		uc.values = make(map[interface{}]NodeID)
		uc.valuesAuthoritative = false
		uc.mu.Unlock()
	}
	for _, idx := range propertyIndexes {
		idx.mu.Lock()
		idx.values = make(map[interface{}][]NodeID)
		idx.sortedNonNilKeys = nil
		idx.keysDirty = true
		idx.mu.Unlock()
	}
	for _, idx := range compositeIndexes {
		idx.mu.Lock()
		idx.fullIndex = make(map[string][]NodeID)
		idx.prefixIndex = make(map[string][]NodeID)
		idx.mu.Unlock()
	}

	prefix := make([]byte, 0, 1+len(namespace)+1)
	prefix = append(prefix, prefixNode)
	prefix = append(prefix, []byte(namespace)...)
	prefix = append(prefix, ':')

	if err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var raw []byte
			if err := item.Value(func(val []byte) error {
				raw = append([]byte(nil), val...)
				return nil
			}); err != nil {
				return err
			}

			node, err := decodeNode(raw)
			if err != nil {
				return fmt.Errorf("schema: rebuild unique values: decode node: %w", err)
			}

			if hasUnique {
				for _, label := range node.Labels {
					for propName, propValue := range node.Properties {
						if err := sm.CheckUniqueConstraint(label, propName, propValue, node.ID); err != nil {
							return fmt.Errorf("schema: rebuild unique values: namespace=%q: %w", namespace, err)
						}
						sm.RegisterUniqueValue(label, propName, propValue, node.ID)
					}
				}
			}

			if hasPropertyIndexes {
				for _, label := range node.Labels {
					for propName, propValue := range node.Properties {
						if _, ok := sm.GetPropertyIndex(label, propName); !ok {
							continue
						}
						if err := sm.PropertyIndexInsert(label, propName, node.ID, propValue); err != nil {
							return fmt.Errorf("schema: rebuild property indexes: namespace=%q label=%q property=%q: %w", namespace, label, propName, err)
						}
					}
				}
			}

			if hasCompositeIndexes {
				for _, label := range node.Labels {
					for _, idx := range sm.GetCompositeIndexesForLabel(label) {
						if idx == nil {
							continue
						}
						if err := idx.IndexNode(node.ID, node.Properties); err != nil {
							return fmt.Errorf("schema: rebuild composite indexes: namespace=%q index=%q: %w", namespace, idx.Name, err)
						}
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	for _, uc := range uniqueConstraints {
		uc.mu.Lock()
		uc.valuesAuthoritative = true
		uc.mu.Unlock()
	}

	return nil
}

// GetSchemaForNamespace returns the schema for a specific database namespace.
// If no schema exists yet, an empty schema is created (and will be persisted once mutated).
func (b *BadgerEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	if namespace == "" {
		namespace = "nornic"
	}

	b.schemasMu.RLock()
	if sm := b.schemas[namespace]; sm != nil {
		b.schemasMu.RUnlock()
		return sm
	}
	b.schemasMu.RUnlock()

	// Lazily create empty schema for new namespaces.
	sm := NewSchemaManager()
	sm.SetPersister(func(def *SchemaDefinition) error {
		return b.persistSchemaDefinition(namespace, def)
	})
	sm.SetKnowledgePolicyChangedHook(func() {
		_ = b.ReconcileDecaySuppression(namespace)
	})

	b.schemasMu.Lock()
	if existing := b.schemas[namespace]; existing != nil {
		b.schemasMu.Unlock()
		return existing
	}
	b.schemas[namespace] = sm
	b.schemasMu.Unlock()

	return sm
}
