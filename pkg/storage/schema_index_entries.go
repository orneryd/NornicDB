package storage

import "context"

// Equality-index entry maintenance: the one owner of how a node is moved
// between the entries of a CompositeIndex.
//
// Every node write moves the node from its old entries to its new ones with
// ReplaceNode, under one hold of idx.mu. A reader therefore sees either the
// old entries or the new ones, never neither: removing and re-inserting in two
// lock sections let a concurrent seek miss a node whose key never changed.
// An entry whose key is unchanged is never removed.
//
// IndexNode and RemoveNode remain the one-sided entry points for maintenance
// that has only one side (filing a value an index build or backfill carries
// directly, and removing an entry with no replacement image); a write that has
// both images uses ReplaceNode so the shared keys stay in place.

// ReplaceNode moves a node from the entries of oldProperties to those of
// newProperties atomically for readers. Nil properties file nothing: pass nil
// old properties for a node the index did not cover, and nil new properties
// for one it no longer covers. Entries both sides share stay in place; the new
// entries are always (re)filed, so a node missing from the index is repaired.
//
// ctx is the lifetime of the write that moved the node: idx.mu admission and
// every canonical key of both sides are prepared before the first posting
// changes, so a cancelled or failed preparation moves nothing and leaves the
// values, full and prefix populations, dirty flag and fill state untouched.
func (idx *CompositeIndex) ReplaceNode(ctx context.Context, nodeID NodeID, oldProperties, newProperties map[string]interface{}) error {
	if err := idx.mu.LockContext(ctx); err != nil {
		return err
	}
	defer idx.mu.Unlock()
	var oldKeys, newKeys preparedCompositeNodeKeys
	if err := idx.prepareNodeKeys(ctx, oldProperties, &oldKeys); err != nil {
		return err
	}
	if err := idx.prepareNodeKeys(ctx, newProperties, &newKeys); err != nil {
		return err
	}

	if len(idx.Properties) == 1 {
		if oldKeys.hasValue && (!newKeys.hasValue || oldKeys.value != newKeys.value) {
			idx.unfileValueLocked(nodeID, oldKeys.value)
		}
		if newKeys.hasValue {
			idx.fileValueLocked(nodeID, newKeys.value)
		}
		return nil
	}

	// Prefixes the two sides share are the same entries on both sides, and so
	// is the full key of two complete key sets whose every value matches.
	shared := 0
	for shared < oldKeys.prefixCount && shared < newKeys.prefixCount &&
		preparedPrefixHash(&oldKeys, shared) == preparedPrefixHash(&newKeys, shared) {
		shared++
	}
	idx.removeNodeKeysLocked(nodeID, &oldKeys, shared)
	idx.applyNodeKeysLocked(nodeID, &newKeys)
	return nil
}

// preparedPrefixHash is the i-th prepared leading-prefix key hash: the fixed
// slots first, then the overflow of an index with more properties than them.
func preparedPrefixHash(keys *preparedCompositeNodeKeys, i int) string {
	if i < len(keys.prefixHashes) {
		return keys.prefixHashes[i]
	}
	return keys.prefixOverflow[i-len(keys.prefixHashes)]
}

// fileValueLocked files nodeID under an arity-1 key. The caller holds idx.mu.
func (idx *CompositeIndex) fileValueLocked(nodeID NodeID, valueKey interface{}) {
	if idx.values == nil {
		idx.values = make(map[interface{}][]NodeID)
	}
	if _, exists := idx.values[valueKey]; !exists {
		idx.keysDirty = true
	}
	idx.values[valueKey] = appendUnique(idx.values[valueKey], nodeID)
}

// unfileValueLocked removes nodeID from an arity-1 key. The caller holds idx.mu.
func (idx *CompositeIndex) unfileValueLocked(nodeID NodeID, valueKey interface{}) {
	ids, exists := idx.values[valueKey]
	if !exists {
		return
	}
	if ids = removeNodeID(ids, nodeID); len(ids) > 0 {
		idx.values[valueKey] = ids
		return
	}
	delete(idx.values, valueKey)
	idx.keysDirty = true
}
