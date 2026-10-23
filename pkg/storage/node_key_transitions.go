package storage

import (
	"reflect"

	"github.com/orneryd/nornicdb/pkg/localization"
)

func nodeUniqueCommitLock(id NodeID) uniqueConstraintLockKey {
	return uniqueConstraintLockKey{label: "\x00node-write", value: string(id)}
}

// ObservedNodeUpdater accepts the image the mutation actually read. Query
// execution supplies it at the write boundary; ordinary reads retain no state.
type ObservedNodeUpdater interface {
	UpdateNodeObserved(before, after *Node) error
}

// UpdateObservedNode is the shared mutation boundary. Engines without an
// explicit transaction keep their ordinary atomic UpdateNode contract.
func UpdateObservedNode(engine Engine, before, after *Node) error {
	if writer, ok := engine.(ObservedNodeUpdater); ok {
		return writer.UpdateNodeObserved(before, after)
	}
	return engine.UpdateNode(after)
}

// nodeIntroducesKey compares the write's observed image with its new image.
// Merely carrying a key through a non-key update does not acquire it again.
func nodeIntroducesKey(before, after *Node, c Constraint) bool {
	if after == nil || !hasLabel(after.Labels, c.Label) {
		return false
	}
	for _, property := range c.Properties {
		if after.Properties[property] == nil {
			return false
		}
	}
	if before == nil || !hasLabel(before.Labels, c.Label) {
		return true
	}
	for _, property := range c.Properties {
		if !compareValues(before.Properties[property], after.Properties[property]) {
			return true
		}
	}
	return false
}

// rememberNodeMutationLocked retains only the first write's unique-key image.
// Repeated writes use that image; create and delete/recreate start a new claim.
func (tx *BadgerTransaction) rememberNodeMutationLocked(id NodeID, before *Node, created bool) {
	if created {
		if tx.nodeWriteBefore != nil {
			tx.nodeWriteBefore[id] = nil
		}
		return
	}
	if _, recorded := tx.nodeWriteBefore[id]; recorded && !created {
		return
	}
	if tx.nodeWriteBefore == nil {
		tx.nodeWriteBefore = make(map[NodeID]*Node)
	}
	if before == nil {
		tx.nodeWriteBefore[id] = nil
		return
	}
	image := &Node{ID: id, Labels: append([]string(nil), before.Labels...), Properties: make(map[string]interface{})}
	for _, c := range tx.engine.GetSchemaForNamespace(tx.namespace).GetConstraintsForLabels(before.Labels) {
		if c.Type != ConstraintUnique && c.Type != ConstraintNodeKey {
			continue
		}
		for _, property := range c.Properties {
			image.Properties[property] = clonePropertyValue(before.Properties[property])
		}
	}
	tx.nodeWriteBefore[id] = image
}

func (tx *BadgerTransaction) nodeIntroducesKeyLocked(node *Node, c Constraint) bool {
	return nodeIntroducesKey(tx.nodeWriteBefore[node.ID], node, c)
}

// NodeDeleted reports the transaction's own tombstone for search admission.
func (tx *BadgerTransaction) NodeDeleted(id NodeID) bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	_, deleted := tx.deletedNodes[id]
	return deleted
}

func clonePropertyValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice || v.IsNil() {
		return value
	}
	copy := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
	for i := 0; i < v.Len(); i++ {
		item := clonePropertyValue(v.Index(i).Interface())
		if item != nil {
			copy.Index(i).Set(reflect.ValueOf(item))
		}
	}
	return copy.Interface()
}

// Commit locks cover removed and added keys. Unchanged UNIQUE keys neither
// acquire a reservation lock nor run uniqueness validation.
func (sm *SchemaManager) nodeTransitionLockKeys(before, after *Node) []uniqueConstraintLockKey {
	var keys []uniqueConstraintLockKey
	if sm.HasUniqueValueTracking() {
		if after != nil {
			keys = append(keys, nodeUniqueCommitLock(after.ID))
		} else if before != nil {
			keys = append(keys, nodeUniqueCommitLock(before.ID))
		}
	}
	for _, image := range []*Node{before, after} {
		if image == nil {
			continue
		}
		for _, c := range sm.GetConstraintsForLabels(image.Labels) {
			if c.EffectiveEntityType() != ConstraintEntityNode {
				continue
			}
			if c.Type == ConstraintUnique && !nodeIntroducesKey(before, after, c) && !nodeIntroducesKey(after, before, c) {
				continue
			}
			if key, ok := nodeConstraintLockKey(c, image); ok {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// Validate the key image before publishing a stale full node body. This is a
// write conflict check, not uniqueness validation: an unchanged key acquires
// no reservation, even when a third writer has reused its old value. The node
// commit lock spans this read and publication, including buffered ACK.
func (tx *BadgerTransaction) validateNodeKeyImagesLocked() error {
	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	for id, before := range tx.nodeWriteBefore {
		pending := tx.pendingNodes[id]
		if before == nil || pending == nil {
			continue
		}
		// The commit's current image must come from the merged stored view.
		// A read cache can lag a replay's physical body; it is not a claim.
		current, err := tx.engine.getQueryNode(id, storedRead, nodeReadCacheSnapshot)
		if err != nil && err != ErrNotFound {
			return &CommitFailureError{Cause: err}
		}
		changed := err == ErrNotFound
		if !changed {
			labels := append(append([]string(nil), before.Labels...), current.Labels...)
			for _, c := range schema.GetConstraintsForLabels(labels) {
				if c.Type != ConstraintUnique {
					continue
				}
				if nodeIntroducesKey(before, current, c) || nodeIntroducesKey(current, before, c) {
					changed = true
					break
				}
			}
		}
		if changed {
			if err := tx.classifyUpdateConflictAsUniqueViolation(id, true); err != nil {
				return err
			}
			return localizedError(localization.StorageTransactionNodeChanged(string(id)), ErrConflict)
		}
	}
	return nil
}
