package storage

import "context"

// IndexNode files a node in the index. An arity-1 index files the node under
// its canonical single-property key (values, the encoding ORDER BY / range /
// equality seeks read); an arity >= 2 index files it under its full and every
// leading-prefix composite key. Call this when creating or updating a node
// with the indexed properties.
//
// ctx is the lifetime of the caller filing the node: index admission, every
// canonical key and the acceptance of the prepared keys observe it. Every key
// is staged before the first posting is written, so a cancelled or failed
// preparation files nothing and leaves the values, full and prefix
// populations, dirty flag and fill state untouched. A retry on a live lifetime
// files the node normally.
//
// A write the engine already accepted carries that write's lifetime rather
// than a request's: context-free native maintenance passes context.Background().
// Request-driven backfills retain their request context.
// Preparation made for a caller's request passes that request's context, so
// cancellation stops it instead of filing a partially prepared attempt.
func (idx *CompositeIndex) IndexNode(ctx context.Context, nodeID NodeID, properties map[string]interface{}) error {
	if err := idx.mu.LockContext(ctx); err != nil {
		return err
	}
	defer idx.mu.Unlock()
	var keys preparedCompositeNodeKeys
	if err := idx.prepareNodeKeys(ctx, properties, &keys); err != nil {
		return err
	}
	idx.applyNodeKeysLocked(nodeID, &keys)
	return nil
}

// preparedCompositeNodeKeys contains every posting projection before mutation.
// Fixed prefix slots keep the usual small arity on the caller's stack.
type preparedCompositeNodeKeys struct {
	value          interface{}
	hasValue       bool
	fullHash       string
	prefixHashes   [8]string
	prefixOverflow []string
	prefixCount    int
}

func (keys *preparedCompositeNodeKeys) appendPrefix(hash string) {
	if keys.prefixCount < len(keys.prefixHashes) {
		keys.prefixHashes[keys.prefixCount] = hash
	} else {
		keys.prefixOverflow = append(keys.prefixOverflow, hash)
	}
	keys.prefixCount++
}

// prepareNodeKeys shares IndexNode's canonical projection with atomic relationship
// replacement. Its caller holds idx.mu; no posting or metadata is changed here.
func (idx *CompositeIndex) prepareNodeKeys(ctx context.Context, properties map[string]interface{}, keys *preparedCompositeNodeKeys) error {
	if len(idx.Properties) == 1 {
		value, exists := properties[idx.Properties[0]]
		if exists {
			var err error
			keys.value, keys.hasValue, err = indexNonNilMembershipKey(ctx, value)
			if err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	values, err := compositeIndexValues(ctx, idx, properties)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return ctx.Err()
	}
	complete := len(values) == len(idx.Properties)
	if len(values) > len(keys.prefixHashes) {
		keys.prefixOverflow = make([]string, 0, len(values)-len(keys.prefixHashes))
	}
	if complete {
		full, err := NewCompositeKey(ctx, values...)
		if err != nil {
			return err
		}
		keys.fullHash = full.Hash
	}
	for i := 1; i <= len(values); i++ {
		if complete && i == len(values) {
			keys.appendPrefix(keys.fullHash)
			break
		}
		prefix, err := NewCompositeKey(ctx, values[:i]...)
		if err != nil {
			return err
		}
		keys.appendPrefix(prefix.Hash)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// applyNodeKeysLocked publishes a complete projection after cancellable preparation.
func (idx *CompositeIndex) applyNodeKeysLocked(nodeID NodeID, keys *preparedCompositeNodeKeys) {
	if keys.hasValue {
		idx.fileValueLocked(nodeID, keys.value)
	}
	if keys.fullHash != "" {
		idx.fullIndex[keys.fullHash] = appendUnique(idx.fullIndex[keys.fullHash], nodeID)
	}
	for _, hash := range keys.prefixHashes[:min(keys.prefixCount, len(keys.prefixHashes))] {
		idx.prefixIndex[hash] = appendUnique(idx.prefixIndex[hash], nodeID)
	}
	for _, hash := range keys.prefixOverflow {
		idx.prefixIndex[hash] = appendUnique(idx.prefixIndex[hash], nodeID)
	}
}

// RemoveNode removes a node from the composite index.
// Call this when deleting a node or updating its indexed properties.
func (idx *CompositeIndex) RemoveNode(nodeID NodeID, properties map[string]interface{}) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.removeNodeLocked(nodeID, properties)
}

// removeNodeLocked is the accepted mutation phase shared by RemoveNode and
// relationship replacement after all cancellable preparation has succeeded.
func (idx *CompositeIndex) removeNodeLocked(nodeID NodeID, properties map[string]interface{}) {
	// This is completion of an accepted removal, independent of its request.
	var keys preparedCompositeNodeKeys
	if err := idx.prepareNodeKeys(context.Background(), properties, &keys); err != nil {
		return
	}
	idx.removeNodeKeysLocked(nodeID, &keys, 0)
}

// removeNodeKeysLocked retires a prepared projection, retaining shared leading
// keys during replacement. The caller holds idx.mu and has prepared both sides.
func (idx *CompositeIndex) removeNodeKeysLocked(nodeID NodeID, keys *preparedCompositeNodeKeys, shared int) {
	if keys.hasValue {
		idx.unfileValueLocked(nodeID, keys.value)
	}
	for i := shared; i < keys.prefixCount; i++ {
		hash := preparedPrefixHash(keys, i)
		idx.prefixIndex[hash] = removeNodeID(idx.prefixIndex[hash], nodeID)
		if len(idx.prefixIndex[hash]) == 0 {
			delete(idx.prefixIndex, hash)
		}
		if keys.fullHash != "" && hash == keys.fullHash {
			idx.fullIndex[hash] = removeNodeID(idx.fullIndex[hash], nodeID)
			if len(idx.fullIndex[hash]) == 0 {
				delete(idx.fullIndex, hash)
			}
		}
	}
}
