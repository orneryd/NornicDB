package storage

// External public-Cypher tests use the existing storage commit-tail gate and
// inspect retained index ownership without adding a production diagnostic API.
func RelationshipSeekTestCommitTail(hook func()) func() { return setCommitTailHook(hook) }

func RelationshipSeekTestEngineIndexCount(engine *BadgerEngine) int {
	engine.relationshipUniqueIndexes.mu.Lock()
	defer engine.relationshipUniqueIndexes.mu.Unlock()
	return len(engine.relationshipUniqueIndexes.entries)
}

// RelationshipSeekTestBucketSize inspects both retained keys and candidates.
func RelationshipSeekTestBucketSize(engine *BadgerEngine, namespace, relType, property, value string) (int, int) {
	set := &engine.relationshipUniqueIndexes
	set.mu.Lock()
	defer set.mu.Unlock()
	idx := set.entries[relationshipUniqueIndexKey{namespace: namespace, relType: relType, properties: relationshipPropertyKey([]string{property})}]
	if idx == nil {
		return 0, 0
	}
	return len(idx.keys), len(idx.index.LookupFull(value))
}
