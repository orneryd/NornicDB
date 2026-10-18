package storage

// External public-Cypher tests use the existing storage commit-tail gate and
// inspect retained index ownership without adding a production diagnostic API.
func RelationshipSeekTestCommitTail(hook func()) func() { return setCommitTailHook(hook) }

func RelationshipSeekTestEngineIndexCount(engine *BadgerEngine) int {
	engine.relationshipUniqueIndexes.mu.Lock()
	defer engine.relationshipUniqueIndexes.mu.Unlock()
	return len(engine.relationshipUniqueIndexes.entries)
}
