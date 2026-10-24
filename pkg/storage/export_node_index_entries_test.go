package storage

// SetNodeIndexEntriesReplacedHookForTesting installs hook to run after each
// node write replaces its equality-index entries, while the writer is still
// publishing its commit. It returns the restore function.
func SetNodeIndexEntriesReplacedHookForTesting(hook func(NodeID)) func() {
	previous := nodeIndexEntriesReplacedHook.Swap(&hook)
	return func() { nodeIndexEntriesReplacedHook.Store(previous) }
}
