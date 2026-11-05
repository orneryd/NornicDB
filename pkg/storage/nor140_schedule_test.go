package storage

// Author: NOR-140 verification lane, Codex (gpt-6.1-sol), 2026-10-26.
// These adapters expose existing scheduling seams to external-package public
// API tests. They neither write cache entries nor implement publication rules.
//
// Ported unchanged from PR1690 commit 56b1e8e24 onto b098f7e70: the seams it
// names (the commit-tail hook and the write-behind buffer's own state view)
// still exist with the same signatures, so no migration was needed here.

func NOR140CommitTailHookForTest(hook func()) func() { return setCommitTailHook(hook) }

func NOR140StopReplayWorkerForTest(b *BadgerEngine) { b.writeBehind.Close() }

func NOR140BufferedNodeForTest(b *BadgerEngine, id NodeID) bool {
	_, found, deleted := b.writeBehind.GetNode(id)
	return found && !deleted
}
