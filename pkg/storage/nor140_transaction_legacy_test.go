//go:build nor140_legacy_tx

package storage

// Author: NOR-140 verification lane, Codex (gpt-6.1-sol), 2026-10-26.
// #1676's pinned pre-release API implicitly uses the client-image policy.
// This test-only build tag changes no product source or product build tags.
func NOR140StageNodeForTest(tx *BadgerTransaction, node *Node) error {
	return tx.UpdateNode(node)
}
