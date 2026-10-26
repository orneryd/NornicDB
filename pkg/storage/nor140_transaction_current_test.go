//go:build !nor140_legacy_tx

package storage

// Author: NOR-140 verification lane, Codex (gpt-6.1-sol), 2026-10-26.
// d663/#1548 expose the client-image policy on the transaction API.
func NOR140StageNodeForTest(tx *BadgerTransaction, node *Node) error {
	return tx.UpdateGraphNode(node, ClientNodeEdits)
}
