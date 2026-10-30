//go:build !nornic_copycount

package storage

// countNodeCopy and countEdgeCopy record each structural entity copy in the
// nornic_copycount test build (structural_copy_count_counted.go), which proves
// every retained delivery is copied exactly once (NOR-160). Other builds
// compile them away.
func countNodeCopy(*Node) {}

func countEdgeCopy(*Edge) {}

func countVectorCopy(*Node) {}
