package storage

import (
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// BulkCreateNodesForRecovery creates recovered nodes while adapting to backend
// transaction limits. It preserves the normal bulk path unless the backend says
// the requested transaction is too large.
func BulkCreateNodesForRecovery(engine Engine, nodes []*Node) error {
	if len(nodes) == 0 {
		return nil
	}
	if err := engine.BulkCreateNodes(nodes); err != nil {
		if !isRecoveryBatchTooLarge(err) || len(nodes) == 1 {
			return err
		}
		mid := len(nodes) / 2
		if err := BulkCreateNodesForRecovery(engine, nodes[:mid]); err != nil {
			return err
		}
		return BulkCreateNodesForRecovery(engine, nodes[mid:])
	}
	return nil
}

// BulkCreateEdgesForRecovery creates recovered edges while adapting to backend
// transaction limits. A single oversized edge still returns the original error.
func BulkCreateEdgesForRecovery(engine Engine, edges []*Edge) error {
	if len(edges) == 0 {
		return nil
	}
	if err := engine.BulkCreateEdges(edges); err != nil {
		if !isRecoveryBatchTooLarge(err) || len(edges) == 1 {
			return err
		}
		mid := len(edges) / 2
		if err := BulkCreateEdgesForRecovery(engine, edges[:mid]); err != nil {
			return err
		}
		return BulkCreateEdgesForRecovery(engine, edges[mid:])
	}
	return nil
}

func isRecoveryBatchTooLarge(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, badger.ErrTxnTooBig) || strings.Contains(err.Error(), badger.ErrTxnTooBig.Error())
}
