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
		if !IsTransactionTooBig(err) || len(nodes) == 1 {
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
		if !IsTransactionTooBig(err) || len(edges) == 1 {
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

// IsTransactionTooBig reports whether err is Badger's "Txn is too big" (the
// transaction's writes exceed the engine's per-transaction limit), wrapped or
// only kept as text by a wrapper that dropped the chain.
func IsTransactionTooBig(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, badger.ErrTxnTooBig) || strings.Contains(err.Error(), badger.ErrTxnTooBig.Error())
}
