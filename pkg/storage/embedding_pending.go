package storage

import (
	"bytes"
	"context"
	"github.com/dgraph-io/badger/v4"
)

// PendingEmbeddingCounter counts MAIN in one snapshot. accept may filter or
// filter IDs without decoding bodies or loading vectors. Key bytes are borrowed
// for the callback only; callers must not retain or modify them.
//
// ctx is the caller's context: a fallback scan of an engine without the index
// runs under it, never under a context nobody can end.
type PendingEmbeddingCounter interface {
	CountPendingEmbeddingIDs(ctx context.Context, accept func([]byte) bool) int
}

func (b *BadgerEngine) CountPendingEmbeddingIDs(_ context.Context, accept func([]byte) bool) int {
	if b.pendingNamespaceConflict("") != nil {
		return 0
	}

	count := 0
	_ = b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixPendingEmbed}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if accept == nil || accept(it.Item().Key()[1:]) {
				count++
			}
		}
		return nil
	})
	return count
}

// CountPendingEmbeddingIDs preserves the storage capability's fallback for
// engines without a durable pending index, using selection's eligibility rule.
func CountPendingEmbeddingIDs(ctx context.Context, engine Engine, accept func([]byte) bool) int {
	if counter, ok := engine.(PendingEmbeddingCounter); ok {
		return counter.CountPendingEmbeddingIDs(ctx, accept)
	}
	count := 0
	_ = engine.StreamNodesWithOptions(ctx, StreamNodesOptions{WithEmbeddings: true}, func(node *Node) error {
		if NodeNeedsEmbedding(node) && (accept == nil || accept([]byte(node.ID))) {
			count++
		}
		return nil
	})
	return count
}

func (n *NamespacedEngine) CountPendingEmbeddingIDs(ctx context.Context, accept func([]byte) bool) int {
	prefix := []byte(n.nodeScope())
	return CountPendingEmbeddingIDs(ctx, n.inner, func(id []byte) bool {
		return bytes.HasPrefix(id, prefix) && (accept == nil || accept(id[len(prefix):]))
	})
}
func (w *WALEngine) CountPendingEmbeddingIDs(ctx context.Context, accept func([]byte) bool) int {
	return CountPendingEmbeddingIDs(ctx, w.engine, accept)
}
func (c *CompositeEngine) CountPendingEmbeddingIDs(ctx context.Context, accept func([]byte) bool) int {
	total := 0
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		total += CountPendingEmbeddingIDs(ctx, engine, accept)
	}
	return total
}
