package multidb

import (
	"errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// An immutable batch is prepared once under write admission, checked before
// commit, and applied at successful publication. No route reassembles its delta.
type accountingBatch struct {
	nodes entityAccountingDelta
	edges entityAccountingDelta
}

type entityAccountingDelta struct {
	count int64
	bytes int64
}

func (t *sizeTrackingEngine) prepareAccountingBatch(tx accountingTransaction, changes storage.TransactionChanges, trackBytes bool) (accountingBatch, error) {
	unprefix := func(id string) string { return strings.TrimPrefix(id, t.dbName+":") }
	nodes, err := storedEntityAccounting(changes.Nodes,
		func(id storage.NodeID) (*storage.Node, error) {
			return storage.GetStoredNode(t.Engine, storage.NodeID(unprefix(string(id))))
		},
		func(node *storage.Node) (*storage.Node, error) {
			image, err := tx.StoredNodeImage(node)
			if err == nil {
				image.ID = storage.NodeID(unprefix(string(image.ID)))
			}
			return image, err
		}, calculateNodeSize, trackBytes, func(id storage.NodeID) (bool, error) {
			return storage.StoredNodeExists(t.Engine, storage.NodeID(unprefix(string(id))))
		})
	if err != nil {
		return accountingBatch{}, err
	}
	edges, err := storedEntityAccounting(changes.Edges,
		func(id storage.EdgeID) (*storage.Edge, error) {
			return storage.GetStoredEdge(t.Engine, storage.EdgeID(unprefix(string(id))))
		},
		func(edge *storage.Edge) (*storage.Edge, error) {
			image, err := tx.StoredEdgeImage(edge)
			if err == nil {
				image.ID = storage.EdgeID(unprefix(string(image.ID)))
				image.StartNode = storage.NodeID(unprefix(string(image.StartNode)))
				image.EndNode = storage.NodeID(unprefix(string(image.EndNode)))
			}
			return image, err
		}, calculateEdgeSize, trackBytes, func(id storage.EdgeID) (bool, error) {
			return storage.StoredEdgeExists(t.Engine, storage.EdgeID(unprefix(string(id))))
		})
	return accountingBatch{nodes: nodes, edges: edges}, err
}

// Fold exactly one hidden-inclusive stored prior/final pair per ID. Existence
// and size are derived together from those same images, including deletion and
// replacement; the operation history is deliberately irrelevant here.
func storedEntityAccounting[K comparable, V any](pending map[K]*V, read func(K) (*V, error), image func(*V) (*V, error), size func(*V) (int64, error), trackBytes bool, presence func(K) (bool, error)) (entityAccountingDelta, error) {
	var delta entityAccountingDelta
	for id, final := range pending {
		var prior *V
		var found bool
		var err error
		if !trackBytes && presence != nil {
			found, err = presence(id)
		} else {
			prior, err = read(id)
			found = prior != nil
		}
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return entityAccountingDelta{}, err
		}
		if errors.Is(err, storage.ErrNotFound) {
			prior, found = nil, false
		}
		pair := [2]struct {
			body   *V
			exists bool
		}{{prior, found}, {final, final != nil}}
		for side, state := range pair {
			if !state.exists {
				continue
			}
			sign := int64(2*side - 1)
			if trackBytes {
				body, err := image(state.body)
				if err != nil {
					return entityAccountingDelta{}, err
				}
				n, err := size(body)
				if err != nil {
					return entityAccountingDelta{}, err
				}
				delta.bytes += sign * n
			}
			delta.count += sign
		}
	}
	return delta, nil
}

func (b accountingBatch) admit(t *sizeTrackingEngine, limits *Limits, currentBytes int64, except ...*accountingReservation) error {
	if limits == nil {
		return nil
	}
	var omitted *accountingReservation
	if len(except) != 0 {
		omitted = except[0]
	}
	reserved := t.reservedExcept(omitted)
	if b.nodes.count > 0 && limits.Storage.MaxNodes > 0 {
		count, err := t.Engine.NodeCount()
		if err != nil {
			return err
		}
		if count+b.nodes.count+reserved.nodes.count > limits.Storage.MaxNodes {
			return localizedError(localization.MultidbMaxNodesReached(t.dbName, count+b.nodes.count, limits.Storage.MaxNodes), ErrStorageLimitExceeded)
		}
	}
	if b.edges.count > 0 && limits.Storage.MaxEdges > 0 {
		count, err := t.Engine.EdgeCount()
		if err != nil {
			return err
		}
		if count+b.edges.count+reserved.edges.count > limits.Storage.MaxEdges {
			return localizedError(localization.MultidbMaxEdgesReached(t.dbName, count+b.edges.count, limits.Storage.MaxEdges), ErrStorageLimitExceeded)
		}
	}
	if delta := b.nodes.bytes + b.edges.bytes; delta > 0 && limits.Storage.MaxBytes > 0 && currentBytes+delta+reserved.nodes.bytes+reserved.edges.bytes > limits.Storage.MaxBytes {
		return localizedError(localization.MultidbMaxBytesExceeded(t.dbName, currentBytes, limits.Storage.MaxBytes, delta), ErrStorageLimitExceeded)
	}
	return nil
}
