package search

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type nodeImageRead uint8

const (
	fullNodeImage nodeImageRead = iota
	metadataNodeImage
)

// readNodeImage owns point acquisition and its observation. Metadata consumers
// use the optional body reader; population/refresh consumers retain full images.
func (s *Service) readNodeImage(ctx context.Context, id storage.NodeID, image nodeImageRead) (*storage.Node, error) {
	counter := queryprofile.Access(ctx, queryprofile.KindNodeByIDSeek, "")
	var node *storage.Node
	var err error
	if reader, ok := s.engine.(storage.NodeWithoutEmbeddingsReader); image == metadataNodeImage && ok {
		node, err = reader.GetNodeWithoutEmbeddings(ctx, id)
	} else {
		node, err = s.engine.GetNode(ctx, id)
	}
	queryprofile.EntityReadResult(counter, node, err)
	return node, err
}

// schemaConstraints acquires the schema's constraint catalog once and records
// that acquisition: the entries the catalog returned, before the caller filters
// them, and the one request. An engine without a schema answers without
// acquiring a catalog, so no request is reported; a schema holding no constraint
// is an observed zero, not a missing measurement.
func (s *Service) schemaConstraints(ctx context.Context) []storage.Constraint {
	schema := s.engine.GetSchema()
	if schema == nil {
		return nil
	}
	constraints := schema.GetAllConstraints()
	queryprofile.Access(ctx, queryprofile.KindSchemaConstraintCatalog, "").Add(int64(len(constraints)), 1)
	return constraints
}

// optionalBatchNodesWithoutEmbeddings makes at most one actual request to the
// engine's optional embedding-free batch reader, which both callers used to
// invoke and observe separately. answered reports whether that reader answered
// the request at all: it is false only when the engine has no such reader or
// declines it with storage.ErrNotImplemented, the two cases that performed no
// read and leave the caller's own fallback to answer. A reader that failed did
// run, so answered is true with the error, and the attempt is one request with
// the zero rows a failed read returned.
func optionalBatchNodesWithoutEmbeddings(ctx context.Context, engine storage.Engine, ids []storage.NodeID) (nodes map[storage.NodeID]*storage.Node, answered bool, err error) {
	reader, ok := engine.(storage.BatchNodeWithoutEmbeddingsReader)
	if !ok {
		return nil, false, nil
	}
	nodes, err = reader.BatchGetNodesWithoutEmbeddings(ctx, ids)
	if errors.Is(err, storage.ErrNotImplemented) {
		return nil, false, nil
	}
	counter := queryprofile.Access(ctx, queryprofile.KindNodeBatchByIDSeek, "")
	if err != nil {
		counter.Add(0, 1)
		return nil, true, err
	}
	counter.Add(int64(len(nodes)), 1)
	return nodes, true, nil
}

func (s *Service) batchGetNodesWithoutEmbeddings(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	// An empty request reads nothing, so it neither runs the optional reader nor
	// reports an attempt.
	if len(ids) == 0 {
		return map[storage.NodeID]*storage.Node{}, nil
	}
	nodes, answered, err := optionalBatchNodesWithoutEmbeddings(ctx, s.engine, ids)
	if err != nil {
		return nil, err
	}
	if answered {
		return nodes, nil
	}
	// This caller's own fallback is the point reader, one actual node read per
	// id. It is not the continuation page's bulk fallback.
	nodes = make(map[storage.NodeID]*storage.Node, len(ids))
	for _, id := range ids {
		node, err := s.readNodeImage(ctx, id, metadataNodeImage)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, err
		}
		nodes[id] = node
	}
	return nodes, nil
}

// readEdgeImage owns the full relationship point read used by refresh and
// population reconciliation. Read loops with a pre-resolved batch counter keep
// their existing reader contract and account at their own acquisition boundary.
func (s *Service) readEdgeImage(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	edge, err := s.engine.GetEdge(ctx, id)
	queryprofile.EntityReadResult(queryprofile.Access(ctx, queryprofile.KindRelationshipByIDSeek, ""), edge, err)
	return edge, err
}
