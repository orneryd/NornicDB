package storage

import "context"

type historicalBufferedImage[T interface{ *Node | *Edge }] struct {
	head, tombstone, atVersion MVCCVersion
	at                         T
	deleted                    bool
}

// resolve considers only acknowledgements that the physical snapshot has yet
// to materialize. A newer tombstone masks any body from a prior incarnation.
func (image historicalBufferedImage[T]) resolve(physical, cutoff MVCCVersion) (T, bool) {
	if image.head.Compare(physical) <= 0 {
		return nil, false
	}
	if image.deleted || (image.tombstone.Compare(physical) > 0 &&
		(image.tombstone.Compare(cutoff) > 0 || image.at == nil || image.tombstone.Compare(image.atVersion) > 0)) {
		return nil, true
	}
	if image.atVersion.Compare(physical) > 0 {
		return image.at, true
	}
	return nil, false
}

type historicalBufferedEntities[ID comparable, T *Node | *Edge] struct {
	images map[ID]historicalBufferedImage[T]
	ids    []ID
}

func (entities *historicalBufferedEntities[ID, T]) record(id ID, body T, version, cutoff MVCCVersion) {
	image, exists := entities.images[id]
	if !exists {
		if entities.images == nil {
			entities.images = make(map[ID]historicalBufferedImage[T])
		}
		entities.ids = append(entities.ids, id)
	}
	if version.Compare(image.head) >= 0 {
		image.head = version
		image.deleted = body == nil
	}
	if body == nil && version.Compare(image.tombstone) > 0 {
		image.tombstone = version
	}
	if version.Compare(cutoff) <= 0 && version.Compare(image.atVersion) >= 0 {
		image.atVersion = version
		image.at = body
	}
	entities.images[id] = image
}

// historicalBufferedRead indexes only the scoped buffered mutations of the
// captured view. Its images borrow immutable bodies from the retained pin.
// Adjacency looks up only the buffered edges attached at the cutoff.
type historicalBufferedRead struct {
	nodes              historicalBufferedEntities[NodeID, *Node]
	edges              historicalBufferedEntities[EdgeID, *Edge]
	incoming, outgoing map[NodeID][]EdgeID
}

func indexHistoricalBufferedRead(ctx context.Context, pin *transactionBeginPresence, prefix string, cutoff MVCCVersion) (*historicalBufferedRead, error) {
	if pin == nil {
		return nil, nil
	}
	var result historicalBufferedRead
	err := pin.visitPinnedCommits(ctx, func(commit *bufferedCommit) error {
		// Legacy buffer entries without an MVCC version are live overlays
		// only. A historical read cannot assign them to a cutoff.
		if commit.version == (MVCCVersion{}) {
			return nil
		}
		return visitCommitEntityImages(ctx, commit.ops,
			func(id NodeID, _, after *Node) error {
				if id != "" && nodeIDInScope(id, prefix) {
					result.nodes.record(id, after, commit.version, cutoff)
				}
				return nil
			},
			func(id EdgeID, _, after *Edge) error {
				if id != "" && edgeIDInScope(id, prefix) {
					result.edges.record(id, after, commit.version, cutoff)
				}
				return nil
			})
	})
	if err != nil {
		return nil, err
	}
	if len(result.nodes.ids) == 0 && len(result.edges.ids) == 0 {
		return nil, nil
	}
	for _, id := range result.edges.ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		edge, _ := result.edges.images[id].resolve(MVCCVersion{}, cutoff)
		if edge == nil {
			continue
		}
		if result.incoming == nil {
			result.incoming = make(map[NodeID][]EdgeID)
			result.outgoing = make(map[NodeID][]EdgeID)
		}
		result.incoming[edge.EndNode] = append(result.incoming[edge.EndNode], id)
		result.outgoing[edge.StartNode] = append(result.outgoing[edge.StartNode], id)
	}
	return &result, nil
}

func (overlay *historicalBufferedRead) adjacentIDs(request AdjacentEdgeRead) []EdgeID {
	if overlay == nil {
		return nil
	}
	if request.Direction == AdjacentIncoming {
		return overlay.incoming[request.Node]
	}
	return overlay.outgoing[request.Node]
}
