package auth

import (
	"context"
	"maps"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func newlyCreated(entities *sync.Map, id any, stamp time.Time) bool {
	prior, ok := entities.Load(id)
	return ok && prior.(time.Time).Equal(stamp)
}

func (g *fineGrainedGraph) allowNodeCreate(node *storage.Node) error {
	if node == nil {
		return storage.ErrInvalidData
	}
	target := FineGrainedResource{Kind: FineGrainedNode, DatabaseID: g.databaseID, HomeDatabaseID: g.request.HomeDatabaseID, Types: node.Labels, CreatingEntity: true}
	if !g.request.Access.Allows("graph.node.create", target) {
		return ErrFineGrainedForbidden
	}
	for _, label := range node.Labels {
		perLabel := target
		perLabel.Types = []string{label}
		if !g.request.Access.Allows("graph.node.create", perLabel) {
			return ErrFineGrainedForbidden
		}
	}
	if err := g.requireGraphTokens(node.Labels, nil); err != nil {
		return err
	}
	if err := g.authorizeCreateProperties("graph.node.property.set", target, node.Properties); err != nil {
		return err
	}
	if (len(node.NamedEmbeddings) != 0 || len(node.ChunkEmbeddings) != 0) &&
		!g.request.Access.Allows("graph.node.embedding.create", target) {
		return ErrFineGrainedForbidden
	}
	return nil
}

func (g *fineGrainedGraph) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	if err := g.allowNodeCreate(node); err != nil {
		return "", err
	}
	id, err := g.engine.CreateNode(ctx, node)
	if err == nil {
		g.createdNodes.Store(id, node.CreatedAt)
	}
	return id, err
}

func (g *fineGrainedGraph) allowEdgeCreate(ctx context.Context, edge *storage.Edge) error {
	if edge == nil {
		return storage.ErrInvalidData
	}
	target := FineGrainedResource{Kind: FineGrainedRelationship, DatabaseID: g.databaseID, HomeDatabaseID: g.request.HomeDatabaseID, Types: []string{edge.Type}, CreatingEntity: true}
	if !g.request.Access.Allows("graph.relationship.create", target) {
		return ErrFineGrainedForbidden
	}
	if err := g.requireGraphTokens(nil, []string{edge.Type}); err != nil {
		return err
	}
	if err := g.authorizeCreateProperties("graph.relationship.property.set", target, edge.Properties); err != nil {
		return err
	}
	for _, id := range [...]storage.NodeID{edge.StartNode, edge.EndNode} {
		node, err := g.engine.GetNode(ctx, id)
		if err != nil {
			return err
		}
		if !newlyCreated(&g.createdNodes, node.ID, node.CreatedAt) &&
			!g.request.Access.Allows("graph.node.traverse", g.nodeResource(node)) {
			return storage.ErrNotFound
		}
	}
	return nil
}

func (g *fineGrainedGraph) CreateEdge(ctx context.Context, edge *storage.Edge) error {
	if err := g.allowEdgeCreate(ctx, edge); err != nil {
		return err
	}
	if err := g.engine.CreateEdge(ctx, edge); err != nil {
		return err
	}
	g.createdEdges.Store(edge.ID, edge.CreatedAt)
	return nil
}

func (g *fineGrainedGraph) requireGraphTokens(labels, types []string) error {
	schema := g.engine.GetSchema()
	if schema == nil {
		return ErrFineGrainedForbidden
	}
	if (!schema.KnowsGraphTokens(labels, nil) && !g.request.AllowsDatabase("database.token.label.create", g.database, "")) ||
		(!schema.KnowsGraphTokens(nil, types) && !g.request.AllowsDatabase("database.token.relationship_type.create", g.database, "")) {
		return ErrFineGrainedForbidden
	}
	return nil
}

func (g *fineGrainedGraph) authorizeCreateProperties(action string, target FineGrainedResource, properties map[string]any) error {
	for key, value := range properties {
		if value == nil {
			continue
		}
		target.Property = key
		if !g.request.Access.Allows(action, target) || !g.canUsePropertyName(key) {
			return ErrFineGrainedForbidden
		}
	}
	return nil
}

func (g *fineGrainedGraph) canUsePropertyName(key string) bool {
	return g.request.AllowsDatabase("database.token.property.create", g.database, "") ||
		storage.KnowsPropertyName(g.engine, key)
}

// applyPropertyEdit checks the change against the authorized image that could
// have produced it, then applies the change to a copy of the stored image.
// Unobserved properties survive updates that never supplied those properties.
func (g *fineGrainedGraph) applyPropertyEdit(action string, target FineGrainedResource, visible, after map[string]any, intent storage.PropertyWriteIntent) (map[string]any, error) {
	merged := make(map[string]any, len(target.EntityProperties))
	maps.Copy(merged, target.EntityProperties)
	for key, set := range intent.Keys {
		target.Property = key
		if !g.request.Access.Allows(action, target) || set && !g.canUsePropertyName(key) {
			return nil, ErrFineGrainedForbidden
		}
		if _, retained := after[key]; !retained {
			delete(merged, key)
		}
	}
	if intent.Replace {
		for key := range merged {
			if _, retained := after[key]; retained {
				continue
			}
			target.Property = key
			if !g.request.Access.Allows(action, target) {
				return nil, ErrFineGrainedForbidden
			}
			delete(merged, key)
		}
	}
	for key, prior := range visible {
		next, retained := after[key]
		if retained && reflect.DeepEqual(prior, next) {
			continue
		}
		target.Property = key
		if !g.request.Access.Allows(action, target) {
			return nil, ErrFineGrainedForbidden
		}
		if !retained {
			delete(merged, key)
		}
	}
	for key, next := range after {
		if prior, observed := visible[key]; observed && reflect.DeepEqual(prior, next) {
			continue
		}
		target.Property = key
		if !g.request.Access.Allows(action, target) || (next != nil && !g.canUsePropertyName(key)) {
			return nil, ErrFineGrainedForbidden
		}
		merged[key] = next
	}
	return merged, nil
}

func (g *fineGrainedGraph) allowLabelChanges(target FineGrainedResource, labels []string) bool {
	prior := target.Types
	for _, label := range prior {
		if !slices.Contains(labels, label) {
			target.Types = []string{label}
			if !g.request.Access.Allows("graph.node.label.remove", target) {
				return false
			}
		}
	}
	for _, label := range labels {
		if !slices.Contains(prior, label) {
			target.Types = []string{label}
			if !g.request.Access.Allows("graph.node.label.add", target) || g.requireGraphTokens([]string{label}, nil) != nil {
				return false
			}
		}
	}
	return true
}

func (g *fineGrainedGraph) UpdateNode(ctx context.Context, request storage.NodeUpdateRequest) error {
	if request.After == nil {
		return storage.ErrInvalidData
	}
	stored, err := g.engine.GetNode(ctx, request.After.ID)
	if err != nil {
		return err
	}
	target := g.nodeResource(stored)
	visible := g.projectNode(stored)
	if newlyCreated(&g.createdNodes, stored.ID, stored.CreatedAt) {
		visible = stored
	}
	if visible == nil || !g.allowLabelChanges(target, request.After.Labels) {
		return ErrFineGrainedForbidden
	}
	if request.Before != nil && (!request.Before.CreatedAt.Equal(stored.CreatedAt) ||
		!request.Before.UpdatedAt.Equal(stored.UpdatedAt) ||
		!slices.Equal(request.Before.Labels, stored.Labels) ||
		!reflect.DeepEqual(request.Before.Properties, visible.Properties)) {
		return storage.ErrConflict
	}
	properties, err := g.applyPropertyEdit("graph.node.property.set", target, visible.Properties, request.After.Properties, request.PropertyWrites)
	if err != nil {
		return err
	}
	updated := *stored
	updated.Labels, updated.Properties = request.After.Labels, properties
	if !reflect.DeepEqual(visible.NamedEmbeddings, request.After.NamedEmbeddings) ||
		!reflect.DeepEqual(visible.ChunkEmbeddings, request.After.ChunkEmbeddings) {
		if !g.request.Access.Allows("graph.node.embedding.set", target) ||
			!g.request.Access.Allows("graph.node.embedding.read", target) {
			return ErrFineGrainedForbidden
		}
		updated.NamedEmbeddings, updated.ChunkEmbeddings = request.After.NamedEmbeddings, request.After.ChunkEmbeddings
	}

	request.Before, request.After, request.PropertyWrites = stored, &updated, storage.PropertyWriteIntent{}
	return g.engine.UpdateNode(ctx, request)
}

func (g *fineGrainedGraph) UpdateEdge(ctx context.Context, request storage.EdgeUpdateRequest) error {
	if request.After == nil {
		return storage.ErrInvalidData
	}
	stored, err := g.engine.GetEdge(ctx, request.After.ID)
	if err != nil {
		return err
	}
	visible, err := g.projectEdge(ctx, stored)
	if err != nil {
		return err
	}
	if newlyCreated(&g.createdEdges, stored.ID, stored.CreatedAt) {
		visible = stored
	}
	if visible == nil || request.After.Type != stored.Type ||
		request.After.StartNode != stored.StartNode || request.After.EndNode != stored.EndNode {
		return ErrFineGrainedForbidden
	}
	if request.Before != nil && (!request.Before.CreatedAt.Equal(stored.CreatedAt) ||
		!request.Before.UpdatedAt.Equal(stored.UpdatedAt) ||
		!reflect.DeepEqual(request.Before.Properties, visible.Properties)) {
		return storage.ErrConflict
	}
	properties, err := g.applyPropertyEdit("graph.relationship.property.set", g.edgeResource(stored), visible.Properties, request.After.Properties, request.PropertyWrites)
	if err != nil {
		return err
	}
	updated := *stored
	updated.Properties = properties
	request.Before, request.After, request.PropertyWrites = stored, &updated, storage.PropertyWriteIntent{}
	return g.engine.UpdateEdge(ctx, request)
}

func (g *fineGrainedGraph) allowEdgeDelete(ctx context.Context, edge *storage.Edge) error {
	visible, err := g.projectEdge(ctx, edge)
	if err != nil {
		return err
	}
	if (visible == nil && !newlyCreated(&g.createdEdges, edge.ID, edge.CreatedAt)) ||
		!g.request.Access.Allows("graph.relationship.delete", g.edgeResource(edge)) {
		return ErrFineGrainedForbidden
	}
	return nil
}

func (g *fineGrainedGraph) validateNodeDeletion(ctx context.Context, id storage.NodeID, mode storage.NodeDeletionMode) error {
	node, err := g.engine.GetNode(ctx, id)
	if err != nil {
		return err
	}
	target := g.nodeResource(node)
	if (!newlyCreated(&g.createdNodes, node.ID, node.CreatedAt) && !g.request.Access.Allows("graph.node.traverse", target)) ||
		!g.request.Access.Allows("graph.node.delete", target) {
		return ErrFineGrainedForbidden
	}
	if mode == storage.CascadeEdges {
		var edges []*storage.Edge
		var err error
		if lister, ok := g.engine.(storage.IncidentEdgeLister); ok {
			edges, err = lister.IncidentEdges(ctx, id)
		} else {
			edges, err = g.engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentBoth, Node: id})
		}
		if err != nil {
			return err
		}
		for _, edge := range edges {
			if err := g.allowEdgeDelete(ctx, edge); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *fineGrainedGraph) DeleteNode(ctx context.Context, id storage.NodeID) error {
	return g.DeleteGraphNode(ctx, id, storage.CascadeEdges)
}

func (g *fineGrainedGraph) DeleteGraphNode(ctx context.Context, id storage.NodeID, mode storage.NodeDeletionMode) error {
	if err := g.validateNodeDeletion(ctx, id, mode); err != nil {
		return err
	}
	if owner, ok := g.engine.(interface {
		DeleteGraphNode(context.Context, storage.NodeID, storage.NodeDeletionMode) error
	}); ok {
		return owner.DeleteGraphNode(ctx, id, mode)
	}
	if mode != storage.CascadeEdges {
		return ErrFineGrainedForbidden
	}
	return g.engine.DeleteNode(ctx, id)
}

func (g *fineGrainedGraph) DeleteEdge(ctx context.Context, id storage.EdgeID) error {
	edge, err := g.engine.GetEdge(ctx, id)
	if err != nil {
		return err
	}
	if err := g.allowEdgeDelete(ctx, edge); err != nil {
		return err
	}
	return g.engine.DeleteEdge(ctx, id)
}

func (g *fineGrainedGraph) BulkCreateNodes(ctx context.Context, nodes []*storage.Node) error {
	for _, node := range nodes {
		if err := g.allowNodeCreate(node); err != nil {
			return err
		}
	}
	if err := g.engine.BulkCreateNodes(ctx, nodes); err != nil {
		return err
	}
	for _, node := range nodes {
		g.createdNodes.Store(node.ID, node.CreatedAt)
	}
	return nil
}

func (g *fineGrainedGraph) BulkCreateEdges(ctx context.Context, edges []*storage.Edge) error {
	for _, edge := range edges {
		if err := g.allowEdgeCreate(ctx, edge); err != nil {
			return err
		}
	}
	if err := g.engine.BulkCreateEdges(ctx, edges); err != nil {
		return err
	}
	for _, edge := range edges {
		g.createdEdges.Store(edge.ID, edge.CreatedAt)
	}
	return nil
}

func (g *fineGrainedGraph) BulkDeleteNodes(ctx context.Context, ids []storage.NodeID) error {
	for _, id := range ids {
		if err := g.validateNodeDeletion(ctx, id, storage.CascadeEdges); err != nil {
			return err
		}
	}
	return g.engine.BulkDeleteNodes(ctx, ids)
}

func (g *fineGrainedGraph) BulkDeleteEdges(ctx context.Context, ids []storage.EdgeID) error {
	for _, id := range ids {
		edge, err := g.engine.GetEdge(ctx, id)
		if err != nil {
			return err
		}
		if err := g.allowEdgeDelete(ctx, edge); err != nil {
			return err
		}
	}
	return g.engine.BulkDeleteEdges(ctx, ids)
}
