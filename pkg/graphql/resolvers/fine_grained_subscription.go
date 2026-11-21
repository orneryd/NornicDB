package resolvers

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (r *subscriptionResolver) openGraphSubscription(ctx context.Context) (string, error) {
	if err := r.authorizeDirectOperation(ctx, auth.PermRead); err != nil {
		return "", err
	}
	if r.EventBroker == nil {
		return "", errors.New("event broker not initialized")
	}
	return r.subscriptionSelection(ctx)
}

func subscribeGraphEvents[T any](ctx context.Context, r *subscriptionResolver, open func(string) <-chan T, project func(context.Context, storage.Engine, T) (T, bool)) (<-chan T, error) {
	selection, err := r.openGraphSubscription(ctx)
	if err != nil {
		return nil, err
	}
	principal := auth.FineGrainedRequestFromContext(ctx)
	if principal == nil {
		return open(selection), nil
	}
	database, engine, err := r.readDatabase(ctx)
	if err != nil {
		return nil, err
	}
	view := principal.Graph(engine, database)
	return watchFineGrainedEvents(ctx, principal, open(selection), func(event T) (T, bool) {
		return project(ctx, view, event)
	}), nil
}

func (r *subscriptionResolver) subscribeDeletedGraphEntities(ctx context.Context, relationship bool, open func(string) <-chan string) (<-chan string, error) {
	selection, err := r.openGraphSubscription(ctx)
	if err != nil {
		return nil, err
	}
	principal := auth.FineGrainedRequestFromContext(ctx)
	if principal == nil {
		return open(selection), nil
	}
	database, err := r.readableDatabase(ctx)
	if err != nil {
		return nil, err
	}
	// A deletion notification carries no stored entity to evaluate. A
	// relationship deletion also needs permission for both endpoints.
	if !principal.AllowsAllEntities("graph.node.traverse", database) ||
		relationship && !principal.AllowsAllEntities("graph.relationship.traverse", database) {
		return nil, auth.ErrFineGrainedForbidden
	}
	return watchFineGrainedEvents(ctx, principal, open(selection), func(id string) (string, bool) {
		return id, true
	}), nil
}

func projectSubscriptionNode(ctx context.Context, view storage.Engine, event *models.Node) (*models.Node, bool) {
	node, err := view.GetNode(ctx, storage.NodeID(event.ID))
	if err != nil {
		return nil, false
	}
	return storageNodeToModel(node), true
}

func projectSubscriptionRelationship(ctx context.Context, view storage.Engine, event *models.Relationship) (*models.Relationship, bool) {
	edge, err := view.GetEdge(ctx, storage.EdgeID(event.ID))
	if err != nil {
		return nil, false
	}
	return storageEdgeToModel(edge), true
}

func watchFineGrainedEvents[T any](ctx context.Context, principal *auth.FineGrainedRequest, input <-chan T, project func(T) (T, bool)) <-chan T {
	output := make(chan T)
	identity := auth.RequestIdentityFromContext(ctx)
	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-input:
				if !ok || principal.Revalidate(ctx, identity) != nil {
					return
				}
				projected, visible := project(event)
				if !visible {
					continue
				}
				select {
				case output <- projected:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}
