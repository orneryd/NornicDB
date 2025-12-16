package resolvers

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
)

// Subscriptions are not yet implemented - these return errors

func (r *subscriptionResolver) subscriptionNodeCreated(ctx context.Context, labels []string) (<-chan *models.Node, error) {
	return nil, fmt.Errorf("subscriptions not yet implemented")
}

func (r *subscriptionResolver) subscriptionNodeUpdated(ctx context.Context, id *string, labels []string) (<-chan *models.Node, error) {
	return nil, fmt.Errorf("subscriptions not yet implemented")
}

func (r *subscriptionResolver) subscriptionNodeDeleted(ctx context.Context, labels []string) (<-chan string, error) {
	return nil, fmt.Errorf("subscriptions not yet implemented")
}

func (r *subscriptionResolver) subscriptionRelationshipCreated(ctx context.Context, types []string) (<-chan *models.Relationship, error) {
	return nil, fmt.Errorf("subscriptions not yet implemented")
}

func (r *subscriptionResolver) subscriptionRelationshipUpdated(ctx context.Context, id *string, types []string) (<-chan *models.Relationship, error) {
	return nil, fmt.Errorf("subscriptions not yet implemented")
}

func (r *subscriptionResolver) subscriptionRelationshipDeleted(ctx context.Context, types []string) (<-chan string, error) {
	return nil, fmt.Errorf("subscriptions not yet implemented")
}

func (r *subscriptionResolver) subscriptionSearchStream(ctx context.Context, query string, options *models.SearchOptions) (<-chan *models.SearchResult, error) {
	return nil, fmt.Errorf("subscriptions not yet implemented")
}
