package resolvers

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
)

// Every resolver has an explicit effect. Cypher pass-through fields declare
// no fixed effect: executeCypher admits their actual statement and selection.
var resolverClasses = map[string]cypher.PrivilegeClass{
	"cypherResultResolver.Rows":                cypher.PrivilegeNone,
	"mutationResolver.CreateNode":              cypher.PrivilegeWrite,
	"mutationResolver.UpdateNode":              cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.DeleteNode":              cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.BulkCreateNodes":         cypher.PrivilegeWrite,
	"mutationResolver.BulkDeleteNodes":         cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.MergeNode":               cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.CreateRelationship":      cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.UpdateRelationship":      cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.DeleteRelationship":      cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.BulkCreateRelationships": cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.BulkDeleteRelationships": cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.MergeRelationship":       cypher.PrivilegeRead | cypher.PrivilegeWrite,
	"mutationResolver.ExecuteCypher":           cypher.PrivilegeNone,
	"mutationResolver.TriggerEmbedding":        cypher.PrivilegeRead | cypher.PrivilegeWrite | cypher.PrivilegeSharedState,
	"mutationResolver.RebuildSearchIndex":      cypher.PrivilegeRead | cypher.PrivilegeSharedState,
	"mutationResolver.RunDecay":                cypher.PrivilegeRead,
	"mutationResolver.ClearAll":                cypher.PrivilegeRead | cypher.PrivilegeWrite | cypher.PrivilegeSharedState,
	"nodeResolver.Relationships":               cypher.PrivilegeRead,
	"nodeResolver.Outgoing":                    cypher.PrivilegeRead,
	"nodeResolver.Incoming":                    cypher.PrivilegeRead,
	"nodeResolver.Neighbors":                   cypher.PrivilegeRead,
	"nodeResolver.Similar":                     cypher.PrivilegeRead,
	"queryResolver.Node":                       cypher.PrivilegeRead,
	"queryResolver.Nodes":                      cypher.PrivilegeRead,
	"queryResolver.AllNodes":                   cypher.PrivilegeRead,
	"queryResolver.NodesByLabel":               cypher.PrivilegeRead,
	"queryResolver.NodeCount":                  cypher.PrivilegeRead,
	"queryResolver.Relationship":               cypher.PrivilegeRead,
	"queryResolver.AllRelationships":           cypher.PrivilegeRead,
	"queryResolver.RelationshipsByType":        cypher.PrivilegeRead,
	"queryResolver.RelationshipsBetween":       cypher.PrivilegeRead,
	"queryResolver.RelationshipCount":          cypher.PrivilegeRead,
	"queryResolver.Search":                     cypher.PrivilegeRead,
	"queryResolver.Similar":                    cypher.PrivilegeRead,
	"queryResolver.SearchByProperty":           cypher.PrivilegeRead,
	"queryResolver.Cypher":                     cypher.PrivilegeNone,
	"queryResolver.Stats":                      cypher.PrivilegeRead,
	"queryResolver.Schema":                     cypher.PrivilegeRead,
	"queryResolver.Labels":                     cypher.PrivilegeRead,
	"queryResolver.RelationshipTypes":          cypher.PrivilegeRead,
	"queryResolver.ShortestPath":               cypher.PrivilegeRead,
	"queryResolver.AllPaths":                   cypher.PrivilegeRead,
	"queryResolver.Neighborhood":               cypher.PrivilegeRead,
	"relationshipResolver.StartNode":           cypher.PrivilegeRead,
	"relationshipResolver.EndNode":             cypher.PrivilegeRead,
	"subscriptionResolver.NodeCreated":         cypher.PrivilegeRead,
	"subscriptionResolver.NodeUpdated":         cypher.PrivilegeRead,
	"subscriptionResolver.NodeDeleted":         cypher.PrivilegeRead,
	"subscriptionResolver.RelationshipCreated": cypher.PrivilegeRead,
	"subscriptionResolver.RelationshipUpdated": cypher.PrivilegeRead,
	"subscriptionResolver.RelationshipDeleted": cypher.PrivilegeRead,
	"subscriptionResolver.SearchStream":        cypher.PrivilegeRead,
}

type subscriptionDatabaseKey struct{}
type subscriptionDatabase struct{ selected, defaultDatabase string }

func (r *Resolver) admitField(ctx context.Context, field string) (context.Context, error) {
	class := resolverClasses[field] // zero is invalid and refused by the owner
	if class == cypher.PrivilegeNone {
		return ctx, nil
	}
	ctx, database, err := cypher.AdmitRequestOperation(ctx, r.dbManager.DefaultDatabaseName(), "", class)
	if err != nil {
		return ctx, err
	}
	// These methods operate on the base DB's embedding/index services. The
	// selected graph's permission cannot authorize effects on another graph.
	if class.Has(cypher.PrivilegeSharedState) || field == "mutationResolver.RunDecay" {
		base := r.dbManager.DefaultDatabaseName()
		if scoped, ok := r.DB.GetStorage().(interface{ Namespace() string }); ok {
			base = scoped.Namespace()
		}
		if _, _, err := cypher.AdmitRequestOperation(ctx, base, base, class); err != nil {
			return ctx, err
		}
		if field == "mutationResolver.TriggerEmbedding" || field == "mutationResolver.ClearAll" {
			for _, target := range cypher.PhysicalUserDatabases(&graphqlDatabaseManagerAdapter{manager: r.dbManager}, r.DB.GetBaseStorageForManager(), base) {
				if _, _, err := cypher.AdmitRequestOperation(ctx, base, target, class); err != nil {
					return ctx, err
				}
			}
		}
	}
	if strings.HasPrefix(field, "subscriptionResolver.") {
		ctx = context.WithValue(ctx, subscriptionDatabaseKey{}, subscriptionDatabase{database, r.dbManager.DefaultDatabaseName()})
	}
	return ctx, nil
}

// eventVisible scopes live payloads using the same request resolver that
// admitted the subscription. Embedded broker users keep their shared stream.
func eventVisibility(ctx context.Context) func(string) bool {
	scope, scoped := ctx.Value(subscriptionDatabaseKey{}).(subscriptionDatabase)
	allows := auth.RequestEntitlementResolverFromContext(ctx)
	if !scoped || allows == nil {
		return func(string) bool { return true }
	}
	return func(id string) bool {
		database := scope.defaultDatabase
		if prefix, _, ok := strings.Cut(id, ":"); ok {
			database = prefix
		}
		return database == scope.selected && allows(database, auth.PermRead)
	}
}
