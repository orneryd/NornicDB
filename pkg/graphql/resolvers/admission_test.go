package resolvers

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func gqlPrincipal(database string, selections map[string]string, read, write, admin bool) context.Context {
	return auth.WithRequestPrincipal(context.Background(), func(_ string, permission auth.Permission) bool {
		switch string(permission) {
		case "access":
			return true
		case "read":
			return read
		case "write":
			return write
		case "admin":
			return admin
		}
		return false
	}, auth.NewRequestDatabaseScope(database, selections))
}

func TestEveryGraphQLResolverHasExpectedClassAndAdmission(t *testing.T) {
	db := testDB(t)
	manager := testDBManager(t, db)
	r := NewResolver(db, manager)
	t.Cleanup(r.EventBroker.Close)
	expected := map[cypher.PrivilegeClass][]string{
		cypher.PrivilegeNone:                         {"cypherResultResolver.Rows", "queryResolver.Cypher", "mutationResolver.ExecuteCypher"},
		cypher.PrivilegeWrite:                        {"mutationResolver.CreateNode", "mutationResolver.BulkCreateNodes"},
		cypher.PrivilegeRead | cypher.PrivilegeWrite: {"mutationResolver.UpdateNode", "mutationResolver.DeleteNode", "mutationResolver.BulkDeleteNodes", "mutationResolver.MergeNode", "mutationResolver.CreateRelationship", "mutationResolver.UpdateRelationship", "mutationResolver.DeleteRelationship", "mutationResolver.BulkCreateRelationships", "mutationResolver.BulkDeleteRelationships", "mutationResolver.MergeRelationship"},
		cypher.PrivilegeRead | cypher.PrivilegeWrite | cypher.PrivilegeSharedState: {"mutationResolver.TriggerEmbedding", "mutationResolver.ClearAll"},
		cypher.PrivilegeRead | cypher.PrivilegeSharedState:                         {"mutationResolver.RebuildSearchIndex"},
		cypher.PrivilegeRead: {
			"mutationResolver.RunDecay", "nodeResolver.Relationships", "nodeResolver.Outgoing", "nodeResolver.Incoming", "nodeResolver.Neighbors", "nodeResolver.Similar",
			"queryResolver.Node", "queryResolver.Nodes", "queryResolver.AllNodes", "queryResolver.NodesByLabel", "queryResolver.NodeCount", "queryResolver.Relationship", "queryResolver.AllRelationships", "queryResolver.RelationshipsByType", "queryResolver.RelationshipsBetween", "queryResolver.RelationshipCount", "queryResolver.Search", "queryResolver.Similar", "queryResolver.SearchByProperty", "queryResolver.Stats", "queryResolver.Schema", "queryResolver.Labels", "queryResolver.RelationshipTypes", "queryResolver.ShortestPath", "queryResolver.AllPaths", "queryResolver.Neighborhood",
			"relationshipResolver.StartNode", "relationshipResolver.EndNode", "subscriptionResolver.NodeCreated", "subscriptionResolver.NodeUpdated", "subscriptionResolver.NodeDeleted", "subscriptionResolver.RelationshipCreated", "subscriptionResolver.RelationshipUpdated", "subscriptionResolver.RelationshipDeleted", "subscriptionResolver.SearchStream",
		},
	}
	fixture := make(map[string]cypher.PrivilegeClass)
	for class, names := range expected {
		for _, name := range names {
			fixture[name] = class
		}
	}
	require.Equal(t, fixture, resolverClasses)
	ctx := gqlPrincipal("", nil, false, false, false)
	seen := make(map[string]bool)
	// Enumerate the actual public resolver interfaces and exercise their entry
	// points. No graph or admin implementation may run before admission.
	for _, resolver := range []any{r.CypherResult(), r.Mutation(), r.Node(), r.Query(), r.Relationship(), r.Subscription()} {
		value := reflect.ValueOf(resolver)
		kind := value.Elem().Type().Name()
		for i := 0; i < value.NumMethod(); i++ {
			method := value.Type().Method(i)
			if method.Name == "CypherResult" || method.Type.NumIn() < 2 || method.Type.In(1) != reflect.TypeOf((*context.Context)(nil)).Elem() {
				continue
			}
			name := kind + "." + method.Name
			class, ok := fixture[name]
			require.True(t, ok, name)
			seen[name] = true
			if class == cypher.PrivilegeNone {
				continue
			}
			t.Run(name, func(t *testing.T) {
				args := []reflect.Value{reflect.ValueOf(ctx)}
				bound := value.Method(i)
				for j := 1; j < bound.Type().NumIn(); j++ {
					args = append(args, reflect.Zero(bound.Type().In(j)))
				}
				results := bound.Call(args)
				err, _ := results[len(results)-1].Interface().(error)
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
			})
		}
	}
	require.Len(t, seen, len(fixture))
}

func TestGraphQLStatsAndSubscriptionUseRequestDatabase(t *testing.T) {
	db := testDB(t)
	manager := testDBManager(t, db)
	require.NoError(t, manager.CreateDatabase("gql_target"))
	r := NewResolver(db, manager)
	t.Cleanup(r.EventBroker.Close)
	base := manager.DefaultDatabaseName()
	for database, count := range map[string]int{base: 3, "gql_target": 1} {
		engine, err := manager.GetStorage(database)
		require.NoError(t, err)
		for i := 0; i < count; i++ {
			_, err = engine.CreateNode(&storage.Node{ID: storage.NodeID(string(rune('a' + i))), Labels: []string{"Scope"}})
			require.NoError(t, err)
		}
	}
	ctx := gqlPrincipal("gql_target", map[string]string{"gql_target": "gql_target"}, true, false, false)
	stats, err := r.Query().Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.NodeCount)
	count, err := r.Query().NodeCount(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	created, err := r.Subscription().NodeCreated(ctx, nil)
	require.NoError(t, err)
	deleted, err := r.Subscription().NodeDeleted(ctx, nil)
	require.NoError(t, err)
	r.EventBroker.PublishNodeCreated(&models.Node{ID: "base-local"})
	r.EventBroker.PublishNodeDeleted("base-local")
	select {
	case <-created:
		t.Fatal("foreign created event")
	default:
	}
	select {
	case <-deleted:
		t.Fatal("foreign deleted event")
	default:
	}
	r.EventBroker.PublishNodeCreated(&models.Node{ID: "gql_target:mine"})
	r.EventBroker.PublishNodeDeleted("gql_target:mine")
	require.Equal(t, "gql_target:mine", (<-created).ID)
	require.Equal(t, "gql_target:mine", <-deleted)
	_, err = r.Mutation().TriggerEmbedding(ctx, nil)
	require.Error(t, err)
	_, err = r.Mutation().RebuildSearchIndex(ctx)
	require.Error(t, err)
}
