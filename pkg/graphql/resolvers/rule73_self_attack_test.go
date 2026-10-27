package resolvers

import (
	"github.com/orneryd/nornicdb/pkg/multidb"

	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/stretchr/testify/require"
)

type rule73GraphQLClass string

const (
	rule73Read      rule73GraphQLClass = "read"
	rule73Write     rule73GraphQLClass = "write"
	rule73ReadWrite rule73GraphQLClass = "read_write"
	rule73Admin     rule73GraphQLClass = "admin"
	rule73Global    rule73GraphQLClass = "global_worker"
)

func rule73GraphQLInventory() map[string]rule73GraphQLClass {
	classes := map[string]rule73GraphQLClass{}
	add := func(class rule73GraphQLClass, names ...string) {
		for _, name := range names {
			classes[name] = class
		}
	}
	add(rule73Read,
		"nodeResolver.Relationships", "nodeResolver.Outgoing", "nodeResolver.Incoming", "nodeResolver.Neighbors", "nodeResolver.Similar",
		"relationshipResolver.StartNode", "relationshipResolver.EndNode",
		"queryResolver.Node", "queryResolver.Nodes", "queryResolver.AllNodes", "queryResolver.NodesByLabel", "queryResolver.NodeCount",
		"queryResolver.Relationship", "queryResolver.AllRelationships", "queryResolver.RelationshipsByType", "queryResolver.RelationshipsBetween", "queryResolver.RelationshipCount",
		"queryResolver.Search", "queryResolver.Similar", "queryResolver.SearchByProperty", "queryResolver.Cypher", "queryResolver.Stats", "queryResolver.Schema",
		"queryResolver.Labels", "queryResolver.RelationshipTypes", "queryResolver.ShortestPath", "queryResolver.AllPaths", "queryResolver.Neighborhood",
		"mutationResolver.RunDecay",
		"subscriptionResolver.NodeCreated", "subscriptionResolver.NodeUpdated", "subscriptionResolver.NodeDeleted",
		"subscriptionResolver.RelationshipCreated", "subscriptionResolver.RelationshipUpdated", "subscriptionResolver.RelationshipDeleted", "subscriptionResolver.SearchStream",
	)
	add(rule73Write, "mutationResolver.CreateNode", "mutationResolver.BulkCreateNodes")
	add(rule73ReadWrite,
		"mutationResolver.UpdateNode", "mutationResolver.DeleteNode", "mutationResolver.BulkDeleteNodes", "mutationResolver.MergeNode",
		"mutationResolver.CreateRelationship", "mutationResolver.UpdateRelationship", "mutationResolver.DeleteRelationship",
		"mutationResolver.BulkCreateRelationships", "mutationResolver.BulkDeleteRelationships", "mutationResolver.MergeRelationship",
		"mutationResolver.ExecuteCypher", "mutationResolver.RebuildSearchIndex",
	)
	add(rule73Admin, "mutationResolver.ClearAll")
	add(rule73Global, "mutationResolver.TriggerEmbedding")
	return classes
}

func rule73GraphQLContext(defaultDatabase string, selections map[string]string, read, write, admin bool) context.Context {
	ctx := auth.WithRequestDatabaseScope(context.Background(), auth.NewRequestDatabaseScope(defaultDatabase, selections))
	ctx = auth.WithRequestDatabaseAccessMode(ctx, auth.FullDatabaseAccessMode)
	ctx = auth.WithRequestResolvedAccessResolver(ctx, func(string) auth.ResolvedAccess {
		return auth.ResolvedAccess{Read: read, Write: write}
	})
	return auth.WithRequestEntitlementResolver(ctx, func(_ string, permission auth.Permission) bool {
		switch permission {
		case auth.PermRead:
			return read
		case auth.PermWrite, auth.PermCreate, auth.PermDelete:
			return write
		case auth.PermAdmin:
			return admin
		default:
			return false
		}
	})
}

func rule73Denied(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "forbidden") || strings.Contains(message, "permission") || strings.Contains(message, "access denied") || strings.Contains(message, "not allowed")
}

func rule73Arg(t reflect.Type) reflect.Value {
	if t == reflect.TypeOf(models.CypherInput{}) {
		return reflect.ValueOf(models.CypherInput{Statement: "MATCH (n) RETURN n LIMIT 1"})
	}
	if t == reflect.TypeOf(&models.Node{}) {
		return reflect.ValueOf(&models.Node{ID: "missing-node"})
	}
	if t == reflect.TypeOf(&models.Relationship{}) {
		return reflect.ValueOf(&models.Relationship{ID: "missing-edge", StartNodeID: "missing-a", EndNodeID: "missing-b"})
	}
	if t.Kind() == reflect.String {
		return reflect.ValueOf("missing").Convert(t)
	}
	return reflect.Zero(t)
}

func rule73Call(ctx context.Context, bound reflect.Value) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic before admission: %v", recovered)
		}
	}()
	args := []reflect.Value{reflect.ValueOf(ctx)}
	for i := 1; i < bound.Type().NumIn(); i++ {
		args = append(args, rule73Arg(bound.Type().In(i)))
	}
	results := bound.Call(args)
	if len(results) == 0 {
		return nil
	}
	last := results[len(results)-1]
	if last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) && !last.IsNil() {
		return last.Interface().(error)
	}
	return nil
}

func TestRule73SelfAttackEveryGraphQLDataResolver(t *testing.T) {
	db := testDB(t)
	manager := testutil.DatabaseManager(t, db)
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "rule73_named"}))
	resolver := NewResolver(db, manager)
	t.Cleanup(resolver.EventBroker.Close)

	profiles := []struct {
		name    string
		ctx     context.Context
		allowed func(rule73GraphQLClass) bool
	}{
		{"default-only", rule73GraphQLContext("nornic", map[string]string{"nornic": "nornic"}, true, true, false), func(class rule73GraphQLClass) bool { return class != rule73Admin && class != rule73Global }},
		{"named-only", rule73GraphQLContext("", map[string]string{"rule73_named": "rule73_named"}, true, true, false), func(class rule73GraphQLClass) bool { return class != rule73Admin && class != rule73Global }},
		{"write-only-default", rule73GraphQLContext("nornic", map[string]string{"nornic": "nornic"}, false, true, false), func(class rule73GraphQLClass) bool { return class == rule73Write }},
		{"write-only-named", rule73GraphQLContext("", map[string]string{"rule73_named": "rule73_named"}, false, true, false), func(class rule73GraphQLClass) bool { return class == rule73Write }},
		{"no-access", rule73GraphQLContext("", nil, false, false, false), func(rule73GraphQLClass) bool { return false }},
		{"admin", rule73GraphQLContext("nornic", map[string]string{"nornic": "nornic", "rule73_named": "rule73_named", "system": "system"}, true, true, true), func(rule73GraphQLClass) bool { return true }},
		{"auth-off", auth.WithRequestAuthenticationDisabled(context.Background()), func(rule73GraphQLClass) bool { return true }},
	}

	classes := rule73GraphQLInventory()
	seen := map[string]bool{}
	for _, root := range []any{resolver.Mutation(), resolver.Node(), resolver.Query(), resolver.Relationship(), resolver.Subscription()} {
		value := reflect.ValueOf(root)
		kind := value.Elem().Type().Name()
		for i := 0; i < value.NumMethod(); i++ {
			method := value.Type().Method(i)
			if method.Type.NumIn() < 2 || method.Type.In(1) != reflect.TypeOf((*context.Context)(nil)).Elem() {
				continue
			}
			name := kind + "." + method.Name
			class, classified := classes[name]
			require.True(t, classified, "unclassified GraphQL resolver %s", name)
			seen[name] = true
			for _, profile := range profiles {
				t.Run(name+"/"+profile.name, func(t *testing.T) {
					ctx, cancel := context.WithCancel(profile.ctx)
					defer cancel()
					err := rule73Call(ctx, value.Method(i))
					denied := rule73Denied(err)
					if profile.allowed(class) {
						require.False(t, denied, "legitimate caller over-denied: %v", err)
					} else {
						require.True(t, denied, "data resolver bypassed admission: %v", err)
					}
				})
			}
		}
	}
	require.Equal(t, len(classes), len(seen), "inventory includes a resolver the generated public interfaces do not expose")
}
