package cypher

import (
	"context"
	"log/slog"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// EntityDatabaseResolver locates a result entity's physical database. It does
// not change the entity or confer permission to read another graph.
type EntityDatabaseResolver func(storage.NodeID, storage.EdgeID) string

// NewEntityDatabaseResolver binds result attribution to the executing context.
// Construction reads metadata only. Each candidate is admitted before storage,
// using the same Read policy as execution. The storage identity reader remains
// canonical, and private remote lookup resources use the same cleanup owner as
// request executors. Non-composite results need no storage lookup.
func NewEntityDatabaseResolver(ctx context.Context, manager DatabaseManagerInterface, database string, reportCleanup func(string, error)) EntityDatabaseResolver {
	if manager == nil {
		return nil
	}
	selection := database
	if canonical, err := manager.ResolveDatabase(database); err == nil {
		database = canonical
	}
	if !manager.IsCompositeDatabase(database) || admitGraphImpersonation(ctx, manager, database) != nil {
		return nil
	}
	constituents, err := manager.GetCompositeConstituents(database)
	if err != nil {
		return nil
	}
	var allows DatabasePermissionResolver
	if authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization); ok {
		allows = authorization.resolver
	} else if check := contextPermissions(ctx); check != nil {
		allows = func(_ string, permission string) bool { return check(permission) }
	}
	if reportCleanup == nil {
		reportCleanup = func(database string, err error) {
			var localizer *localization.Manager
			localizer.Log(ctx, slog.Default(), slog.LevelError, localization.OperationStorageCleanupFailedEvent(database, err))
		}
	}
	return func(nodeID storage.NodeID, edgeID storage.EdgeID) string {
		if nodeID == "" && edgeID == "" {
			return selection
		}
		type resource struct {
			database string
			owner    *ownedStorageCleanup
		}
		var resources []resource
		defer func() {
			for _, acquired := range resources {
				acquired.owner.release(func(err error) { reportCleanup(acquired.database, err) })
			}
		}()
		factories := make(map[string]func() (storage.Engine, error), len(constituents))
		names := make(map[string]string, len(constituents))
		modes := make(map[string]string, len(constituents))
		for _, raw := range constituents {
			ref, valid := toConstituentRef(raw)
			if !valid {
				continue
			}
			graph := database + "." + ref.Alias
			name := ref.DatabaseName
			if !strings.EqualFold(ref.Type, "remote") {
				if canonical, err := manager.ResolveDatabase(name); err == nil {
					name = canonical
				}
			}
			names[ref.Alias], modes[ref.Alias] = name, ref.AccessMode
			factories[ref.Alias] = func() (storage.Engine, error) {
				if err := admitGraphImpersonation(ctx, manager, graph); err != nil {
					return nil, err
				}
				if err := AdmitOperation(ctx, allows, graph, PrivilegeRead); err != nil {
					return nil, err
				}
				value, err := manager.GetStorageForUse(graph, GetAuthTokenFromContext(ctx))
				if err != nil {
					return nil, err
				}
				engine, valid := value.(storage.Engine)
				if !valid {
					return nil, localizedError(localization.CypherCommandRoutingFabricStorageTypeInvalid(graph), nil)
				}
				if strings.EqualFold(ref.Type, "remote") {
					resources = append(resources, resource{graph, &ownedStorageCleanup{close: engine.Close}})
				}
				return engine, nil
			}
		}
		lookup := storage.NewDeferredCompositeEngine(factories, names, modes)
		var found string
		if nodeID != "" {
			found = lookup.ConstituentDatabaseForNode(nodeID)
		} else {
			found = lookup.ConstituentDatabaseForEdge(edgeID)
		}
		if found != "" {
			return found
		}
		return selection
	}
}
