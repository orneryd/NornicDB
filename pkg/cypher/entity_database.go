package cypher

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// EntityDatabaseResolver locates a result entity's physical database. It does
// not change the entity or confer permission to read another graph.
type EntityDatabaseResolver func(storage.NodeID, storage.EdgeID) string

// NewEntityDatabaseResolver binds result attribution to the executing context
// for a single lookup: each call resolves one entity and releases what it
// opened (NewEntityDatabaseAttribution). Non-composite results need none.
func NewEntityDatabaseResolver(ctx context.Context, manager DatabaseManagerInterface, database string, reportCleanup func(string, error)) EntityDatabaseResolver {
	attribution := NewEntityDatabaseAttribution(ctx, manager, database, reportCleanup)
	if attribution == nil {
		return nil
	}
	return func(nodeID storage.NodeID, edgeID storage.EdgeID) string {
		defer attribution.Release()
		return attribution.Resolve(nodeID, edgeID)
	}
}

// EntityDatabaseAttribution attributes one result's entities to their
// physical databases. Construction reads metadata only. The constituent
// lookup is built on the first entity and kept, each answer is kept per
// entity, and Release closes the remote engines the lookup opened (each
// constituent is admitted before its storage, with execution's Read policy,
// and opened at most once until then); a later entity rebuilds the lookup.
// A serializer resolves a result's entities and releases once, so a result
// with N entities over C constituents opens each constituent once, not N
// times. It is safe for concurrent use.
type EntityDatabaseAttribution struct {
	selection string
	build     func() (lookup *storage.CompositeEngine, release func())

	mu      sync.Mutex
	lookup  *storage.CompositeEngine
	release func()
	nodes   map[storage.NodeID]string
	edges   map[storage.EdgeID]string
}

// NewEntityDatabaseAttribution is the attribution of a result of database
// for the executing context, or nil when database isn't a composite (its
// entities are all its own) or can't be attributed.
func NewEntityDatabaseAttribution(ctx context.Context, manager DatabaseManagerInterface, database string, reportCleanup func(string, error)) *EntityDatabaseAttribution {
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
	build := func() (*storage.CompositeEngine, func()) {
		type resource struct {
			database string
			owner    *ownedStorageCleanup
		}
		var resourcesMu sync.Mutex
		var resources []resource
		release := func() {
			resourcesMu.Lock()
			acquired := resources
			resources = nil
			resourcesMu.Unlock()
			for _, each := range acquired {
				each.owner.release(func(err error) { reportCleanup(each.database, err) })
			}
		}
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
					resourcesMu.Lock()
					resources = append(resources, resource{graph, &ownedStorageCleanup{close: engine.Close}})
					resourcesMu.Unlock()
				}
				return engine, nil
			}
		}
		return storage.NewDeferredCompositeEngine(factories, names, modes), release
	}
	return &EntityDatabaseAttribution{selection: selection, build: build}
}

// Resolve is the physical database of the node nodeID or, with no node, the
// relationship edgeID; the selected database when none holds it.
func (a *EntityDatabaseAttribution) Resolve(nodeID storage.NodeID, edgeID storage.EdgeID) string {
	if nodeID == "" && edgeID == "" {
		return a.selection
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if nodeID != "" {
		if found, cached := a.nodes[nodeID]; cached {
			return found
		}
	} else if found, cached := a.edges[edgeID]; cached {
		return found
	}
	if a.lookup == nil {
		a.lookup, a.release = a.build()
	}
	var found string
	if nodeID != "" {
		found = a.lookup.ConstituentDatabaseForNode(nodeID)
	} else {
		found = a.lookup.ConstituentDatabaseForEdge(edgeID)
	}
	if found == "" {
		found = a.selection
	}
	if nodeID != "" {
		if a.nodes == nil {
			a.nodes = make(map[storage.NodeID]string)
		}
		a.nodes[nodeID] = found
	} else {
		if a.edges == nil {
			a.edges = make(map[storage.EdgeID]string)
		}
		a.edges[edgeID] = found
	}
	return found
}

// Release closes the remote engines the lookup opened and drops the lookup;
// the answers already found are kept. A nil attribution releases nothing.
func (a *EntityDatabaseAttribution) Release() {
	if a == nil {
		return
	}
	a.mu.Lock()
	release := a.release
	a.lookup, a.release = nil, nil
	a.mu.Unlock()
	if release != nil {
		release()
	}
}
