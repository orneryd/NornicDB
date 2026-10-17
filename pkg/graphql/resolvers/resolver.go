package resolvers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

// Resolver is the root resolver for the GraphQL schema.
// All operations use namespaced storage to ensure proper database isolation.
type Resolver struct {
	// DB is used only for admin operations (embeddings, search stats, etc.)
	// All node/edge operations use namespaced storage via dbManager.
	DB        *nornicdb.DB
	dbManager *multidb.DatabaseManager // Required: provides namespaced storage for all operations
	StartTime time.Time

	// EventBroker manages GraphQL subscriptions
	EventBroker *EventBroker
}

// NewResolver creates a new resolver that uses the default database's
// namespaced storage engine. This ensures all operations executed via GraphQL
// use the correct database namespace.
//
// Parameters:
//   - db: Base database instance (used for admin operations)
//   - dbManager: Database manager (required for namespaced storage)
func NewResolver(db *nornicdb.DB, dbManager *multidb.DatabaseManager) *Resolver {
	if dbManager == nil {
		panic("dbManager is required for GraphQL resolver")
	}

	// Create event broker for GraphQL subscriptions
	broker := NewEventBroker()

	resolver := &Resolver{
		DB:          db,
		dbManager:   dbManager,
		StartTime:   time.Now(),
		EventBroker: broker,
	}

	// Wire up storage event callbacks to event broker
	// This enables GraphQL subscriptions to receive real-time updates
	// Get the underlying storage engine and unwrap layers (NamespacedEngine, WALEngine)
	// to reach the BadgerEngine that implements StorageEventNotifier
	// Events from all namespaces are published, each with its database (the
	// storage namespace), which scoped subscriptions filter on.
	if db != nil {
		underlyingEngine := db.GetStorage()

		// Unwrap NamespacedEngine if present (from db.GetStorage())
		if namespacedEngine, ok := underlyingEngine.(*storage.NamespacedEngine); ok {
			underlyingEngine = namespacedEngine.GetInnerEngine()
		}

		// Unwrap WALEngine if present
		if walEngine, ok := underlyingEngine.(storage.EngineUnwrapper); ok {
			underlyingEngine = walEngine.GetInnerEngine()
		}

		if notifier, ok := underlyingEngine.(storage.StorageEventNotifier); ok {
			// Get default database name for namespace prefix removal
			defaultDBName := dbManager.DefaultDatabaseName()
			namespacePrefix := defaultDBName + ":"

			// Helper to unprefix node ID
			unprefixNodeID := func(id storage.NodeID) storage.NodeID {
				idStr := string(id)
				if strings.HasPrefix(idStr, namespacePrefix) {
					return storage.NodeID(idStr[len(namespacePrefix):])
				}
				return id
			}

			// Helper to unprefix edge ID
			unprefixEdgeID := func(id storage.EdgeID) storage.EdgeID {
				idStr := string(id)
				if strings.HasPrefix(idStr, namespacePrefix) {
					return storage.EdgeID(idStr[len(namespacePrefix):])
				}
				return id
			}

			// eventDatabase is the database a stored ID belongs to: its
			// storage namespace, which the engine (not the user) puts before
			// the first ':'; an ID without one is the default database's.
			// Subscribers are scoped by it, never by the published ID, which
			// a user can give any text, including another database's prefix.
			eventDatabase := func(id string) string {
				if namespace, _, ok := strings.Cut(id, ":"); ok {
					return namespace
				}
				return defaultDBName
			}

			// Node events
			notifier.OnNodeCreated(func(node *storage.Node) {
				// Create a copy with unprefixed ID for GraphQL
				unprefixedNode := *storage.NewNormalEventResultProjection(underlyingEngine).Node(node)
				unprefixedNode.ID = unprefixNodeID(node.ID)
				broker.PublishNodeCreatedIn(eventDatabase(string(node.ID)), storageNodeToModel(&unprefixedNode))
			})
			notifier.OnNodeUpdated(func(node *storage.Node) {
				unprefixedNode := *storage.NewNormalEventResultProjection(underlyingEngine).Node(node)
				unprefixedNode.ID = unprefixNodeID(node.ID)
				broker.PublishNodeUpdatedIn(eventDatabase(string(node.ID)), storageNodeToModel(&unprefixedNode))
			})
			notifier.OnNodeDeleted(func(nodeID storage.NodeID) {
				unprefixedID := unprefixNodeID(nodeID)
				broker.PublishNodeDeletedIn(eventDatabase(string(nodeID)), string(unprefixedID))
			})

			// Relationship (edge) events
			notifier.OnEdgeCreated(func(edge *storage.Edge) {
				unprefixedEdge := *storage.NewNormalEventResultProjection(underlyingEngine).Edge(edge)
				unprefixedEdge.ID = unprefixEdgeID(edge.ID)
				unprefixedEdge.StartNode = unprefixNodeID(edge.StartNode)
				unprefixedEdge.EndNode = unprefixNodeID(edge.EndNode)
				broker.PublishRelationshipCreatedIn(eventDatabase(string(edge.ID)), storageEdgeToModel(&unprefixedEdge))
			})
			notifier.OnEdgeUpdated(func(edge *storage.Edge) {
				unprefixedEdge := *storage.NewNormalEventResultProjection(underlyingEngine).Edge(edge)
				unprefixedEdge.ID = unprefixEdgeID(edge.ID)
				unprefixedEdge.StartNode = unprefixNodeID(edge.StartNode)
				unprefixedEdge.EndNode = unprefixNodeID(edge.EndNode)
				broker.PublishRelationshipUpdatedIn(eventDatabase(string(edge.ID)), storageEdgeToModel(&unprefixedEdge))
			})
			notifier.OnEdgeDeleted(func(edgeID storage.EdgeID) {
				unprefixedID := unprefixEdgeID(edgeID)
				broker.PublishRelationshipDeletedIn(eventDatabase(string(edgeID)), string(unprefixedID))
			})
		}
	}

	return resolver
}

// getCypherExecutor returns the Cypher executor using the specified database's namespaced storage.
// If database is empty, uses the configured default database (typically "nornic").
// Enforces per-database RBAC: if request context has DatabaseAccessMode, checks CanAccessDatabase(effectiveDb).
func (r *Resolver) getCypherExecutor(ctx context.Context, database string) (*cypher.StorageExecutor, error) {
	effectiveDb, err := r.resolveCypherDatabase(ctx, database)
	if err != nil {
		return nil, err
	}
	mode := auth.RequestDatabaseAccessModeFromContext(ctx)
	if mode != nil && !mode.CanAccessDatabase(effectiveDb) {
		return nil, fmt.Errorf("access to database '%s' is not allowed", effectiveDb)
	}

	storage, err := r.dbManager.GetStorage(effectiveDb)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found: %w", effectiveDb, err)
	}

	executor := cypher.NewStatementExecutor(storage)
	executor.SetDatabaseManager(&graphqlDatabaseManagerAdapter{manager: r.dbManager})

	// Copy configuration from base DB's executor if available
	if r.DB != nil {
		baseExecutor := r.DB.GetCypherExecutor()
		if baseExecutor != nil {
			executor.SetLocalizationRenderer(baseExecutor.GetLocalizationRenderer())
			// Copy embedder for vector search queries (CALL db.index.vector.queryNodes)
			// This ensures GraphQL queries can use string-based vector search
			executor.ShareEmbedderFrom(baseExecutor)
		}

		// Wire up embedding callback to use the base DB's embed queue
		// This ensures nodes created via GraphQL get automatically queued for embedding
		embedQueue := r.DB.GetEmbedQueue()
		if embedQueue != nil {
			executor.SetNodeMutatedCallback(func(nodeID string) {
				embedQueue.Enqueue(nodeID)
			})
		}
	}
	return executor, nil
}

func (r *Resolver) resolveCypherDatabase(ctx context.Context, database string) (string, error) {
	_, selected, err := cypher.AdmitRequestOperation(ctx, r.dbManager.DefaultDatabaseName(), database, cypher.PrivilegeNone)
	if err != nil {
		return "", err
	}
	if auth.RequestDatabaseScopeFromContext(ctx) != nil {
		return selected, nil
	}
	resolved, err := r.dbManager.ResolveDatabase(selected)
	if err != nil {
		return "", fmt.Errorf("database '%s' not found: %w", database, err)
	}
	return resolved, nil
}

// readDatabase is the database a search or similarity request reads and its
// storage: the database the request selects, resolved as executeCypher
// resolves it (resolveCypherDatabase), on which the principal must hold read
// (the request's entitlement resolver), as a Cypher read there needs.
func (r *Resolver) readDatabase(ctx context.Context) (string, storage.Engine, error) {
	// Admission's refusal stands as given: a database the principal may not
	// use is refused with Neo4j's database-access text, a missing read with
	// the read refusal.
	_, database, err := cypher.AdmitRequestOperation(ctx, r.dbManager.DefaultDatabaseName(), "", cypher.PrivilegeRead)
	if err != nil {
		return "", nil, err
	}
	// A composite's storage reads every constituent, which this one decision
	// on the composite does not cover; the REST search and similarity routes
	// refuse it the same way.
	if r.dbManager.IsCompositeDatabase(database) {
		return "", nil, fmt.Errorf("search on composite database '%s' is not supported; target a constituent database explicitly", database)
	}
	engine, err := r.dbManager.GetStorage(database)
	if err != nil {
		return "", nil, fmt.Errorf("database '%s' not found: %w", database, err)
	}
	return database, engine, nil
}

// executeCypher executes a Cypher query using the specified database's namespaced storage.
// If database is empty, uses the default database.
// Authorization is derived from the Cypher statement and enforced by the executor.
func (r *Resolver) executeCypher(ctx context.Context, query string, params map[string]interface{}, database string) (*nornicdb.CypherResult, error) {
	effectiveDb, err := r.resolveCypherDatabase(ctx, database)
	if err != nil {
		return nil, err
	}
	// The principal's one permission resolver (cypher.PrincipalAccess, which
	// the server attaches to the request): the executor authorizes the
	// statement and every database it selects with it, and the principal's
	// boosted procedure grants come with the request too, as on HTTP and
	// Bolt.
	if allows := auth.RequestEntitlementResolverFromContext(ctx); allows != nil {
		ctx = cypher.WithDatabasePermissionResolver(ctx, effectiveDb, func(database, permission string) bool {
			return allows(database, auth.Permission(permission))
		})
	}
	if err := cypher.AdmitStatement(ctx, &graphqlDatabaseManagerAdapter{manager: r.dbManager}, effectiveDb, query); err != nil {
		return nil, err
	}
	executor, err := r.getCypherExecutor(ctx, effectiveDb)
	if err != nil {
		return nil, err
	}

	result, err := executor.Execute(cypher.WithClientStatement(ctx), query, params)
	if err != nil {
		return nil, err
	}

	return &nornicdb.CypherResult{
		Columns: result.Columns,
		Rows:    result.Rows,
	}, nil
}

// getStorage returns the default database's namespaced storage engine.
func (r *Resolver) getStorage() (storage.Engine, error) {
	return r.dbManager.GetDefaultStorage()
}
