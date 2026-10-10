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
	// Note: Events from all namespaces will be published. Namespace filtering
	// can be added later if needed by checking node/edge ID prefixes.
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

			// Node events
			notifier.OnNodeCreated(func(node *storage.Node) {
				// Create a copy with unprefixed ID for GraphQL
				unprefixedNode := *node
				unprefixedNode.ID = unprefixNodeID(node.ID)
				broker.PublishNodeCreated(storageNodeToModel(&unprefixedNode))
			})
			notifier.OnNodeUpdated(func(node *storage.Node) {
				unprefixedNode := *node
				unprefixedNode.ID = unprefixNodeID(node.ID)
				broker.PublishNodeUpdated(storageNodeToModel(&unprefixedNode))
			})
			notifier.OnNodeDeleted(func(nodeID storage.NodeID) {
				unprefixedID := unprefixNodeID(nodeID)
				broker.PublishNodeDeleted(string(unprefixedID))
			})

			// Relationship (edge) events
			notifier.OnEdgeCreated(func(edge *storage.Edge) {
				unprefixedEdge := *edge
				unprefixedEdge.ID = unprefixEdgeID(edge.ID)
				unprefixedEdge.StartNode = unprefixNodeID(edge.StartNode)
				unprefixedEdge.EndNode = unprefixNodeID(edge.EndNode)
				broker.PublishRelationshipCreated(storageEdgeToModel(&unprefixedEdge))
			})
			notifier.OnEdgeUpdated(func(edge *storage.Edge) {
				unprefixedEdge := *edge
				unprefixedEdge.ID = unprefixEdgeID(edge.ID)
				unprefixedEdge.StartNode = unprefixNodeID(edge.StartNode)
				unprefixedEdge.EndNode = unprefixNodeID(edge.EndNode)
				broker.PublishRelationshipUpdated(storageEdgeToModel(&unprefixedEdge))
			})
			notifier.OnEdgeDeleted(func(edgeID storage.EdgeID) {
				unprefixedID := unprefixEdgeID(edgeID)
				broker.PublishRelationshipDeleted(string(unprefixedID))
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

	executor := cypher.NewStorageExecutor(storage)
	executor.SetDatabaseManager(&graphqlDatabaseManagerAdapter{manager: r.dbManager})

	// Copy configuration from base DB's executor if available
	if r.DB != nil {
		baseExecutor := r.DB.GetCypherExecutor()
		if baseExecutor != nil {
			executor.SetLocalizationRenderer(baseExecutor.GetLocalizationRenderer())
			// Copy embedder for vector search queries (CALL db.index.vector.queryNodes)
			// This ensures GraphQL queries can use string-based vector search
			if embedder := baseExecutor.GetEmbedder(); embedder != nil {
				executor.SetEmbedder(embedder)
			}
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
	if scope := auth.RequestDatabaseScopeFromContext(ctx); scope != nil {
		resolved, allowed := scope.Resolve(database)
		if !allowed {
			return "", fmt.Errorf("access to database '%s' is not allowed", database)
		}
		return resolved, nil
	}
	if database == "" {
		return r.dbManager.DefaultDatabaseName(), nil
	}
	resolved, err := r.dbManager.ResolveDatabase(database)
	if err != nil {
		return "", fmt.Errorf("database '%s' not found: %w", database, err)
	}
	return resolved, nil
}

// executeCypher executes a Cypher query using the specified database's namespaced storage.
// If database is empty, uses the default database.
// Authorization is derived from the Cypher statement and enforced by the executor.
func (r *Resolver) executeCypher(ctx context.Context, query string, params map[string]interface{}, database string) (*nornicdb.CypherResult, error) {
	effectiveDb, err := r.resolveCypherDatabase(ctx, database)
	if err != nil {
		return nil, err
	}
	if entitlementResolver := auth.RequestEntitlementResolverFromContext(ctx); entitlementResolver != nil {
		ctx = cypher.WithDatabasePermissionResolver(ctx, effectiveDb, func(database, permission string) bool {
			if scope := auth.RequestDatabaseScopeFromContext(ctx); scope != nil {
				resolved, allowed := scope.Resolve(database)
				if !allowed {
					return false
				}
				database = resolved
			}
			return entitlementResolver(database, auth.Permission(permission))
		})
	} else if accessResolver := auth.RequestResolvedAccessResolverFromContext(ctx); accessResolver != nil {
		ctx = cypher.WithDatabasePermissionResolver(ctx, effectiveDb, func(database, permission string) bool {
			if scope := auth.RequestDatabaseScopeFromContext(ctx); scope != nil {
				resolved, allowed := scope.Resolve(database)
				if !allowed {
					return false
				}
				database = resolved
			}
			access := accessResolver(database)
			switch auth.Permission(permission) {
			case auth.PermRead:
				return access.Read
			case auth.PermWrite:
				return access.Write
			default:
				return false
			}
		})
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
