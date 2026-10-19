package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/nornicgrpc"
	nornicpb "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/qdrantgrpc"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"google.golang.org/grpc"
)

// grpcDatabaseAccessMode keeps gRPC selections aligned with the existing graph
// selection rules, including every database read by a composite collection.
type grpcDatabaseAccessMode struct {
	auth.DatabaseAccessMode
	allows func(string) bool
}

func (m grpcDatabaseAccessMode) CanAccessDatabase(database string) bool { return m.allows(database) }

func (s *Server) grpcDatabaseTargets(database string, mode auth.DatabaseAccessMode) ([]string, bool) {
	physical, allowed := s.graphAccess(mode, database)
	if !allowed { return nil, false }
	if s.dbManager == nil { return []string{physical}, true }
	if resolved, err := s.dbManager.ResolveDatabase(physical); err == nil { physical = resolved }
	if mode != nil && !mode.CanAccessDatabase(physical) { return nil, false }
	info, err := s.dbManager.GetDatabase(physical)
	if err != nil || info.Type != "composite" { return []string{physical}, true }
	targets := []string{physical}
	for _, constituent := range info.Constituents {
		if constituent.Type != "local" { return nil, false }
		target, ok := s.graphAccess(mode, constituent.DatabaseName)
		if !ok { return nil, false }
		if resolved, err := s.dbManager.ResolveDatabase(target); err == nil { target = resolved }
		if mode != nil && !mode.CanAccessDatabase(target) { return nil, false }
		targets = append(targets, target)
	}
	return targets, true
}

func (s *Server) grpcDatabaseAccessForRoles(roles []string, database string) auth.ResolvedAccess {
	targets, allowed := s.grpcDatabaseTargets(database, nil)
	if !allowed { return auth.ResolvedAccess{} }
	access := auth.ResolvedAccess{Read: true, Write: true}
	for _, target := range targets {
		resolved := s.GetResolvedAccessForRoles(roles, target)
		access.Read = access.Read && resolved.Read
		access.Write = access.Write && resolved.Write
	}
	return access
}
func (s *Server) startQdrantGRPC() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.qdrantGRPCServer != nil && s.qdrantGRPCServer.IsRunning() {
		return nil
	}

	features := s.config.Features
	if features == nil {
		globalConfig := nornicConfig.LoadFromEnv()
		features = &globalConfig.Features
		s.config.Features = features
	}

	if !features.QdrantGRPCEnabled {
		return nil
	}

	dbName := s.dbManager.DefaultDatabaseName()
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		return fmt.Errorf("qdrant grpc: failed to get storage for database %q: %w", dbName, err)
	}

	searchSvc, err := s.db.GetOrCreateSearchService(dbName, storageEngine)
	if err != nil {
		return fmt.Errorf("qdrant grpc: failed to get search service for database %q: %w", dbName, err)
	}

	baseStorage := s.db.GetBaseStorageForManager()

	cfg := qdrantgrpc.DefaultConfig()
	cfg.Logger = s.config.Logger
	cfg.Localizer = s.localizer
	if features.QdrantGRPCListenAddr != "" {
		cfg.ListenAddr = features.QdrantGRPCListenAddr
	}
	// If NornicDB-managed embeddings are enabled, prevent Qdrant clients from
	// directly mutating stored vectors to avoid conflicting sources of truth.
	cfg.AllowVectorMutations = !s.config.EmbeddingEnabled
	if features.QdrantGRPCMaxVectorDim > 0 {
		cfg.MaxVectorDim = features.QdrantGRPCMaxVectorDim
	}
	if features.QdrantGRPCMaxBatchPoints > 0 {
		cfg.MaxBatchPoints = features.QdrantGRPCMaxBatchPoints
	}
	if features.QdrantGRPCMaxTopK > 0 {
		cfg.MaxTopK = features.QdrantGRPCMaxTopK
	}
	if features.QdrantGRPCTLSEnabled {
		clientAuthMode, err := bolt.ParseClientAuthMode(features.QdrantGRPCTLSClientAuthMode)
		if err != nil {
			return fmt.Errorf("qdrant grpc: invalid TLS client auth mode: %w", err)
		}
		if features.QdrantGRPCTLSClientCA != "" || clientAuthMode != bolt.ClientAuthNone {
			cfg.TLSConfig, err = bolt.LoadTLSConfigWithClientCA(
				features.QdrantGRPCTLSCert,
				features.QdrantGRPCTLSKey,
				features.QdrantGRPCTLSClientCA,
				clientAuthMode,
			)
		} else {
			cfg.TLSConfig, err = bolt.LoadTLSConfig(features.QdrantGRPCTLSCert, features.QdrantGRPCTLSKey)
		}
		if err != nil {
			return fmt.Errorf("qdrant grpc: failed to load TLS configuration: %w", err)
		}
	}
	if s.config.EmbeddingEnabled {
		cfg.EmbedQuery = s.db.EmbedQuery
		cfg.BindQueryEmbedding = s.db.WithQueryEmbeddingSnapshot
	}
	if len(features.QdrantGRPCMethodPermissions) > 0 {
		overrides := make(map[string]auth.Permission, len(features.QdrantGRPCMethodPermissions))
		for k, v := range features.QdrantGRPCMethodPermissions {
			p, ok := parsePermissionString(v)
			if !ok {
				return fmt.Errorf("qdrant grpc: invalid RBAC permission %q for method %q", v, k)
			}
			overrides[k] = p
		}
		cfg.MethodPermissions = overrides
	}
	// Per-database RBAC: when auth is enabled, gRPC enforces same allowlist + privileges as HTTP/Bolt.
	if s.isRBACEnforced() {
		cfg.DatabaseAccessModeResolver = func(roles []string) auth.DatabaseAccessMode {
			mode := s.GetDatabaseAccessModeForRoles(roles)
			if mode == nil { return nil }
			return grpcDatabaseAccessMode{DatabaseAccessMode: mode, allows: func(database string) bool {
				_, allowed := s.grpcDatabaseTargets(database, mode)
				return allowed
			}}
		}
		cfg.ResolvedAccessResolver = s.grpcDatabaseAccessForRoles
	}

	searchProvider := func(database string, store storage.Engine) (*search.Service, error) {
		svc, err := s.db.GetOrCreateSearchService(database, store)
		if err != nil {
			return nil, err
		}
		// An intentionally disabled vector service has no hits. Provider
		// failures remain errors, so an unavailable index cannot acknowledge a
		// write or disguise an incomplete search as an empty successful answer.
		if !svc.VectorEnabled() {
			return nil, nil
		}
		return svc, nil
	}

	// A dropped collection's database loses its search index too, as with
	// Cypher's DROP DATABASE.
	cfg.DropDatabase = s.databaseManager().DropDatabase

	grpcServer, err := qdrantgrpc.NewServerWithDatabaseManager(cfg, s.dbManager, baseStorage, searchProvider, s.auth)
	if err != nil {
		return fmt.Errorf("qdrant grpc: failed to initialize server: %w", err)
	}

	rerankEnabled := searchSvc.RerankerAvailable(context.Background())
	nornicSearchSvc, err := nornicgrpc.NewService(
		nornicgrpc.Config{
			DefaultDatabase:  dbName,
			MaxLimit:         cfg.MaxTopK,
			RerankEnabled:    rerankEnabled,
			Localizer:        s.localizer,
			OwnerFromContext: qdrantgrpc.AuthenticatedPrincipalID,
			AllowDatabase: func(ctx context.Context, database string) error {
				return grpcServer.AllowDatabaseAccess(ctx, database, false)
			},
			ResolveContinuationDatabase: s.db.ResolveSearchContinuationDatabase,
			DiscardContinuation:         s.db.DiscardSearchContinuation,
			PrepareQueryEmbedding: func(_ context.Context, database string) (nornicgrpc.EmbedQueryFunc, nornicgrpc.ChunkQueryFunc) {
				queryEmbedding := s.db.QueryEmbeddingForDB(database)
				return queryEmbedding.EmbedQuery, queryEmbedding.ChunkQuery
			},
			ResolveDatabase: func(ctx context.Context, database string) (nornicgrpc.DatabaseDependencies, error) {
				store, err := s.dbManager.GetStorage(database)
				if err != nil {
					return nornicgrpc.DatabaseDependencies{}, err
				}
				svc, err := s.db.GetOrCreateSearchService(database, store)
				if err != nil {
					return nornicgrpc.DatabaseDependencies{}, err
				}
				return nornicgrpc.DatabaseDependencies{Searcher: svc}, nil
			},
		},
		nil, nil, // PrepareQueryEmbedding binds both callbacks only for text requests.
		searchSvc,
	)
	if err != nil {
		return fmt.Errorf("qdrant grpc: failed to init nornic search service: %w", err)
	}

	if err := grpcServer.RegisterAdditionalServices(func(gs *grpc.Server) {
		nornicpb.RegisterNornicSearchServer(gs, nornicSearchSvc)
	}); err != nil {
		return fmt.Errorf("qdrant grpc: failed to register nornic search service: %w", err)
	}

	if err := grpcServer.Start(); err != nil {
		return fmt.Errorf("qdrant grpc: failed to start: %w", err)
	}

	s.qdrantGRPCServer = grpcServer
	s.qdrantCollectionStore = grpcServer.CollectionStore()

	s.logEvent(context.Background(), slog.LevelInfo, localization.ServerQdrantGRPCEnabledEvent(dbName, grpcServer.Addr()))
	return nil
}

// parsePermissionString maps string IDs to auth.Permission (canonical entitlement IDs from auth/entitlements.go).
func parsePermissionString(v string) (auth.Permission, bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case string(auth.PermRead):
		return auth.PermRead, true
	case string(auth.PermWrite):
		return auth.PermWrite, true
	case string(auth.PermCreate):
		return auth.PermCreate, true
	case string(auth.PermDelete):
		return auth.PermDelete, true
	case string(auth.PermAdmin):
		return auth.PermAdmin, true
	case string(auth.PermSchema):
		return auth.PermSchema, true
	case string(auth.PermUserManage):
		return auth.PermUserManage, true
	default:
		return "", false
	}
}

func (s *Server) stopQdrantGRPC() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.qdrantGRPCServer != nil {
		s.qdrantGRPCServer.Stop()
		s.qdrantGRPCServer = nil
	}
	s.qdrantCollectionStore = nil
}
