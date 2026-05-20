package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicgrpc"
	nornicpb "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/qdrantgrpc"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"google.golang.org/grpc"
)

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
	if s.config.EmbeddingEnabled {
		cfg.EmbedQuery = s.db.EmbedQuery
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
	if s.auth != nil && s.auth.IsSecurityEnabled() {
		cfg.DatabaseAccessModeResolver = s.GetDatabaseAccessModeForRoles
		cfg.ResolvedAccessResolver = s.GetResolvedAccessForRoles
	}

	searchProvider := func(database string, store storage.Engine) (*search.Service, error) {
		svc, err := s.db.GetOrCreateSearchService(database, store)
		if err != nil {
			return nil, err
		}
		// Per-DB master switch: the Qdrant gRPC bridge serves vector
		// queries; if the database has vector search disabled, surface a
		// deterministic error rather than handing back a service whose
		// ANN substrate isn't populated. External Qdrant clients see
		// the same "off" semantics they'd get against a database that
		// simply has no vectors.
		if !svc.VectorEnabled() {
			return nil, fmt.Errorf("qdrant grpc: vector search is disabled for database %q (set NORNICDB_SEARCH_VECTOR_ENABLED=true or per-DB override to enable)", database)
		}
		return svc, nil
	}

	grpcServer, err := qdrantgrpc.NewServerWithDatabaseManager(cfg, s.dbManager, baseStorage, searchProvider, s.auth)
	if err != nil {
		return fmt.Errorf("qdrant grpc: failed to initialize server: %w", err)
	}

	rerankEnabled := searchSvc.RerankerAvailable(context.Background())
	nornicSearchSvc, err := nornicgrpc.NewService(
		nornicgrpc.Config{
			DefaultDatabase: dbName,
			MaxLimit:        cfg.MaxTopK,
			RerankEnabled:   rerankEnabled,
		},
		func(ctx context.Context, query string) ([]float32, error) {
			return s.db.EmbedQuery(ctx, query)
		},
		func(ctx context.Context, query string) ([]string, error) {
			return s.db.ChunkQueryForDB(ctx, dbName, query)
		},
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

	s.log.Info("qdrant grpc enabled", "db", dbName, "addr", grpcServer.Addr())
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
