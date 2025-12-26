package server

import (
	"context"
	"fmt"
	"log"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicgrpc"
	nornicpb "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/qdrantgrpc"
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

	grpcServer, registry, err := qdrantgrpc.NewServerWithPersistentRegistry(cfg, storageEngine, searchSvc)
	if err != nil {
		return fmt.Errorf("qdrant grpc: failed to initialize server: %w", err)
	}

	nornicSearchSvc, err := nornicgrpc.NewService(
		nornicgrpc.Config{
			DefaultDatabase: dbName,
			MaxLimit:        cfg.MaxTopK,
		},
		func(ctx context.Context, query string) ([]float32, error) {
			return s.db.EmbedQuery(ctx, query)
		},
		searchSvc,
	)
	if err != nil {
		registry.Close()
		return fmt.Errorf("qdrant grpc: failed to init nornic search service: %w", err)
	}

	if err := grpcServer.RegisterAdditionalServices(func(gs *grpc.Server) {
		nornicpb.RegisterNornicSearchServer(gs, nornicSearchSvc)
	}); err != nil {
		registry.Close()
		return fmt.Errorf("qdrant grpc: failed to register nornic search service: %w", err)
	}

	if err := grpcServer.Start(); err != nil {
		registry.Close()
		return fmt.Errorf("qdrant grpc: failed to start: %w", err)
	}

	s.qdrantGRPCServer = grpcServer
	s.qdrantGRPCRegistry = registry

	log.Printf("✓ Qdrant gRPC enabled (db=%s, addr=%s)", dbName, grpcServer.Addr())
	return nil
}

func (s *Server) stopQdrantGRPC() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.qdrantGRPCServer != nil {
		s.qdrantGRPCServer.Stop()
		s.qdrantGRPCServer = nil
	}
	if s.qdrantGRPCRegistry != nil {
		s.qdrantGRPCRegistry.Close()
		s.qdrantGRPCRegistry = nil
	}
}
