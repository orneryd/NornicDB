// Package qdrantgrpc provides a Qdrant-compatible gRPC API for NornicDB.
//
// This package enables existing Qdrant SDKs (Python, Go, Rust, etc.) to connect
// to NornicDB without modification. It implements a subset of the Qdrant gRPC API
// pinned to v1.16.x, providing vector search operations via NornicDB's search.Service.
//
// # Integration with NornicDB Search
//
// This package integrates with the existing search.Service to ensure:
//   - Points added via Qdrant gRPC are searchable via /nornicdb/search
//   - Points added via Cypher are searchable via Qdrant gRPC
//   - A single unified vector index is maintained
//
// # Compatibility
//
// The following Qdrant services are implemented:
//   - Collections: CreateCollection, GetCollectionInfo, ListCollections, DeleteCollection
//   - Points: Upsert, Get, Delete, Search, SearchBatch, Count
//   - Health: Standard gRPC health checking
//
// # Data Model Mapping
//
//   - Qdrant Collection → Collection metadata in registry
//   - Qdrant Point → NornicDB Node with embedding in ChunkEmbeddings[0]
//   - Qdrant Payload → NornicDB Node properties
//   - Qdrant PointId → NornicDB NodeID (prefixed: qdrant:<collection>:<id>)
//
// # Feature Flag
//
// The Qdrant gRPC endpoint is controlled by a feature flag:
//   - Environment: NORNICDB_QDRANT_GRPC_ENABLED=true
//   - Config: config.Features.QdrantGRPCEnabled = true
//
// # Usage
//
//	// Create server with NornicDB storage and search
//	cfg := qdrantgrpc.DefaultConfig()
//	srv, err := qdrantgrpc.NewServer(cfg, storage, registry, searchService)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Start listening
//	if err := srv.Start(); err != nil {
//		log.Fatal(err)
//	}
//	defer srv.Stop()
//
// # ELI12
//
// Think of this like a translator at a restaurant:
//   - Qdrant SDKs "speak Qdrant language" (their API)
//   - NornicDB "speaks NornicDB language" (its internal API)
//   - This server translates between them so they can communicate
//   - When a Qdrant client asks to store a vector, we translate it to NornicDB format
//   - When NornicDB returns results, we translate back to Qdrant format
package qdrantgrpc

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Config holds configuration for the Qdrant gRPC server.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":6334")
	ListenAddr string

	// MaxVectorDim is the maximum allowed vector dimension
	MaxVectorDim int

	// MaxBatchPoints is the maximum points per upsert batch
	MaxBatchPoints int

	// MaxPayloadBytes is the maximum payload size per point
	MaxPayloadBytes int

	// MaxTopK is the maximum results per search
	MaxTopK int

	// MaxFilterClauses is the maximum filter conditions
	MaxFilterClauses int

	// RequestTimeout is the default deadline for requests
	RequestTimeout time.Duration

	// MaxConcurrentStreams per connection
	MaxConcurrentStreams uint32

	// MaxRecvMsgSize in bytes
	MaxRecvMsgSize int

	// MaxSendMsgSize in bytes
	MaxSendMsgSize int

	// EnableReflection enables gRPC server reflection
	EnableReflection bool

	// SnapshotDir is the directory for storing snapshots
	SnapshotDir string
}

// DefaultConfig returns sensible defaults for the Qdrant gRPC server.
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:           ":6334",
		MaxVectorDim:         4096,
		MaxBatchPoints:       1000,
		MaxPayloadBytes:      1024 * 1024, // 1MB
		MaxTopK:              1000,
		MaxFilterClauses:     100,
		RequestTimeout:       30 * time.Second,
		MaxConcurrentStreams: 100,
		MaxRecvMsgSize:       64 * 1024 * 1024, // 64MB
		MaxSendMsgSize:       64 * 1024 * 1024, // 64MB
		EnableReflection:     true,
		SnapshotDir:          "./data/qdrant-snapshots",
	}
}

// Server is the Qdrant-compatible gRPC server.
type Server struct {
	config        *Config
	storage       storage.Engine
	registry      CollectionRegistry
	searchService *search.Service

	grpcServer *grpc.Server
	listener   net.Listener

	mu      sync.RWMutex
	started bool
}

// NewServer creates a new Qdrant gRPC server.
//
// Parameters:
//   - config: Server configuration (use DefaultConfig() for sensible defaults)
//   - storage: NornicDB storage engine for persisting points
//   - registry: Collection registry for managing collection metadata
//   - searchService: NornicDB search service for unified vector indexing (can be nil)
//
// If searchService is nil, points are still stored but not indexed for search.
// For production use, always provide a search.Service.
//
// Returns the server instance ready to Start().
func NewServer(config *Config, store storage.Engine, registry CollectionRegistry, searchService *search.Service) (*Server, error) {
	if config == nil {
		config = DefaultConfig()
	}
	if store == nil {
		return nil, fmt.Errorf("storage engine required")
	}
	if registry == nil {
		return nil, fmt.Errorf("collection registry required")
	}

	return &Server{
		config:        config,
		storage:       store,
		registry:      registry,
		searchService: searchService,
	}, nil
}

// Start begins listening for gRPC connections.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("server already started")
	}

	// Create listener
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.ListenAddr, err)
	}
	s.listener = listener

	// Create gRPC server with options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.config.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(s.config.MaxSendMsgSize),
		grpc.MaxConcurrentStreams(s.config.MaxConcurrentStreams),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     5 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  1 * time.Minute,
			Timeout:               20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	s.grpcServer = grpc.NewServer(opts...)

	// Register services
	collectionsService := NewCollectionsService(s.registry)
	pb.RegisterCollectionsServer(s.grpcServer, collectionsService)

	pointsService := NewPointsService(s.config, s.storage, s.registry, s.searchService)
	pb.RegisterPointsServer(s.grpcServer, pointsService)

	snapshotsService := NewSnapshotsService(s.config, s.storage, s.registry, s.config.SnapshotDir)
	pb.RegisterSnapshotsServer(s.grpcServer, snapshotsService)

	healthService := NewHealthService()
	pb.RegisterHealthServer(s.grpcServer, healthService)

	// Enable reflection for debugging
	if s.config.EnableReflection {
		reflection.Register(s.grpcServer)
	}

	s.started = true

	// Start serving in background
	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			log.Printf("Qdrant gRPC server error: %v", err)
		}
	}()

	log.Printf("✅ Qdrant gRPC server listening on %s", s.config.ListenAddr)
	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.started = false
	log.Println("Qdrant gRPC server stopped")
}

// Addr returns the server's listen address.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

// IsRunning returns whether the server is currently running.
func (s *Server) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started
}

// NewServerWithPersistentRegistry creates a server with a persistent collection registry.
// This is the recommended way to create a production Qdrant gRPC server.
//
// The persistent registry:
//   - Persists collection metadata to storage
//   - Loads existing collections on startup
//
// The search service (if provided):
//   - Indexes points for unified vector search
//   - Enables cross-endpoint search (Qdrant gRPC + /nornicdb/search)
//
// Example:
//
//	storage := badger.NewEngine("./data")
//	searchSvc := search.NewService(storage)
//	srv, registry, err := qdrantgrpc.NewServerWithPersistentRegistry(nil, storage, searchSvc)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer registry.Close()
//	srv.Start()
func NewServerWithPersistentRegistry(config *Config, store storage.Engine, searchService *search.Service) (*Server, *PersistentCollectionRegistry, error) {
	if store == nil {
		return nil, nil, fmt.Errorf("storage engine required")
	}

	registry, err := NewPersistentCollectionRegistry(store)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create collection registry: %w", err)
	}

	server, err := NewServer(config, store, registry, searchService)
	if err != nil {
		registry.Close()
		return nil, nil, err
	}

	return server, registry, nil
}
