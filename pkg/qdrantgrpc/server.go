// Package qdrantgrpc provides Qdrant-compatible gRPC APIs for NornicDB.
//
// This package enables existing Qdrant SDKs (Python, Go, Rust, etc.) to connect
// to NornicDB without modification by implementing the upstream Qdrant protobuf
// contract (package `qdrant`, pinned to v1.16.x).
//
// NornicDB does not expose any additional “compat” gRPC contract for Qdrant.
// The only public Qdrant surface is the upstream Qdrant protobuf contract.
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
// The upstream Qdrant SDK surface currently implements the core methods used by
// qdrant-client (Python) and other SDKs for typical vector workloads:
//   - Collections: Create, Get, List, Delete, Update, CollectionExists
//   - Points: Upsert, Get, Delete, Count, Search, Scroll, payload ops, vector ops
//
// Additional upstream Qdrant RPCs can be added incrementally as needed.
//
// # Data Model Mapping
//
//   - Qdrant Collection → Collection metadata in registry
//   - Qdrant Point → NornicDB Node with embeddings in ChunkEmbeddings (supports named vectors)
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
//	srv, err := qdrantgrpc.NewServer(cfg, storage, registry, searchService, authenticator)
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
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
)

// Config holds configuration for the Qdrant gRPC server.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":6334")
	ListenAddr string

	// AllowVectorMutations controls whether Qdrant points operations are allowed to
	// directly set/update/delete stored vectors.
	//
	// When NornicDB-managed embeddings are enabled, operators typically want to
	// prevent external clients from overwriting embeddings via the Qdrant API.
	// In that mode, vector mutation endpoints return FailedPrecondition.
	//
	// When NornicDB-managed embeddings are disabled, set this to true to allow
	// Qdrant clients to fully manage vectors.
	AllowVectorMutations bool

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

	// EmbedQuery, when set, allows the Qdrant Query API to accept text/inference
	// inputs (e.g. VectorInput.Document) and have NornicDB embed the query text.
	//
	// If nil, those query variants return FailedPrecondition.
	EmbedQuery func(ctx context.Context, text string) ([]float32, error)

	// MethodPermissions optionally overrides the default RBAC requirements for
	// specific RPCs.
	//
	// Keys are of the form "<Service>/<Method>", using the short gRPC service name:
	//   - "Collections/Create"
	//   - "Points/Upsert"
	//   - "Snapshots/List"
	//   - "ServerReflection/ServerReflectionInfo"
	//
	// If a request's method is not found in either this map or the built-in
	// defaults, the request is denied (default-deny).
	MethodPermissions map[string]auth.Permission
}

// DefaultConfig returns sensible defaults for the Qdrant gRPC server.
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:           ":6334",
		AllowVectorMutations: true,
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
	vecIndex      *vectorIndexCache
	authenticator *auth.Authenticator // Authentication for gRPC requests

	grpcServer *grpc.Server
	listener   net.Listener
	register   []func(*grpc.Server)

	mu      sync.RWMutex
	started bool
}

type contextKeyClaims struct{}

// NewServer creates a new Qdrant gRPC server.
//
// Parameters:
//   - config: Server configuration (use DefaultConfig() for sensible defaults)
//   - store: NornicDB storage engine for persisting points
//   - registry: Collection registry for managing collection metadata
//   - searchService: NornicDB search service for unified vector indexing (can be nil)
//   - authenticator: Authentication for gRPC requests (can be nil if auth disabled)
//
// If searchService is nil, points are still stored but not indexed for search.
// For production use, always provide a search.Service.
//
// Returns the server instance ready to Start().
func NewServer(config *Config, store storage.Engine, registry CollectionRegistry, searchService *search.Service, authenticator *auth.Authenticator) (*Server, error) {
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
		vecIndex:      newVectorIndexCache(),
		authenticator: authenticator,
		register:      nil,
	}, nil
}

// RegisterAdditionalServices registers additional gRPC services on the same server.
// This must be called before Start().
func (s *Server) RegisterAdditionalServices(fn func(*grpc.Server)) error {
	if fn == nil {
		return fmt.Errorf("registrar is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("cannot register services after start")
	}
	s.register = append(s.register, fn)
	return nil
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

	// Add authentication interceptor if authenticator is provided
	if s.authenticator != nil && s.authenticator.IsSecurityEnabled() {
		opts = append(opts, grpc.UnaryInterceptor(s.unaryAuthInterceptor))
		opts = append(opts, grpc.StreamInterceptor(s.streamAuthInterceptor))
	}

	s.grpcServer = grpc.NewServer(opts...)

	collectionsService := NewCollectionsService(s.registry, s.storage, s.searchService, s.vecIndex)
	qpb.RegisterCollectionsServer(s.grpcServer, collectionsService)

	pointsService := NewPointsService(s.config, s.storage, s.registry, s.searchService, s.vecIndex)
	qpb.RegisterPointsServer(s.grpcServer, pointsService)

	snapshotsService := NewSnapshotsService(s.config, s.storage, s.registry, s.config.SnapshotDir)
	qpb.RegisterSnapshotsServer(s.grpcServer, snapshotsService)

	for _, fn := range s.register {
		fn(s.grpcServer)
	}

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

func (s *Server) authorizeMethod(fullMethod string, claims *auth.JWTClaims) error {
	required, ok := requiredPermissionForMethod(fullMethod)
	if !ok {
		// Default-deny: unknown method should not be allowed implicitly.
		return status.Errorf(codes.PermissionDenied, "permission denied")
	}
	if s.config != nil && s.config.MethodPermissions != nil {
		if key, ok := methodKey(fullMethod); ok {
			if override, ok := s.config.MethodPermissions[key]; ok {
				required = override
			}
		}
	}
	if hasPermissionFromClaims(claims, required) {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "permission denied")
}

// unaryAuthInterceptor authenticates unary gRPC requests.
// Supports both Basic Auth (username/password) and Bearer JWT tokens, matching HTTP endpoint behavior.
func (s *Server) unaryAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Check if auth is enabled
	if s.authenticator == nil || !s.authenticator.IsSecurityEnabled() {
		return handler(ctx, req)
	}

	var claims *auth.JWTClaims
	var err error

	// Try Basic Auth first (Neo4j compatibility, same as HTTP endpoints)
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if authHeaders := md.Get("authorization"); len(authHeaders) > 0 {
			authHeader := authHeaders[0]
			if strings.HasPrefix(authHeader, "Basic ") {
				claims, err = s.handleBasicAuth(ctx, authHeader)
				if err == nil {
					if err := s.authorizeMethod(info.FullMethod, claims); err != nil {
						return nil, err
					}
					ctx = context.WithValue(ctx, contextKeyClaims{}, claims)
					return handler(ctx, req)
				}
				// If Basic Auth fails, fall through to try Bearer token
			}
		}
	}

	// Try Bearer/JWT token extraction
	token, err := s.extractTokenFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "authentication required: %v", err)
	}

	// Validate token
	claims, err = s.authenticator.ValidateToken(token)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid or expired token")
	}

	// Add claims to context for use in handlers
	if err := s.authorizeMethod(info.FullMethod, claims); err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKeyClaims{}, claims)
	return handler(ctx, req)
}

// streamAuthInterceptor authenticates streaming gRPC requests.
// Supports both Basic Auth (username/password) and Bearer JWT tokens, matching HTTP endpoint behavior.
func (s *Server) streamAuthInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// Check if auth is enabled
	if s.authenticator == nil || !s.authenticator.IsSecurityEnabled() {
		return handler(srv, ss)
	}

	ctx := ss.Context()
	var claims *auth.JWTClaims
	var err error

	// Try Basic Auth first (Neo4j compatibility, same as HTTP endpoints)
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if authHeaders := md.Get("authorization"); len(authHeaders) > 0 {
			authHeader := authHeaders[0]
			if strings.HasPrefix(authHeader, "Basic ") {
				claims, err = s.handleBasicAuth(ctx, authHeader)
				if err == nil {
					if err := s.authorizeMethod(info.FullMethod, claims); err != nil {
						return err
					}
					ctx = context.WithValue(ctx, contextKeyClaims{}, claims)
					return handler(srv, &authServerStream{ServerStream: ss, ctx: ctx})
				}
				// If Basic Auth fails, fall through to try Bearer token
			}
		}
	}

	// Try Bearer/JWT token extraction
	token, err := s.extractTokenFromMetadata(ctx)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "authentication required: %v", err)
	}

	// Validate token
	claims, err = s.authenticator.ValidateToken(token)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "invalid or expired token")
	}

	// Add claims to context for use in handlers
	if err := s.authorizeMethod(info.FullMethod, claims); err != nil {
		return err
	}
	ctx = context.WithValue(ctx, contextKeyClaims{}, claims)
	return handler(srv, &authServerStream{ServerStream: ss, ctx: ctx})
}

// handleBasicAuth handles Basic authentication for gRPC requests.
// Matches the behavior of HTTP endpoint Basic Auth.
func (s *Server) handleBasicAuth(ctx context.Context, authHeader string) (*auth.JWTClaims, error) {
	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid basic auth encoding")
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid basic auth format")
	}

	username, password := parts[0], parts[1]

	// Get client IP from metadata if available
	clientIP := "unknown"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if forwarded := md.Get("x-forwarded-for"); len(forwarded) > 0 {
			clientIP = forwarded[0]
		} else if realIP := md.Get("x-real-ip"); len(realIP) > 0 {
			clientIP = realIP[0]
		}
	}

	// Authenticate and get user
	_, user, err := s.authenticator.Authenticate(username, password, clientIP, "gRPC")
	if err != nil {
		return nil, err
	}

	// Convert user to claims
	roles := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roles[i] = string(role)
	}

	return &auth.JWTClaims{
		Sub:      user.ID,
		Username: user.Username,
		Email:    user.Email,
		Roles:    roles,
	}, nil
}

func hasPermissionFromClaims(claims *auth.JWTClaims, required auth.Permission) bool {
	if claims == nil {
		return false
	}
	for _, roleStr := range claims.Roles {
		role := auth.Role(roleStr)
		perms, ok := auth.RolePermissions[role]
		if !ok {
			continue
		}
		for _, p := range perms {
			if p == required {
				return true
			}
		}
	}
	return false
}

func methodKey(fullMethod string) (string, bool) {
	if fullMethod == "" {
		return "", false
	}

	trimmed := strings.TrimPrefix(fullMethod, "/")
	serviceFull, method, ok := strings.Cut(trimmed, "/")
	if !ok || serviceFull == "" || method == "" {
		return "", false
	}

	// Use short service name (after last '.') for stability.
	serviceShort := serviceFull
	if idx := strings.LastIndexByte(serviceFull, '.'); idx >= 0 && idx+1 < len(serviceFull) {
		serviceShort = serviceFull[idx+1:]
	}
	return serviceShort + "/" + method, true
}

var defaultMethodPermissions = map[string]auth.Permission{
	// Collections (admin/DDL-ish)
	"Collections/Create":                   auth.PermCreate,
	"Collections/Update":                   auth.PermCreate,
	"Collections/UpdateAliases":            auth.PermCreate,
	"Collections/CreateAlias":              auth.PermCreate,
	"Collections/DeleteAlias":              auth.PermCreate,
	"Collections/RenameAlias":              auth.PermCreate,
	"Collections/Delete":                   auth.PermDelete,
	"Collections/Get":                      auth.PermRead,
	"Collections/List":                     auth.PermRead,
	"Collections/GetAliases":               auth.PermRead,
	"Collections/CollectionExists":         auth.PermRead,
	"Collections/GetCollectionClusterInfo": auth.PermRead,

	// Points (data plane)
	"Points/Search":         auth.PermRead,
	"Points/SearchBatch":    auth.PermRead,
	"Points/Scroll":         auth.PermRead,
	"Points/Get":            auth.PermRead,
	"Points/Count":          auth.PermRead,
	"Points/Recommend":      auth.PermRead,
	"Points/RecommendBatch": auth.PermRead,
	"Points/Discover":       auth.PermRead,
	"Points/DiscoverBatch":  auth.PermRead,

	"Points/Upsert":           auth.PermWrite,
	"Points/UpdateVectors":    auth.PermWrite,
	"Points/DeleteVectors":    auth.PermWrite,
	"Points/SetPayload":       auth.PermWrite,
	"Points/OverwritePayload": auth.PermWrite,
	"Points/ClearPayload":     auth.PermWrite,

	"Points/Delete": auth.PermDelete,

	// Snapshots are privileged operations.
	"Snapshots/Create":  auth.PermAdmin,
	"Snapshots/List":    auth.PermAdmin,
	"Snapshots/Delete":  auth.PermAdmin,
	"Snapshots/Recover": auth.PermAdmin,

	// gRPC reflection is useful for debugging but should be gated.
	"ServerReflection/ServerReflectionInfo": auth.PermAdmin,
}

func requiredPermissionForMethod(fullMethod string) (auth.Permission, bool) {
	key, ok := methodKey(fullMethod)
	if !ok {
		return "", false
	}
	p, ok := defaultMethodPermissions[key]
	return p, ok
}

// extractTokenFromMetadata extracts JWT token from gRPC metadata.
// Supports both "authorization" (Bearer token) and "x-api-key" headers.
func (s *Server) extractTokenFromMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("no metadata found")
	}

	// Try Authorization header (Bearer token)
	if authHeaders := md.Get("authorization"); len(authHeaders) > 0 {
		authHeader := authHeaders[0]
		if strings.HasPrefix(authHeader, "Bearer ") {
			return strings.TrimPrefix(authHeader, "Bearer "), nil
		}
		// Basic auth is handled separately in handleBasicAuth
	}

	// Try X-API-Key header
	if apiKeys := md.Get("x-api-key"); len(apiKeys) > 0 {
		return apiKeys[0], nil
	}

	return "", fmt.Errorf("no authentication token found in metadata")
}

// authServerStream wraps grpc.ServerStream to provide a custom context.
type authServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (a *authServerStream) Context() context.Context {
	return a.ctx
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
//	authenticator := auth.NewAuthenticator(auth.DefaultAuthConfig())
//	srv, registry, err := qdrantgrpc.NewServerWithPersistentRegistry(nil, storage, searchSvc, authenticator)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer registry.Close()
//	srv.Start()
func NewServerWithPersistentRegistry(config *Config, store storage.Engine, searchService *search.Service, authenticator *auth.Authenticator) (*Server, *PersistentCollectionRegistry, error) {
	if store == nil {
		return nil, nil, fmt.Errorf("storage engine required")
	}

	registry, err := NewPersistentCollectionRegistry(store)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create collection registry: %w", err)
	}

	server, err := NewServer(config, store, registry, searchService, authenticator)
	if err != nil {
		registry.Close()
		return nil, nil, err
	}

	return server, registry, nil
}
