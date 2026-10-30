// Package multidb provides multi-database support for NornicDB.
//
// This package implements Neo4j 4.x-style multi-database support, allowing
// multiple logical databases (tenants) to share a single physical storage backend
// while maintaining complete data isolation.
package multidb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// DatabaseManager manages multiple logical databases within a single storage engine.
//
// It provides:
//   - Database creation and deletion
//   - Database metadata tracking
//   - Namespaced storage engine views
//   - Neo4j 4.x multi-database compatibility
//
// Reads use the published catalog. Mutations require their original operation
// context and serialize through physical receipt settlement. Canceling a waiter
// after acceptance never rolls back the accepted catalog proposal.
//
// Example:
//
//	// Create manager with shared storage
//	ctx := context.Background()
//	inner, _ := storage.NewBadgerEngine("./data")
//	manager, _ := multidb.NewDatabaseManager(ctx, inner, nil)
//
//	// Create databases
//	manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: "tenant_a"})
//	manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: "tenant_b"})
//
//	// Get namespaced storage for a tenant
//	tenantStorage, _ := manager.GetStorage("tenant_a")
//
//	// Use storage (isolated to tenant_a)
//	tenantStorage.CreateNode(&storage.Node{ID: "123"})
type DatabaseManager struct {
	mu            sync.RWMutex
	catalogGate   chan struct{}
	dropAdmission writeAdmission

	// Shared underlying storage
	inner storage.Engine

	// Database metadata (persisted in "system" namespace)
	pendingDrops    map[string]bool
	databases       map[string]*DatabaseInfo
	serverID        string
	serverCreatedAt time.Time

	// Configuration
	config *Config

	// Cached database-scoped engines (avoid recreating)
	engines map[string]storage.Engine
	// Cached limit checkers share query permits and rate state per database.
	limitCheckers            map[string]*databaseLimitChecker
	statementCaches          map[string]StatementCacheResource
	statementCacheScopes     map[string]*StatementCacheScope
	statementCacheGeneration uint64
	statementCachesClosed    bool
	statementCacheIndexID    uint64
	statementCacheIndexes    map[uint64]func()

	// Factory used to create storage engines for remote constituents.
	remoteEngineFactory RemoteEngineFactory

	// Optional encryptor for remote constituent credentials persisted in metadata.
	remoteCredentialCipher *remoteCredentialCipher
}

// DatabaseInfo holds metadata about a database.
type DatabaseInfo struct {
	ID           string           `json:"id"`
	Name         string           `json:"name"`
	CreatedAt    time.Time        `json:"created_at"`
	CreatedBy    string           `json:"created_by,omitempty"`
	Status       string           `json:"status"` // "online", "offline"
	Type         string           `json:"type"`   // "standard", "system"
	IsDefault    bool             `json:"is_default"`
	NodeCount    int64            `json:"node_count,omitempty"` // Cached, may be stale
	UpdatedAt    time.Time        `json:"updated_at"`
	Aliases      []string         `json:"aliases,omitempty"`      // Database aliases (Neo4j-compatible)
	Limits       *Limits          `json:"limits,omitempty"`       // Resource limits
	Constituents []ConstituentRef `json:"constituents,omitempty"` // Constituent databases (for composite type)
	// runtimeConstituentsByAlias is rebuilt when constituent metadata changes.
	// It is private and never serialized or returned from metadata snapshots.
	runtimeConstituentsByAlias map[string]*runtimeConstituent
	// Options are the database's options (CREATE DATABASE … OPTIONS, ALTER
	// DATABASE … SET OPTION), by Neo4j's option name.
	Options map[string]string `json:"options,omitempty"`

	managed atomic.Pointer[managedDatabaseRuntime]

	// Size tracking (incremental, not recalculated)
	totalSize       int64 // Total storage size in bytes
	nodeSize        int64 // Total size of all nodes in bytes
	edgeSize        int64 // Total size of all edges in bytes
	sizeInitialized bool  // Whether size has been calculated at least once
	// Actual counts, canonical byte counters, and accepted pending allowances
	// share sizeMu so publication replaces a reservation atomically.
	usageNodes, usageEdges int64
	usageInitialized       bool
	reserved               reservedUsage
	pending                map[string]reservedUsage
	sizeMu                 sync.RWMutex // Protects actual and reserved usage
}

// Config holds DatabaseManager configuration.
type Config struct {
	// DefaultDatabase is the database used when none is specified (default: "nornic")
	// This matches Neo4j's behavior where "neo4j" is the default, but NornicDB uses "nornic"
	DefaultDatabase string

	// SystemDatabase stores metadata (default: "system")
	SystemDatabase string

	// MaxDatabases limits total databases (0 = unlimited)
	MaxDatabases int

	// AllowDropDefault allows dropping the default database
	AllowDropDefault bool

	// RemoteEngineFactory creates a storage engine for a remote constituent.
	// If nil, remote constituents are not executable (metadata may still be stored).
	RemoteEngineFactory RemoteEngineFactory

	// RemoteCredentialEncryptionKey encrypts remote constituent user/password values
	// before metadata persistence. If empty, user_password auth mode is rejected.
	RemoteCredentialEncryptionKey string
}

// DefaultConfig returns default configuration.
// The default database name is "nornic" (NornicDB's equivalent of Neo4j's "neo4j").
func DefaultConfig() *Config {
	return &Config{
		DefaultDatabase:  "nornic",
		SystemDatabase:   "system",
		MaxDatabases:     0, // Unlimited
		AllowDropDefault: false,
	}
}

// NewDatabaseManager creates a new database manager.
// ctx is the explicit startup/process lifetime, distinct from later request
// contexts. Writable catalog storage must expose native entity preparation and
// the authoritative EntityCommitSubmitter receipt capability through its wrapper
// route. Read-only startup does not admit catalog writes.
//
// Parameters:
//   - inner: The underlying storage engine (shared by all databases)
//   - config: Configuration (nil for defaults)
//
// On creation, initializes:
//   - System database (for metadata)
//   - Default database ("nornic" by default, configurable)
func NewDatabaseManager(ctx context.Context, inner storage.Engine, config *Config) (*DatabaseManager, error) {
	if ctx == nil {
		return nil, fmt.Errorf("database manager requires a startup context: %w", storage.ErrInvalidData)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config == nil {
		config = DefaultConfig()
	}
	// DatabaseManager requires an un-namespaced base engine. It creates NamespacedEngines
	// per database. Passing a NamespacedEngine here would double-prefix IDs and can
	// leak system metadata into the default database (e.g., "nornic:system:...").
	if _, ok := inner.(*storage.NamespacedEngine); ok {
		return nil, fmt.Errorf("multidb: NewDatabaseManager requires base storage (non-namespaced); pass db.GetBaseStorageForManager()")
	}
	// Ensure callers can't accidentally make the system database the default.
	// Neo4j reserves the system database for metadata and administration commands.
	if config.SystemDatabase == "" {
		config.SystemDatabase = "system"
	}
	if config.DefaultDatabase == "" {
		config.DefaultDatabase = "nornic"
	}
	if config.DefaultDatabase == config.SystemDatabase {
		log.Printf("⚠️  multidb: default database %q matches system database; forcing default to %q", config.DefaultDatabase, "nornic")
		config.DefaultDatabase = "nornic"
	}

	m := &DatabaseManager{
		catalogGate:         make(chan struct{}, 1),
		inner:               inner,
		databases:           make(map[string]*DatabaseInfo),
		config:              config,
		engines:             make(map[string]storage.Engine),
		limitCheckers:       make(map[string]*databaseLimitChecker),
		remoteEngineFactory: config.RemoteEngineFactory,
	}
	m.catalogGate <- struct{}{}
	if key := strings.TrimSpace(config.RemoteCredentialEncryptionKey); key != "" {
		cipher, err := newRemoteCredentialCipher(key)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize remote credential cipher: %w", err)
		}
		m.remoteCredentialCipher = cipher
	}

	// Load existing databases from system namespace
	identityComplete, loadErr := m.loadMetadata(ctx)
	if loadErr != nil {
		return nil, fmt.Errorf("failed to load database metadata: %w", loadErr)
	}
	for name, info := range m.databases {
		m.applyChangeCapture(name, info.Options)
	}

	// If the underlying engine is read-only (e.g. HA standby), do not attempt
	// to create/migrate metadata. Standby nodes should start quickly and serve
	// reads/UI while replication catches them up.
	readOnly := false
	if leader, ok := inner.(interface{ IsLeader() bool }); ok && !leader.IsLeader() {
		readOnly = true
	}

	// Ensure system and default databases exist (in-memory always; persisted only if writable).
	if readOnly {
		if _, exists := m.databases[m.config.SystemDatabase]; !exists {
			m.databases[m.config.SystemDatabase] = &DatabaseInfo{
				Name:      m.config.SystemDatabase,
				CreatedAt: time.Now(),
				Status:    "online",
				Type:      "system",
				IsDefault: false,
				UpdatedAt: time.Now(),
			}
		}
		if _, exists := m.databases[m.config.DefaultDatabase]; !exists {
			m.databases[m.config.DefaultDatabase] = &DatabaseInfo{
				Name:      m.config.DefaultDatabase,
				CreatedAt: time.Now(),
				Status:    "online",
				Type:      "standard",
				IsDefault: true,
				UpdatedAt: time.Now(),
			}
		}
		log.Printf("ℹ️  multidb: storage is read-only; skipping metadata writes and migrations")
		if err := m.registerGraphRuntimeOwners(); err != nil {
			return nil, err
		}
		return m, nil
	}

	if err := m.recoverPendingDrops(ctx); err != nil {
		return nil, err
	}

	if err := m.ensureSystemDatabases(ctx, identityComplete); err != nil {
		return nil, err
	}

	// One-time migration: move any pre-multi-db (unprefixed) data into the default
	// database namespace so the rest of the system can remain strictly namespaced.
	if err := m.migrateLegacyData(); err != nil {
		return nil, fmt.Errorf("failed to migrate legacy data: %w", err)
	}

	// Cleanup: if a previous run mistakenly constructed DatabaseManager with a
	// NamespacedEngine, system metadata nodes may have been stored under the default
	// namespace as "defaultDb:system:<...>". Remove those leaked nodes so normal
	// queries against the default database don't show system internals.
	m.cleanupLeakedSystemNodes()

	if err := m.registerGraphRuntimeOwners(); err != nil {
		return nil, err
	}
	return m, nil
}

// registerGraphRuntimeOwners attaches graph admission before raw replication
// can reach a namespace. Construction only registers callbacks; it never scans.
func (m *DatabaseManager) registerGraphRuntimeOwners() error {
	for name, info := range m.databases {
		if m.pendingDrops[name] || info.Type != "standard" || name == m.config.SystemDatabase {
			continue
		}
		_, checker, err := m.databaseViewLocked(name, false)
		if err != nil {
			return err
		}
		wrapped := newSizeTrackingEngine(storage.NewNamespacedEngine(m.inner, name), m, name, checker).(*sizeTrackingEngine)
		if wrapped.runtimeErr != nil {
			return wrapped.runtimeErr
		}
	}
	return nil
}

func (m *DatabaseManager) cleanupLeakedSystemNodes() {
	systemPrefix := m.config.SystemDatabase + ":"
	if m.config.SystemDatabase == "" {
		systemPrefix = "system:"
	}

	// Only target known system-internal nodes (metadata, migration, users) that
	// should never live in user databases.
	isLeak := func(node *storage.Node) bool {
		if node == nil {
			return false
		}
		id := string(node.ID)
		if !strings.HasPrefix(id, systemPrefix) {
			return false
		}
		hasSystemLabel := false
		for _, label := range node.Labels {
			if label == "_System" {
				hasSystemLabel = true
				break
			}
		}
		if !hasSystemLabel {
			return false
		}
		// Restrict deletions to well-known internal namespaces.
		return id == systemPrefix+"databases:metadata" ||
			strings.HasPrefix(id, systemPrefix+"migration:") ||
			strings.HasPrefix(id, systemPrefix+"user:")
	}
	candidatePrefixes := []string{
		systemPrefix + "databases:metadata",
		systemPrefix + "migration:",
		systemPrefix + "user:",
	}

	removed := 0
	for dbName, info := range m.databases {
		if info == nil {
			continue
		}
		if dbName == m.config.SystemDatabase {
			continue
		}
		engine := storage.NewNamespacedEngine(m.inner, dbName)
		candidateIDs := make(map[storage.NodeID]struct{})
		scanFailed := false
		for _, prefix := range candidatePrefixes {
			err := engine.StreamNodesByPrefix(context.Background(), prefix, func(node *storage.Node) error {
				if isLeak(node) {
					candidateIDs[node.ID] = struct{}{}
				}
				return nil
			})
			if err != nil {
				scanFailed = true
				break
			}
		}
		if scanFailed {
			continue
		}
		for id := range candidateIDs {
			if err := engine.DeleteNode(id); err == nil {
				removed++
			}
		}
	}
	if removed > 0 {
		log.Printf("🧹 multidb: removed %d leaked system nodes from user databases", removed)
	}
}

// ensureSystemDatabases creates system and default databases if they don't exist.
func (m *DatabaseManager) ensureSystemDatabases(ctx context.Context, identityComplete bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Constructor-only input: the canonical load product proves every identity
	// was persisted, rather than merely synthesized from a legacy creation time.
	if identityComplete && m.databases[m.config.SystemDatabase] != nil && m.databases[m.config.DefaultDatabase] != nil {
		return nil
	}

	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {
		// System database
		if _, exists := m.databases[m.config.SystemDatabase]; !exists {
			m.databases[m.config.SystemDatabase] = &DatabaseInfo{
				Name:      m.config.SystemDatabase,
				CreatedAt: time.Now(),
				Status:    "online",
				Type:      "system",
				IsDefault: false,
				UpdatedAt: time.Now(),
			}
		}

		// Default database
		if _, exists := m.databases[m.config.DefaultDatabase]; !exists {
			m.databases[m.config.DefaultDatabase] = &DatabaseInfo{
				Name:      m.config.DefaultDatabase,
				CreatedAt: time.Now(),
				Status:    "online",
				Type:      "standard",
				IsDefault: true,
				UpdatedAt: time.Now(),
			}
		}

		return nil

	})
}

// CreateDatabase creates a new database.
//
// Parameters:
//   - request: Database name and initial options, copied into one catalog publication
//
// Returns ErrDatabaseExists if database already exists.
// Returns ErrMaxDatabasesReached if limit exceeded.
func (m *DatabaseManager) CreateDatabase(ctx context.Context, request DatabaseCreateRequest) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {

		// Validate name
		if request.Name == "" {
			return ErrInvalidDatabaseName
		}

		// Check if exists
		if _, exists := m.databases[request.Name]; exists || m.pendingDrops[request.Name] {
			return ErrDatabaseExists
		}

		// Check limit
		if m.config.MaxDatabases > 0 && len(m.databases) >= m.config.MaxDatabases {
			return ErrMaxDatabasesReached
		}

		// Validate and detach initial options before preparing any catalog write.
		var options map[string]string
		if len(request.Options) > 0 {
			options = make(map[string]string, len(request.Options))
			for key, value := range request.Options {
				mode := strings.ToUpper(strings.TrimSpace(value))
				if key != OptionTxLogEnrichment || (mode != "OFF" && mode != "DIFF" && mode != "FULL") {
					return fmt.Errorf("invalid initial database option %q: %w", key, storage.ErrInvalidData)
				}
				options[key] = mode
			}
		}
		// Create metadata
		m.databases[request.Name] = &DatabaseInfo{
			Name:      request.Name,
			Options:   options,
			CreatedAt: time.Now(),
			Status:    "online",
			Type:      "standard",
			IsDefault: false,
			UpdatedAt: time.Now(),
		}

		return nil

	})
}

// DropDatabase removes a database and all its data.
//
// Parameters:
//   - name: Database name to drop
//
// Returns ErrDatabaseNotFound if database doesn't exist.
// Returns ErrCannotDropSystemDB for system/default databases.
func (m *DatabaseManager) DropDatabase(ctx context.Context, name string) error {
	if ctx == nil {
		return fmt.Errorf("DROP requires an operation context: %w", storage.ErrInvalidData)
	}
	// A physical purge is a distinct synchronous owner. Serializing DROP
	// invocations prevents a delayed old-generation purge from deleting a fresh
	// same-name database; catalog receipts separately retain accepted authority.
	release, err := m.dropAdmission.acquire(ctx, true, false)
	if err != nil {
		return err
	}
	return m.dropAdmitted(ctx, name, release)
}

// dropAdmitted is the DROP owner's body. It decides absent, pending
// completion or existing for name. The caller holds exclusive DROP admission
// and hands release to this call. It is released when this call returns, or,
// for an accepted drop, when its completion ends, which is later when the
// request stops waiting first (completeAcceptedDrop).
func (m *DatabaseManager) dropAdmitted(ctx context.Context, name string, release func()) error {
	handedOff := false
	defer func() {
		if !handedOff {
			release()
		}
	}()
	policy := catalogTransitionPolicy{durability: catalogDurableDrop, dropName: name}
	completionOnly := false
	var intentErr error
	if err := m.transitionCatalog(ctx, policy, func(candidate *DatabaseManager) error {
		info := candidate.databases[name]
		if info == nil {
			if !candidate.pendingDrops[name] {
				return ErrDatabaseNotFound
			}
			// Final metadata already applied, but its Sync acknowledgement failed.
			// Complete only the existing fence; never purge a new graph generation.
			candidate.transitionPendingDrop(name, false)
			completionOnly = true
			return nil
		}
		if info.Type == "system" {
			return ErrCannotDropSystemDB
		}
		if info.IsDefault && !candidate.config.AllowDropDefault && !candidate.pendingDrops[name] {
			return ErrCannotDropDefaultDB
		}
		candidate.transitionPendingDrop(name, true)
		return nil
	}); err != nil {
		// Author: NOR-288 lane (Claude), 2026-10-30. An applied intent whose
		// acknowledgement failed is still this owner's drop: its fence is held,
		// so finish it here. The outcome stays non-retryable; no client retry
		// is invited, because a name-only retry could reach a newer database.
		var accepted *storage.AcceptedCommitError
		if completionOnly || !errors.As(err, &accepted) || accepted.State != storage.CommitApplied {
			return err
		}
		intentErr = err
	}
	if completionOnly {
		return nil
	}
	// Receipt publication and durable intent completed before irreversible work.
	// Failures after acceptance are accepted outcomes, never retryable
	// rejections a name-only retry could carry to a newer database.
	handedOff = true
	return m.completeAcceptedDrop(ctx, name, policy, intentErr, release)
}

// completeAcceptedDrop finishes a DROP whose intent is accepted: the data
// purge and the final catalog transition. They are accepted work of the
// storage owner (storage.AcceptedWorkFor) and run under its lifetime, not the
// request's, so a request that ends at any point after acceptance (during the
// purge, while waiting for the catalog gate, before or after the final
// submission) cannot leave the drop unfinished. The DROP admission passes to
// that work and is released when it ends, so a later DROP or CREATE of the
// name is decided after it. The caller waits for the outcome while ctx lasts;
// when ctx ends first, it reports the accepted outcome, which is not
// retryable, and the completion continues. A genuine purge or final-Sync
// failure, or engine Close ending the owner's lifetime, leaves the fence held
// for recovery, a later DROP or CREATE OR REPLACE to finish.
// Author: NOR-288 lane (Claude), 2026-10-31.
func (m *DatabaseManager) completeAcceptedDrop(ctx context.Context, name string, policy catalogTransitionPolicy, intentErr error, release func()) error {
	work, err := storage.AcceptedWorkFor(m.inner)
	if err != nil {
		release()
		return acceptedDropOutcome(err)
	}
	if !work.Enter() {
		release()
		return acceptedDropOutcome(storage.ErrStorageClosed)
	}
	done := make(chan error, 1)
	go func() {
		defer work.Leave()
		var result error
		defer func() { done <- result }()
		defer release()
		result = m.finishAcceptedDrop(work.Context(), name, policy, intentErr)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return acceptedDropOutcome(ctx.Err())
	}
}

// finishAcceptedDrop purges an accepted drop's data and removes its catalog
// record and fence, under the owner's work context.
func (m *DatabaseManager) finishAcceptedDrop(ctx context.Context, name string, policy catalogTransitionPolicy, intentErr error) error {
	if _, _, err := m.inner.DeleteByPrefix(name + ":"); err != nil {
		return acceptedDropOutcome(localizedError(localization.MultidbManagerDeleteDatabaseDataFailed(err), err))
	}
	if err := m.transitionCatalog(ctx, policy, func(candidate *DatabaseManager) error {
		delete(candidate.databases, name)
		candidate.transitionPendingDrop(name, false)
		return nil
	}); err != nil {
		return acceptedDropOutcome(localizedError(localization.MultidbCompositePersistMetadataAfterDropFailed(err), err))
	}
	return intentErr
}

// acceptedDropOutcome reports a failure after this owner accepted a DROP,
// through the one accepted-outcome constructor. Its state is the one the
// failing catalog transition reported (applied with a failed acknowledgement,
// or pending); a failure outside a catalog transition (purge, cancellation)
// is pending. The fence stays held until the drop is finished.
func acceptedDropOutcome(err error) error {
	state := storage.CommitRecoveryPending
	var transition *storage.AcceptedCommitError
	if errors.As(err, &transition) {
		state = transition.State
	}
	return storage.AcceptedOutcome(state, "", err)
}

// dropFenceHeld reports whether name's unavailable-name DROP fence is held.
func (m *DatabaseManager) dropFenceHeld(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pendingDrops[name]
}

// GetStorage returns a namespaced storage engine for the specified database.
//
// The returned engine is scoped to the database - all operations only
// affect data within that namespace.
func (m *DatabaseManager) GetStorage(name string) (storage.Engine, error) {
	return m.GetStorageWithAuth(name, "")
}

// GetStorageWithAuth returns a storage engine for the specified database and forwards
// authToken to remote constituent factories when composite databases include remotes.
func (m *DatabaseManager) GetStorageWithAuth(name string, authToken string) (storage.Engine, error) {
	m.mu.RLock()

	// Check cache first
	if engine, exists := m.engines[name]; exists {
		m.mu.RUnlock()
		return engine, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if engine, exists := m.engines[name]; exists {
		return engine, nil
	}

	// Resolve dotted composite constituent references first (e.g. "cmp.tr").
	// This is required for protocol-level USE routing where the effective database
	// name is a constituent graph reference, not a top-level database metadata key.
	if constituent, resolvedName, ok := m.resolveCompositeConstituentInternal(name); ok {
		if strings.EqualFold(strings.TrimSpace(constituent.ref.Type), "remote") {
			return m.getRemoteStorageResolvedInternal(constituent, authToken)
		}
		engine, _, err := m.databaseViewLocked(resolvedName, true)
		return engine, err
	}

	// Validate database exists
	info, exists := m.databases[name]
	if !exists {
		return nil, ErrDatabaseNotFound
	}

	if m.pendingDrops[name] || info.Status != "online" {
		return nil, ErrDatabaseOffline
	}

	// Handle composite databases differently
	if info.Type == "composite" {
		// Build constituent engines map
		constituents := make(map[string]storage.Engine)
		constituentNames := make(map[string]string)
		accessModes := make(map[string]string)

		for _, ref := range canonicalConstituents(info.Constituents, "") {
			var constituentStorage storage.Engine
			actualName := ref.DatabaseName
			var err error
			resolved := info.runtimeConstituentsByAlias[ref.Alias]
			if resolved == nil {
				m.rebuildRuntimeConstituentsFor(info)
				resolved = info.runtimeConstituentsByAlias[ref.Alias]
			}

			switch ref.Type {
			case "remote":
				if resolved == nil {
					return nil, ErrDatabaseNotFound
				}
				constituentStorage, err = m.getRemoteStorageResolvedInternal(resolved, authToken)
				if err != nil {
					return nil, localizedError(localization.MultidbManagerGetRemoteStorageFailed(ref.Alias, err), err)
				}
			default:
				if resolved == nil || resolved.err != nil {
					if resolved != nil {
						err = resolved.err
					}
					return nil, localizedError(localization.MultidbCompositeConstituentDatabaseNotFound(ref.DatabaseName, err), err)
				}
				actualName = resolved.resolvedDatabaseName

				// Get storage for constituent
				constituentStorage, _, err = m.databaseViewLocked(actualName, true)
				if err != nil {
					return nil, localizedError(localization.MultidbManagerGetConstituentStorageFailed(ref.DatabaseName, err), err)
				}
			}

			constituents[ref.Alias] = constituentStorage
			constituentNames[ref.Alias] = actualName
			accessModes[ref.Alias] = ref.AccessMode
		}

		// Create composite engine with intelligent default routing
		// Routing will be auto-configured based on constituent aliases and access modes
		compositeEngine := storage.NewCompositeEngine(constituents, constituentNames, accessModes)
		compositeEngine.SetCompositeName(name)
		// Note: We don't cache composite engines the same way as they're lightweight wrappers
		return compositeEngine, nil
	}

	engine, _, err := m.databaseViewLocked(name, true)
	return engine, err
}

// resolveCompositeConstituentInternal resolves dotted graph references in the form
// "<composite>.<alias>" to their precomputed constituent entry and backing database name.
// Must be called with lock held.
func (m *DatabaseManager) resolveCompositeConstituentInternal(name string) (*runtimeConstituent, string, bool) {
	dotIdx := strings.IndexByte(name, '.')
	if dotIdx <= 0 || dotIdx >= len(name)-1 {
		return nil, "", false
	}

	compositeName := name[:dotIdx]
	constituentAlias := name[dotIdx+1:]
	if compositeName == "" || constituentAlias == "" {
		return nil, "", false
	}

	info, exists := m.databases[compositeName]
	if !exists || info.Type != "composite" {
		return nil, "", false
	}
	if info.runtimeConstituentsByAlias == nil {
		m.rebuildRuntimeConstituentsFor(info)
	}
	resolved := info.runtimeConstituentsByAlias[constituentAlias]
	if resolved == nil {
		_, ref, found := findCanonicalConstituent(info.Constituents, constituentAlias)
		if !found {
			return nil, "", false
		}
		resolved = info.runtimeConstituentsByAlias[ref.Alias]
		if resolved == nil {
			return nil, "", false
		}
	}
	if resolved.err != nil && !strings.EqualFold(strings.TrimSpace(resolved.ref.Type), "remote") {
		return nil, "", false
	}
	return resolved, resolved.resolvedDatabaseName, true
}

// databaseViewLocked is the single lookup/construction owner for a namespace's
// storage view and quota checker. A checker-only request never opens storage or
// invokes runtime callbacks. The caller holds m.mu; cached views stay unchanged.
func (m *DatabaseManager) databaseViewLocked(name string, includeStorage bool) (storage.Engine, *databaseLimitChecker, error) {
	if includeStorage {
		if m.pendingDrops[name] {
			return nil, nil, ErrDatabaseOffline
		}
		if engine, exists := m.engines[name]; exists {
			return engine, m.limitCheckers[name], nil
		}
	}
	info := m.databases[name]
	if info == nil {
		return nil, nil, ErrDatabaseNotFound
	}
	if includeStorage && info.Status != "online" {
		return nil, nil, ErrDatabaseOffline
	}
	checker := m.limitCheckers[name]
	if checker == nil {
		checker = &databaseLimitChecker{manager: m, databaseName: name, localPolicy: &databaseLimitPolicy{}}
		checker.localPolicy.updateLimits(info.limitsSnapshot())
		checker.info.Store(info)
		m.limitCheckers[name] = checker
	}
	if !includeStorage {
		return nil, checker, nil
	}
	engine := newSizeTrackingEngine(storage.NewNamespacedEngine(m.inner, name), m, name, checker)
	m.engines[name] = engine
	return engine, checker, nil
}

// getRemoteStorageResolvedInternal creates an engine from a precomputed remote identity.
// Must be called with lock held.
func (m *DatabaseManager) getRemoteStorageResolvedInternal(ref *runtimeConstituent, authToken string) (storage.Engine, error) {
	if ref == nil {
		return nil, ErrDatabaseNotFound
	}
	if m.remoteEngineFactory == nil {
		return nil, localizedError(localization.MultidbManagerRemoteEngineFactoryNotConfigured(ref.ref.Alias), nil)
	}
	runtimeRef := ref.storedRef
	resolveErr := ref.err
	if !ref.explicitAuth && strings.TrimSpace(authToken) != "" {
		runtimeRef = ref.forwardedRef
		resolveErr = ref.forwardedErr
	}
	if resolveErr != nil {
		return nil, resolveErr
	}
	engine, err := m.remoteEngineFactory(runtimeRef, authToken)
	if err != nil {
		secrets := ref.redactionValues
		if authToken != "" {
			secrets = append(append([]string(nil), ref.redactionValues...), authToken)
		}
		redactedErr := observability.RedactError(err, secrets...)
		if ref.redactedIdentity != "" {
			return nil, fmt.Errorf("remote constituent %s: %w", ref.redactedIdentity, redactedErr)
		}
		return nil, redactedErr
	}
	if engine == nil {
		return nil, localizedError(localization.MultidbManagerRemoteEngineFactoryReturnedNil(ref.ref.Alias), nil)
	}
	return engine, nil
}

// GetDefaultStorage returns storage for the default database.
func (m *DatabaseManager) GetDefaultStorage() (storage.Engine, error) {
	return m.GetStorage(m.config.DefaultDatabase)
}

// ListDatabases returns all database info.
func (m *DatabaseManager) ListDatabases() []*DatabaseInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*DatabaseInfo, 0, len(m.databases))
	for _, info := range m.databases {
		result = append(result, cloneDatabaseInfo(info, m.pendingDrops[info.Name], databaseSnapshotVisible))
	}
	return result
}

// GetDatabase returns info for a specific database.
func (m *DatabaseManager) GetDatabase(name string) (*DatabaseInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.databases[name]
	if !exists {
		return nil, ErrDatabaseNotFound
	}

	return cloneDatabaseInfo(info, m.pendingDrops[info.Name], databaseSnapshotVisible), nil
}

// databaseSnapshotPurpose distinguishes complete catalog state from the
// canonical winner projection exposed by public metadata readers.
type databaseSnapshotPurpose uint8

const (
	databaseSnapshotFull databaseSnapshotPurpose = iota
	databaseSnapshotVisible
)

// cloneDatabaseInfo returns a deep-enough, lock-safe snapshot of DatabaseInfo.
// Caller must hold m.mu (read or write) before invoking.
func cloneDatabaseInfo(info *DatabaseInfo, pendingDrop bool, purpose databaseSnapshotPurpose) *DatabaseInfo {
	status := info.Status
	if pendingDrop {
		status = "offline"
	}
	limitsCopy := info.limitsSnapshot()

	aliases := append([]string(nil), info.Aliases...)
	var constituents []ConstituentRef
	if purpose == databaseSnapshotVisible {
		constituents = snapshotConstituents(info.Constituents)
	} else {
		// Admission retains exact-distinct folded registrations. A hidden earlier
		// registration must survive persistence so removing the winner after reopen
		// exposes the same graph as removing it before reopen.
		constituents = append([]ConstituentRef(nil), info.Constituents...)
	}

	accounting := info.accountingOwner()
	accounting.sizeMu.RLock()
	totalSize := accounting.totalSize
	nodeSize := accounting.nodeSize
	edgeSize := accounting.edgeSize
	sizeInitialized := accounting.sizeInitialized
	accounting.sizeMu.RUnlock()

	return &DatabaseInfo{
		ID:              info.ID,
		Name:            info.Name,
		CreatedAt:       info.CreatedAt,
		CreatedBy:       info.CreatedBy,
		Status:          status,
		Type:            info.Type,
		IsDefault:       info.IsDefault,
		NodeCount:       info.NodeCount,
		UpdatedAt:       info.UpdatedAt,
		Aliases:         aliases,
		Limits:          limitsCopy,
		Constituents:    constituents,
		Options:         maps.Clone(info.Options),
		totalSize:       totalSize,
		nodeSize:        nodeSize,
		edgeSize:        edgeSize,
		sizeInitialized: sizeInitialized,
	}
}

// Exists checks if a database exists.
func (m *DatabaseManager) Exists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.databases[name] != nil
}

// DefaultDatabaseName returns the default database name.
func (m *DatabaseManager) DefaultDatabaseName() string {
	// Never advertise the system database as the default.
	if m.config.DefaultDatabase == m.config.SystemDatabase {
		return "nornic"
	}
	return m.config.DefaultDatabase
}

// SetDatabaseStatus sets a database online/offline.
func (m *DatabaseManager) SetDatabaseStatus(ctx context.Context, name, status string) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {

		info, exists := m.databases[name]
		if !exists {
			return ErrDatabaseNotFound
		}

		if status != "online" && status != "offline" {
			return localizedError(localization.MultidbManagerInvalidStatus(status), nil)
		}

		info.Status = status
		info.UpdatedAt = time.Now()

		return nil

	})
}

// Close releases resources.
func (m *DatabaseManager) Close() error {
	m.mu.Lock()

	// Clear all cached engines
	m.engines = make(map[string]storage.Engine)
	m.limitCheckers = make(map[string]*databaseLimitChecker)
	m.statementCachesClosed = true
	m.disposeStatementCachesLocked()

	// Native close can wait for accepted work. Its terminal callback needs the
	// metadata lock to publish, so release it before entering that owner.
	m.unlockStatementCacheIndexes(true, true)
	return m.inner.Close()
}

// ResolveDatabase resolves an alias or database name to the actual database name.
func (m *DatabaseManager) ResolveDatabase(nameOrAlias string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if it's an actual database name
	if _, exists := m.databases[nameOrAlias]; exists {
		return nameOrAlias, nil
	}

	// Check if it's an alias
	for dbName, info := range m.databases {
		for _, alias := range info.Aliases {
			if alias == nameOrAlias {
				return dbName, nil
			}
		}
	}

	return "", ErrDatabaseNotFound
}

// ResolveConnectionLimitOwner resolves a Bolt database selection to the local
// database whose connection limit owns it. Local composite constituents are
// charged to their underlying database; remote constituents use the composite.
func (m *DatabaseManager) ResolveConnectionLimitOwner(selection string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if dot := strings.IndexByte(selection, '.'); dot > 0 && dot < len(selection)-1 {
		compositeName, err := m.resolveDatabaseInternal(selection[:dot])
		if err != nil {
			return "", err
		}
		info := m.databases[compositeName]
		if info == nil || info.Type != "composite" {
			return "", ErrDatabaseNotFound
		}
		_, constituent, found := findCanonicalConstituent(info.Constituents, selection[dot+1:])
		if !found {
			return "", ErrDatabaseNotFound
		}
		if constituent.Type == "local" {
			return m.resolveDatabaseInternal(constituent.DatabaseName)
		}
		return compositeName, nil
	}
	return m.resolveDatabaseInternal(selection)
}

// CreateAlias creates an alias for a database (Neo4j-compatible).
func (m *DatabaseManager) CreateAlias(ctx context.Context, alias, databaseName string) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {

		// Validate target database exists
		info, exists := m.databases[databaseName]
		if !exists {
			return ErrDatabaseNotFound
		}

		// Validate alias doesn't conflict with existing database name
		// NOTE: do NOT call m.Exists() here — we already hold m.mu.Lock() and
		// Exists() acquires m.mu.RLock(), which deadlocks on a non-re-entrant RWMutex.
		if m.databases[alias] != nil {
			return ErrAliasConflict
		}

		// Validate alias name
		if err := m.validateAliasName(alias); err != nil {
			return err
		}

		// Check if alias is already used by another database
		for _, dbInfo := range m.databases {
			for _, existingAlias := range dbInfo.Aliases {
				if existingAlias == alias {
					return ErrAliasExists
				}
			}
		}

		// Add alias
		if info.Aliases == nil {
			info.Aliases = []string{}
		}
		info.Aliases = append(info.Aliases, alias)
		info.UpdatedAt = time.Now()

		return nil

	})
}

// DropAlias removes an alias (Neo4j-compatible).
func (m *DatabaseManager) DropAlias(ctx context.Context, alias string) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {

		// Find database with this alias
		for _, info := range m.databases {
			for i, existingAlias := range info.Aliases {
				if existingAlias == alias {
					// Remove alias
					info.Aliases = slices.Delete(info.Aliases, i, i+1)
					info.UpdatedAt = time.Now()
					return nil
				}
			}
		}

		return ErrAliasNotFound

	})
}

// ListAliases returns all aliases for a database, or all aliases if database is empty.
func (m *DatabaseManager) ListAliases(databaseName string) map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]string)

	if databaseName != "" {
		// List aliases for specific database
		if info, exists := m.databases[databaseName]; exists {
			for _, alias := range info.Aliases {
				result[alias] = databaseName
			}
		}
	} else {
		// List all aliases
		for dbName, info := range m.databases {
			for _, alias := range info.Aliases {
				result[alias] = dbName
			}
		}
	}

	return result
}

// SetDatabaseLimits sets resource limits for a database.
func (m *DatabaseManager) SetDatabaseLimits(ctx context.Context, databaseName string, limits *Limits) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(candidate *DatabaseManager) error {
		info := candidate.databases[databaseName]
		if info == nil || candidate.pendingDrops[databaseName] {
			return ErrDatabaseNotFound
		}
		info.Limits = cloneLimits(limits)
		info.UpdatedAt = time.Now()
		return nil
	})
}

// GetDatabaseLimits returns resource limits for a database.
func (m *DatabaseManager) GetDatabaseLimits(databaseName string) (*Limits, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.databases[databaseName]
	if !exists {
		return nil, ErrDatabaseNotFound
	}

	return info.limitsSnapshot(), nil
}

func cloneLimits(limits *Limits) *Limits {
	if limits == nil {
		return nil
	}
	copy := *limits
	return &copy
}

// validateAliasName validates an alias name.
func (m *DatabaseManager) validateAliasName(alias string) error {
	if alias == "" {
		return ErrInvalidAliasName
	}

	// Alias cannot contain whitespace
	if strings.ContainsAny(alias, " \t\n\r") {
		return localizedError(localization.MultidbManagerAliasContainsWhitespace(alias, ErrInvalidAliasName), ErrInvalidAliasName)
	}

	// Alias cannot be reserved names
	reserved := []string{"system", m.config.DefaultDatabase}
	for _, reservedName := range reserved {
		if alias == reservedName {
			return localizedError(localization.MultidbManagerAliasReserved(alias, ErrInvalidAliasName), ErrInvalidAliasName)
		}
	}

	return nil
}

// IncrementStorageSize increments the tracked storage size for a database.
//
// This should be called after successful node/edge creation to maintain accurate
// size tracking for MaxBytes limit enforcement. The sizes should be calculated
// using the same gob encoding used by the storage engine.
//
// Parameters:
//   - databaseName: The database to update
//   - nodeSize: Size in bytes of the node that was created (0 if no node created)
//   - edgeSize: Size in bytes of the edge that was created (0 if no edge created)
//
// Example:
//
//	// After successfully creating a node
//	nodeSize, _ := calculateNodeSize(node)
//	manager.IncrementStorageSize("tenant_a", nodeSize, 0)
//
//	// After successfully creating an edge
//	edgeSize, _ := calculateEdgeSize(edge)
//	manager.IncrementStorageSize("tenant_a", 0, edgeSize)
//
// Thread-safe: This method is safe to call from multiple goroutines.
func (m *DatabaseManager) IncrementStorageSize(databaseName string, nodeSize, edgeSize int64) {
	m.applyStorageSizeDelta(databaseName, nodeSize, edgeSize)
}

// DecrementStorageSize decrements the tracked storage size for a database.
//
// This should be called after successful node/edge deletion to maintain accurate
// size tracking for MaxBytes limit enforcement. The sizes should be the same
// values that were used when the entities were created.
//
// Parameters:
//   - databaseName: The database to update
//   - nodeSize: Size in bytes of the node that was deleted (0 if no node deleted)
//   - edgeSize: Size in bytes of the edge that was deleted (0 if no edge deleted)
//
// Example:
//
//	// After successfully deleting a node (size known from creation)
//	manager.DecrementStorageSize("tenant_a", nodeSize, 0)
//
//	// After successfully deleting an edge (size known from creation)
//	manager.DecrementStorageSize("tenant_a", 0, edgeSize)
//
// Thread-safe: This method is safe to call from multiple goroutines.
// Defensive: Size is prevented from going negative (resets to 0 if underflow).
func (m *DatabaseManager) DecrementStorageSize(databaseName string, nodeSize, edgeSize int64) {
	m.applyStorageSizeDelta(databaseName, -nodeSize, -edgeSize)
}

// GetStorageSize returns the current tracked storage size for a database.
//
// Returns:
//   - totalSize: Total storage size in bytes (sum of all nodes and edges)
//   - nodeSize: Total size of all nodes in bytes
//   - edgeSize: Total size of all edges in bytes
//
// The size is tracked incrementally and initialized lazily on first access.
// This provides O(1) access for limit checking without recalculating from all entities.
//
// Example:
//
//	totalSize, nodeSize, edgeSize := manager.GetStorageSize(ctx, "tenant_a")
//	fmt.Printf("Database uses %d bytes (%d from nodes, %d from edges)\n",
//		totalSize, nodeSize, edgeSize)
//
// Thread-safe: This method is safe to call from multiple goroutines.
func (m *DatabaseManager) GetStorageSize(ctx context.Context, databaseName string) (int64, int64, int64) {
	total, nodes, edges, _ := m.readStorageSize(ctx, databaseName, nil, sizeForReporting)
	return total, nodes, edges
}

// readStorageSize resolves the actual reader before lazy initialization. Manager
// lookup locks are released before storage callbacks; the canonical accounting
// owner supplies all scalar counters even if initialization reports an error.
type storageSizeReadMode uint8

const (
	sizeForReporting storageSizeReadMode = iota
	sizeForAdmission
)

func (m *DatabaseManager) readStorageSize(ctx context.Context, databaseName string, engine storage.Engine, mode storageSizeReadMode) (int64, int64, int64, error) {
	m.mu.RLock()
	info := m.databases[databaseName]
	if info != nil && engine == nil {
		if cached, ok := m.engines[databaseName]; ok {
			engine = cached
		} else {
			engine = storage.NewNamespacedEngine(m.inner, databaseName)
		}
	}
	m.mu.RUnlock()
	if info == nil {
		return 0, 0, 0, ErrDatabaseNotFound
	}
	err := m.ensureStorageSizeInitialized(ctx, databaseName, engine)
	if mode == sizeForAdmission {
		if err != nil {
			return 0, 0, 0, err
		}
		// Admission retains its original post-callback catalog check. Reporting
		// retains its original captured-info read and ignored initialization error.
		m.mu.RLock()
		info = m.databases[databaseName]
		m.mu.RUnlock()
		if info == nil {
			return 0, 0, 0, ErrDatabaseNotFound
		}
	}
	info = info.accountingOwner()
	info.sizeMu.RLock()
	defer info.sizeMu.RUnlock()
	return info.totalSize, info.nodeSize, info.edgeSize, err
}
