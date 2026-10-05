package fabric

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// RemoteFragmentExecutor executes fragments against a remote NornicDB instance
// via the RemoteEngine transport (Bolt or HTTP, auto-detected from URI scheme).
type RemoteFragmentExecutor struct {
	// engineCache caches RemoteEngine instances by URI+database key to avoid
	// reconnecting on every fragment execution.
	engineCache map[string]*storage.RemoteEngine
	txHandles   map[string]storage.RemoteCypherTx
	mu          sync.RWMutex
}

// NewRemoteFragmentExecutor creates a remote executor.
func NewRemoteFragmentExecutor() *RemoteFragmentExecutor {
	return &RemoteFragmentExecutor{
		engineCache: make(map[string]*storage.RemoteEngine),
		txHandles:   make(map[string]storage.RemoteCypherTx),
	}
}

// Execute runs a Cypher query against a remote NornicDB instance.
//
// Parameters:
//   - ctx: context for cancellation/deadline propagation
//   - loc: the remote location (URI, database, auth config)
//   - query: the Cypher query to execute
//   - params: query parameters
//   - authToken: the caller's auth token for OIDC forwarding
func (r *RemoteFragmentExecutor) Execute(ctx context.Context, loc *LocationRemote, query string, params map[string]interface{}, authToken string) (*ResultStream, error) {
	engine, err := r.getOrCreateEngine(loc, authToken)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to remote '%s' at %s: %w", loc.DBName, loc.URI, err)
	}

	columns, rows, err := r.executeRemoteQuery(ctx, engine, query, params)
	if err != nil {
		return nil, fmt.Errorf("remote execution on '%s' (%s) failed: %w", loc.DBName, loc.URI, err)
	}

	return &ResultStream{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// ExecuteRows runs a Cypher query against a remote location and returns a row iterator.
func (r *RemoteFragmentExecutor) ExecuteRows(ctx context.Context, loc *LocationRemote, query string, params map[string]interface{}, authToken string) ([]string, RowIterator, error) {
	res, err := r.Execute(ctx, loc, query, params, authToken)
	if err != nil {
		return nil, nil, err
	}
	if res == nil {
		return nil, NewResultRowIterator(nil), nil
	}
	return res.Columns, NewResultRowIterator(res), nil
}

// Close closes all cached remote engines.
func (r *RemoteFragmentExecutor) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var lastErr error
	for key, handle := range r.txHandles {
		if err := handle.Rollback(context.Background()); err != nil {
			lastErr = fmt.Errorf("failed to rollback remote tx handle '%s': %w", key, err)
		}
		delete(r.txHandles, key)
	}
	for key, engine := range r.engineCache {
		if err := engine.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close remote engine '%s': %w", key, err)
		}
		delete(r.engineCache, key)
	}
	return lastErr
}

// cacheKey produces a unique key for engine caching.
func cacheKey(loc *LocationRemote, authToken string) string {
	modeRaw := strings.TrimSpace(loc.AuthMode)
	mode := modeRaw
	if mode == "" || strings.EqualFold(mode, "oidc_forwarding") {
		mode = "oidc_forwarding"
	} else if strings.EqualFold(mode, "user_password") {
		mode = "user_password"
	}

	authIdentity := "none"
	switch mode {
	case "user_password":
		authIdentity = strings.TrimSpace(loc.User) + ":" + strings.TrimSpace(loc.Password)
	default:
		// oidc_forwarding: include forwarded token context to prevent cross-caller reuse.
		authIdentity = strings.TrimSpace(authToken)
	}

	sum := fnv64aString(authIdentity)
	var b strings.Builder
	// uri|db|mode|authhash
	b.Grow(len(loc.URI) + len(loc.DBName) + len(mode) + 20)
	b.WriteString(loc.URI)
	b.WriteByte('|')
	b.WriteString(loc.DBName)
	b.WriteByte('|')
	b.WriteString(mode)
	b.WriteByte('|')
	b.WriteString(strconv.FormatUint(sum, 16))
	return b.String()
}

func fnv64aString(s string) uint64 {
	var h uint64 = 14695981039346656037
	const prime uint64 = 1099511628211
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

// getOrCreateEngine returns a cached engine or creates a new one.
func (r *RemoteFragmentExecutor) getOrCreateEngine(loc *LocationRemote, authToken string) (*storage.RemoteEngine, error) {
	key := cacheKey(loc, authToken)

	r.mu.RLock()
	if engine, exists := r.engineCache[key]; exists {
		r.mu.RUnlock()
		return engine, nil
	}
	r.mu.RUnlock()

	// Creating an engine doesn't connect, so it happens under the lock and a
	// concurrent caller that created it first is reused.
	r.mu.Lock()
	defer r.mu.Unlock()
	engine, exists := r.engineCache[key]
	if !exists {
		cfg := storage.RemoteEngineConfig{
			URI:      loc.URI,
			Database: loc.DBName,
		}

		switch loc.AuthMode {
		case "user_password":
			cfg.User = loc.User
			cfg.Password = loc.Password
		default:
			// oidc_forwarding: forward the caller's auth token.
			cfg.AuthToken = authToken
		}

		created, err := storage.NewRemoteEngine(cfg)
		if err != nil {
			return nil, err
		}
		engine = created
		r.engineCache[key] = engine
	}
	return engine, nil
}

// executeRemoteQuery runs a Cypher query via a RemoteEngine and returns columns + rows.
// This uses the engine's GetNodesByLabel as a transport mechanism for arbitrary Cypher.
// The RemoteEngine.transport.query method handles the actual Bolt/HTTP execution.
func (r *RemoteFragmentExecutor) executeRemoteQuery(ctx context.Context, engine *storage.RemoteEngine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	sub, hasSub := SubTransactionFromContext(ctx)
	if !hasSub || sub == nil {
		return engine.QueryCypher(ctx, query, params)
	}
	handle, err := r.getOrCreateTxHandle(ctx, sub, engine)
	if err != nil {
		return nil, nil, err
	}
	return handle.QueryCypher(ctx, query, params)
}

func (r *RemoteFragmentExecutor) getOrCreateTxHandle(ctx context.Context, sub *SubTransaction, engine *storage.RemoteEngine) (storage.RemoteCypherTx, error) {
	r.mu.RLock()
	if handle, ok := r.txHandles[sub.ShardName]; ok {
		r.mu.RUnlock()
		return handle, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if handle, ok := r.txHandles[sub.ShardName]; ok {
		return handle, nil
	}
	handle, err := engine.BeginCypherTx(ctx)
	if err != nil {
		return nil, err
	}
	tx, ok := FabricTransactionFromContext(ctx)
	if !ok || tx == nil {
		_ = handle.Rollback(ctx)
		return nil, fmt.Errorf("fabric transaction context is missing for remote sub-transaction '%s'", sub.ShardName)
	}
	commitFn := func(_ *SubTransaction) error { return handle.Commit(ctx) }
	rollbackFn := func(_ *SubTransaction) error { return handle.Rollback(ctx) }
	if err := tx.BindParticipantCallbacks(sub.ShardName, commitFn, rollbackFn); err != nil {
		_ = handle.Rollback(ctx)
		return nil, err
	}
	r.txHandles[sub.ShardName] = handle
	return handle, nil
}
