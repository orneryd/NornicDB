package fabric

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Remote factories may decorate an engine. Fabric depends on its Cypher
// capabilities rather than the concrete transport implementation.
type remoteCypherEngine interface {
	QueryCypher(context.Context, string, map[string]interface{}, storage.RemoteAccessMode) ([]string, [][]interface{}, error)
	BeginCypherTx(context.Context) (storage.RemoteCypherTx, error)
	Close() error
}

// RemoteFragmentExecutor executes fragments against a remote NornicDB instance
// via the RemoteEngine transport (Bolt or HTTP, auto-detected from URI scheme).
// Execute and ExecuteRows accept raw remote source. Compiler fragments emit
// accepted source back to raw before this transport boundary.
type RemoteFragmentExecutor struct {
	// engineCache caches RemoteEngine instances by endpoint and exact auth identity to avoid
	// reconnecting on every fragment execution.
	engineCache map[remoteEngineIdentity]remoteCypherEngine
	txHandles   map[string]*remoteParticipant
	mu          sync.RWMutex
}

// remoteParticipant is a remote sub-transaction, which ends once: after
// RollbackParticipants rolled it back early, the transaction's COMMIT,
// ROLLBACK and Close report that outcome instead of ending it again.
type remoteParticipant struct {
	tx        storage.RemoteCypherTx
	mu        sync.Mutex
	ended     bool
	committed bool
	done      chan struct{} // closed when the end that ran has returned
	err       error         // the outcome of the end that ran, set before done closes
}

// end commits or rolls back the sub-transaction, the first time it is
// called; a later call returns that outcome (once it is known) and never
// commits a rolled-back sub-transaction. The network call holds no lock. A
// Bolt call that ctx's deadline stopped records that: the remote server then
// ends the sub-transaction itself, the driver having closed its connection.
// A sub-transaction the connection doesn't end is rolled back detached from
// ctx (rollbackDetached).
func (p *remoteParticipant) end(ctx context.Context, commit bool) error {
	p.mu.Lock()
	if p.ended {
		committed, done := p.committed, p.done
		p.mu.Unlock()
		if commit && !committed {
			return fmt.Errorf("remote sub-transaction was rolled back")
		}
		<-done
		return p.err
	}
	p.ended, p.committed, p.done = true, commit, make(chan struct{})
	p.mu.Unlock()
	if independent, ok := p.tx.(interface{ OutlivesConnection() bool }); ok && !commit && independent.OutlivesConnection() {
		return p.rollbackDetached(ctx)
	}
	var err error
	if commit {
		err = p.tx.Commit(ctx)
	} else {
		err = p.tx.Rollback(ctx)
	}
	if err != nil && ctx.Err() != nil {
		err = fmt.Errorf("remote sub-transaction did not end before its deadline: %w (%w)", ctx.Err(), err)
	}
	p.err = err
	close(p.done)
	return err
}

// endWithin ends the participant within storage.RemoteRequestTimeout.
func (p *remoteParticipant) endWithin(ctx context.Context, commit bool) error {
	ctx, cancel := context.WithTimeout(ctx, storage.RemoteRequestTimeout)
	defer cancel()
	return p.end(ctx, commit)
}

// rollbackDetached rolls back a sub-transaction that outlives its connection
// (the HTTP transaction API's), whose rollback must reach the server: it runs
// detached from ctx, bounded by storage.RemoteRequestTimeout. end returns by
// ctx's deadline even while it runs; its outcome is recorded when it ends,
// and every later end of the participant (Close included) waits for it.
func (p *remoteParticipant) rollbackDetached(ctx context.Context) error {
	go func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storage.RemoteRequestTimeout)
		defer cancel()
		p.err = p.tx.Rollback(rollbackCtx)
		close(p.done)
	}()
	select {
	case <-p.done:
		return p.err
	case <-ctx.Done():
		return fmt.Errorf("remote sub-transaction did not end before its deadline: %w; its rollback continues", ctx.Err())
	}
}

// NewRemoteFragmentExecutor creates a remote executor.
func NewRemoteFragmentExecutor() *RemoteFragmentExecutor {
	return &RemoteFragmentExecutor{
		engineCache: make(map[remoteEngineIdentity]remoteCypherEngine),
		txHandles:   make(map[string]*remoteParticipant),
	}
}

// RollbackParticipants rolls back every remote sub-transaction now, for a
// transaction that failed and can no longer commit: each remote server
// releases what its sub-transaction holds (MERGE key locks) instead of
// keeping it until the client ends the transaction. The participants roll
// back concurrently, so ctx's deadline bounds the whole call and one that
// doesn't respond doesn't hold up the others. A rollback error (a deadline
// included) is recorded on its participant, which reports it when the
// transaction ends; the first one is returned.
func (r *RemoteFragmentExecutor) RollbackParticipants(ctx context.Context) error {
	r.mu.RLock()
	participants := make([]*remoteParticipant, 0, len(r.txHandles))
	for _, participant := range r.txHandles {
		participants = append(participants, participant)
	}
	r.mu.RUnlock()
	errs := make([]error, len(participants))
	var wg sync.WaitGroup
	for i, participant := range participants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = participant.end(ctx, false)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Execute runs a Cypher query against a remote NornicDB instance.
//
// Parameters:
//   - ctx: context for cancellation/deadline propagation
//   - loc: the remote location (URI, database, auth config)
//   - query: the Cypher query to execute
//   - params: query parameters
//   - authToken: the caller's auth token for OIDC forwarding
func (r *RemoteFragmentExecutor) Execute(ctx context.Context, loc *LocationRemote, query string, params map[string]interface{}, authToken string, mode storage.RemoteAccessMode) (*ResultStream, error) {
	engine, err := r.getOrCreateEngine(loc, authToken)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to remote '%s' at %s: %w", loc.DBName, observability.RedactURL(loc.URI), err)
	}

	columns, rows, err := r.executeRemoteQuery(ctx, engine, query, params, mode)
	if err != nil {
		return nil, fmt.Errorf("remote execution on '%s' (%s) failed: %w", loc.DBName, observability.RedactURL(loc.URI), err)
	}

	return &ResultStream{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// ExecuteRows runs a Cypher query against a remote location and returns a row iterator.
func (r *RemoteFragmentExecutor) ExecuteRows(ctx context.Context, loc *LocationRemote, query string, params map[string]interface{}, authToken string, mode storage.RemoteAccessMode) ([]string, RowIterator, error) {
	return resultRows(r.Execute(ctx, loc, query, params, authToken, mode))
}

// Close closes all cached remote engines.
func (r *RemoteFragmentExecutor) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var lastErr error
	for key, handle := range r.txHandles {
		if err := handle.end(context.Background(), false); err != nil {
			lastErr = fmt.Errorf("failed to rollback remote tx handle '%s': %w", observability.RedactText(key), err)
		}
		delete(r.txHandles, key)
	}
	for key, engine := range r.engineCache {
		if err := engine.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close remote engine '%s': %w", observability.RedactText(key.uri+"|"+key.database), err)
		}
		delete(r.engineCache, key)
	}
	return lastErr
}

// remoteEngineIdentity keeps admitted credential fields distinct and exact.
// It must never be formatted into diagnostics because it contains credentials.
type remoteEngineIdentity struct {
	graph, uri, database, mode, user, password, token, revision string
}

func cacheKey(loc *LocationRemote, authToken string) remoteEngineIdentity {
	mode := strings.TrimSpace(loc.AuthMode)
	if mode == "" || strings.EqualFold(mode, "oidc_forwarding") {
		mode = "oidc_forwarding"
	} else if strings.EqualFold(mode, "user_password") {
		mode = "user_password"
	}
	key := remoteEngineIdentity{uri: loc.URI, database: loc.DBName, mode: mode, revision: loc.CredentialRevision}
	if loc.ResolveEngine != nil {
		key.graph = loc.GraphName
	}
	if mode == "user_password" {
		key.user, key.password = loc.User, loc.Password
	} else {
		key.token = strings.TrimSpace(authToken)
	}
	return key
}

// getOrCreateEngine returns a cached engine or creates a new one.
func (r *RemoteFragmentExecutor) getOrCreateEngine(loc *LocationRemote, authToken string) (remoteCypherEngine, error) {
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
		if loc.ResolveEngine != nil {
			resolved, err := loc.ResolveEngine(authToken)
			if err != nil {
				return nil, err
			}
			remote, ok := resolved.(remoteCypherEngine)
			if !ok {
				return nil, fmt.Errorf("remote constituent '%s' resolved to %T", loc.GraphName, resolved)
			}
			r.engineCache[key] = remote
			return remote, nil
		}
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

// executeRemoteQuery uses the factory's Cypher capabilities for implicit or
// explicit transactions, preserving decorator behavior on both paths.
func (r *RemoteFragmentExecutor) executeRemoteQuery(ctx context.Context, engine remoteCypherEngine, query string, params map[string]interface{}, mode storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
	sub, hasSub := SubTransactionFromContext(ctx)
	if !hasSub || sub == nil {
		return engine.QueryCypher(ctx, query, params, mode)
	}
	handle, err := r.getOrCreateTxHandle(ctx, sub, engine)
	if err != nil {
		return nil, nil, err
	}
	columns, rows, err := handle.tx.QueryCypher(ctx, query, params)
	if err != nil {
		return nil, nil, err
	}
	write, known := false, false
	if reporter, ok := handle.tx.(storage.RemoteCypherMode); ok {
		write, known = reporter.ResolvedWriteMode()
	}
	if write || !known && unresolvedRemoteMode(ctx) {
		tx, ok := FabricTransactionFromContext(ctx)
		if !ok || tx == nil {
			return nil, nil, fmt.Errorf("fabric transaction context is missing for remote sub-transaction '%s'", sub.ShardName)
		}
		if _, err := tx.GetOrOpen(sub.ShardName, true); err != nil {
			return nil, nil, err
		}
	}
	return columns, rows, nil
}

func (r *RemoteFragmentExecutor) getOrCreateTxHandle(ctx context.Context, sub *SubTransaction, engine remoteCypherEngine) (*remoteParticipant, error) {
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
	tx, err := engine.BeginCypherTx(storage.WithCoordinatorOwnedTransaction(ctx))
	if err != nil {
		return nil, err
	}
	handle := &remoteParticipant{tx: tx}
	fabricTx, ok := FabricTransactionFromContext(ctx)
	if !ok || fabricTx == nil {
		_ = handle.end(ctx, false)
		return nil, fmt.Errorf("fabric transaction context is missing for remote sub-transaction '%s'", sub.ShardName)
	}
	// The sub-transaction ends with its transaction, after the statement or
	// APPLY batch that enlisted it has returned and cancelled ctx. The
	// transaction owns commit; rollback cleanup remains independent of its
	// cancellation. The existing remote terminal owner bounds both.
	terminalCtx := fabricTx.BindContext(ctx)
	rollbackCtx := context.WithoutCancel(ctx)
	commitFn := func(_ *SubTransaction) error { return handle.endWithin(terminalCtx, true) }
	rollbackFn := func(_ *SubTransaction) error { return handle.endWithin(rollbackCtx, false) }
	if err := fabricTx.BindParticipantCallbacks(sub.ShardName, commitFn, rollbackFn); err != nil {
		_ = handle.end(ctx, false)
		return nil, err
	}
	r.txHandles[sub.ShardName] = handle
	return handle, nil
}
