package fabric

import (
	"context"
	"fmt"
	"strings"
	"sync"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Remote factories may decorate an engine. Fabric depends on its Cypher
// capabilities rather than the concrete transport implementation.
type remoteCypherEngine interface {
	QueryCypher(context.Context, string, map[string]interface{}, storage.RemoteAccessMode) ([]string, [][]interface{}, error)
	BeginCypherTx(context.Context, storage.RemoteAccessMode) (storage.RemoteCypherTx, error)
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

// remoteCompletion records one terminal RPC. Its result is immutable after
// done closes; commit and cleanup have distinct outcomes.
type remoteCompletion struct {
	done     chan struct{}
	err      error
	panicked any
}

// remoteParticipant owns one commit or rollback, plus at most one cleanup
// rollback after a failed commit. Cleanup never changes the commit's outcome.
type remoteParticipant struct {
	tx        storage.RemoteCypherTx
	write     bool
	mu        sync.Mutex
	committed bool // primary operation requested commit
	primary   *remoteCompletion
	cleanup   *remoteCompletion
}

func (p *remoteParticipant) end(ctx context.Context, commit bool) error {
	p.mu.Lock()
	if p.primary == nil {
		result := &remoteCompletion{done: make(chan struct{})}
		p.primary, p.committed = result, commit
		p.mu.Unlock()
		return p.complete(ctx, commit, result)
	}
	primary, committed := p.primary, p.committed
	p.mu.Unlock()
	<-primary.done
	if commit && !committed {
		if primary.err != nil {
			return fmt.Errorf("remote sub-transaction rollback failed: %w", primary.err)
		}
		return fmt.Errorf("remote sub-transaction was rolled back")
	}
	if !commit && committed {
		if primary.err == nil {
			return nil
		}
		// Failed COMMIT may leave a remote session alive. ROLLBACK is cleanup,
		// never a replay of COMMIT and never evidence that a lost write was undone.
		p.mu.Lock()
		result := p.cleanup
		owner := result == nil
		if owner {
			result = &remoteCompletion{done: make(chan struct{})}
			p.cleanup = result
		}
		p.mu.Unlock()
		if owner {
			return p.complete(ctx, false, result)
		}
		<-result.done
		return result.err
	}
	return primary.err
}

// complete is the sole terminal RPC runner. A transport panic publishes its
// result before rethrowing on the waiting owner, so other finalizers can finish.
func (p *remoteParticipant) complete(ctx context.Context, commit bool, result *remoteCompletion) error {
	run := func(workCtx context.Context) {
		defer close(result.done)
		defer func() {
			if cause := recover(); cause != nil {
				result.panicked = cause
				if commit {
					result.err = fmt.Errorf("%w: remote commit panicked: %v", ErrCommitOutcomeUnknown, cause)
				} else {
					result.err = fmt.Errorf("remote rollback panicked: %v", cause)
				}
			}
		}()
		if commit {
			result.err = p.tx.Commit(workCtx)
		} else {
			result.err = p.tx.Rollback(workCtx)
		}
		if result.err != nil && workCtx.Err() != nil {
			result.err = nornicerrors.CombineTerminalOutcomes(fmt.Errorf("remote sub-transaction did not end before its deadline: %w", workCtx.Err()), result.err)
		}
	}
	independent, hasLifetime := p.tx.(interface{ OutlivesConnection() bool })
	if !commit && hasLifetime && independent.OutlivesConnection() {
		go func() {
			detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), storage.RemoteRequestTimeout)
			defer cancel()
			run(detached)
		}()
		select {
		case <-result.done:
		case <-ctx.Done():
			return fmt.Errorf("remote sub-transaction did not end before its deadline: %w; its rollback continues", ctx.Err())
		}
	} else {
		run(ctx)
	}
	if result.panicked != nil {
		panic(result.panicked)
	}
	return result.err
}

// commitOutcome observes the original terminal result, independently of cleanup.
func (p *remoteParticipant) commitOutcome() string {
	p.mu.Lock()
	result, commit := p.primary, p.committed
	p.mu.Unlock()
	if result == nil || !commit {
		return "rolledback"
	}
	<-result.done
	if result.err == nil {
		return "committed"
	}
	return StateUnknown
}

func (p *remoteParticipant) endWithin(ctx context.Context, commit bool) error {
	ctx, cancel := context.WithTimeout(ctx, storage.RemoteRequestTimeout)
	defer cancel()
	return p.end(ctx, commit)
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
// transaction ends; the returned error retains every rollback failure.
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
	return nornicerrors.CombineTerminalOutcomes(nil, errs...)
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

// Close ends cached remote participants and engines, retaining every cleanup error.
func (r *RemoteFragmentExecutor) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var cleanupErrors []error
	for key, handle := range r.txHandles {
		if err := handle.end(context.Background(), false); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to rollback remote tx handle '%s': %w", observability.RedactText(key), err))
		}
		delete(r.txHandles, key)
	}
	for key, engine := range r.engineCache {
		if err := engine.Close(); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to close remote engine '%s': %w", observability.RedactText(key.uri+"|"+key.database), err))
		}
		delete(r.engineCache, key)
	}
	return nornicerrors.CombineTerminalOutcomes(nil, cleanupErrors...)
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
	tx, ok := FabricTransactionFromContext(ctx)
	if !ok {
		return nil, nil, fmt.Errorf("fabric transaction context is missing for remote sub-transaction '%s'", sub.ShardName)
	}
	uncertain := unresolvedRemoteMode(ctx)
	if mode == storage.RemoteAccessWrite && !uncertain {
		if _, err := tx.GetOrOpen(sub.ShardName, true); err != nil {
			return nil, nil, err
		}
	}
	sessionMode := mode
	if uncertain || tx.WriteShard() == sub.ShardName {
		sessionMode = storage.RemoteAccessWrite
	}
	handle, err := r.getOrCreateTxHandle(ctx, sub, engine, sessionMode)
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
	if write && mode == storage.RemoteAccessRead && !uncertain {
		return nil, nil, fmt.Errorf("remote read participant '%s' reported a write", sub.ShardName)
	}
	if uncertain && (write || !known) {
		if _, err := tx.GetOrOpen(sub.ShardName, true); err != nil {
			return nil, nil, err
		}
	}

	return columns, rows, nil
}

func (r *RemoteFragmentExecutor) getOrCreateTxHandle(ctx context.Context, sub *SubTransaction, engine remoteCypherEngine, mode storage.RemoteAccessMode) (result *remoteParticipant, resultErr error) {
	r.mu.RLock()
	if handle, ok := r.txHandles[sub.ShardName]; ok && (handle.write || mode == storage.RemoteAccessRead) {
		r.mu.RUnlock()
		return handle, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if handle, ok := r.txHandles[sub.ShardName]; ok {
		if handle.write || mode == storage.RemoteAccessRead {
			return handle, nil
		}
		// The accepted READ-to-WRITE transition ends the empty read session.
		if err := handle.endWithin(context.WithoutCancel(ctx), false); err != nil {
			return nil, err
		}
		delete(r.txHandles, sub.ShardName)
	}
	tx, err := engine.BeginCypherTx(storage.WithCoordinatorOwnedTransaction(ctx), mode)
	if err != nil {
		return nil, err
	}
	handle := &remoteParticipant{tx: tx, write: mode == storage.RemoteAccessWrite}
	registered := false
	defer func() {
		if !registered {
			resultErr = nornicerrors.CombineTerminalOutcomes(resultErr, handle.endWithin(context.WithoutCancel(ctx), false))
		}
	}()

	fabricTx, ok := FabricTransactionFromContext(ctx)
	if !ok || fabricTx == nil {
		return nil, fmt.Errorf("fabric transaction context is missing for remote sub-transaction '%s'", sub.ShardName)
	}
	// The sub-transaction ends with its transaction, after the statement or
	// APPLY batch that enlisted it has returned and cancelled ctx. The
	// transaction owns commit; rollback cleanup remains independent of its
	// cancellation. The existing remote terminal owner bounds both.
	terminalCtx := fabricTx.BindContext(ctx)
	rollbackCtx := context.WithoutCancel(ctx)
	commitFn := func(participant *SubTransaction) error { return handle.endWithin(terminalCtx, participant.IsWrite) }
	rollbackFn := func(_ *SubTransaction) error { return handle.endWithin(rollbackCtx, false) }
	if err := fabricTx.BindParticipantCallbacks(sub.ShardName, ParticipantCallbacks{Commit: commitFn, Rollback: rollbackFn, Outcome: handle.commitOutcome}); err != nil {
		return nil, err
	}
	r.txHandles[sub.ShardName] = handle
	registered = true
	return handle, nil
}

// RunInStatement enlists issued storage in its owning statement before transport.
func (r *RemoteFragmentExecutor) RunInStatement(begin context.Context, tx *FabricTransaction, shard string, engine *storage.RemoteEngine, ctx context.Context, query string, params map[string]interface{}, mode storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
	sub, err := tx.GetOrOpen(shard, false)
	if err != nil {
		return nil, nil, err
	}
	ctx = WithFabricTransaction(WithSubTransaction(ctx, sub), tx)
	ctx = WithTransactionLifetime(ctx, tx.BindContext(begin))
	return r.executeRemoteQuery(ctx, engine, query, params, mode)
}
