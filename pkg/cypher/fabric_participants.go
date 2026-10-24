package cypher

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// A composite statement writes at most one database, and everything it
// writes commits or rolls back with the whole statement. Every statement of
// the composite statement on a local database runs on that database's
// participant storage (newParticipantStorage): an ordinary transaction
// storage wrapper, transparent to every capability lookup as on one database,
// whose transaction begins on the first operation that reaches it, a read
// included, so the statement reads that database in one snapshot. The
// transaction decides which database is written: before its first write it
// asks its write admission (storage.BadgerTransaction.SetWriteAdmission),
// which claims the database as the statement's write participant
// (FabricTransaction.ClaimWrite), so a write to a second database is refused
// before it is made. The statement's coordinator commits or rolls back every
// participant's transaction. A statement that writes nothing claims nothing:
// unlike Neo4j, which takes a statement's write database from its access mode
// before it runs, a SET that matches no node or a MERGE that matches leaves
// the statement free to write another database. The composite database
// itself is a router (compositeStatement.router): it is never a participant,
// and the constituents it routes to are reached through their participant
// storage. A remote constituent's engine inside the statement runs every
// operation in the statement's remote session (compositeStatement.remote), as
// its query fragments do: nothing on a remote commits on its own. Statement
// text classifies nothing locally (fragmentWrites is a hint for remote
// sessions and dispatch).

// compositeStatement is the composite statement a statement runs in.
type compositeStatement struct {
	executor *cypherFabricExecutor
	tx       *fabric.FabricTransaction
	// begin is the context participants' transactions begin with, outside
	// the composite statement: its client, credentials and metadata. An
	// explicit transaction's participant may begin in a later statement, so
	// it is not cancelled with this one.
	begin context.Context
	// remotes are the remote constituents' engines inside the statement by
	// participant name (remote), guarded by the executor's mu.
	remotes map[string]*storage.RemoteEngine
}

type compositeStatementKey struct{}

// compositeContext is the context of a statement run inside a composite
// statement on one database: it carries the statement, and the database's
// participant storage, or none (the composite router, a remote
// constituent), in place of any storage of the statement it is nested in.
type compositeContext struct {
	context.Context
	statement *compositeStatement
	storage   *transactionStorageWrapper
}

func (c *compositeContext) Value(key any) any {
	switch key {
	case compositeStatementKey{}:
		return c.statement
	case ctxKeyTxStorage:
		if c.storage == nil {
			return nil
		}
		return c.storage
	}
	return c.Context.Value(key)
}

func compositeStatementFrom(ctx context.Context) *compositeStatement {
	statement, _ := ctx.Value(compositeStatementKey{}).(*compositeStatement)
	return statement
}

// withoutCompositeStatement is ctx outside any composite statement.
func withoutCompositeStatement(ctx context.Context) context.Context {
	if compositeStatementFrom(ctx) == nil {
		return ctx
	}
	return context.WithValue(context.WithValue(ctx, compositeStatementKey{}, (*compositeStatement)(nil)), ctxKeyTxStorage, nil)
}

// participantHandle is one local database's part in a composite statement,
// shared by every statement of it on that database (and, in an explicit
// transaction, by every statement of the transaction).
type participantHandle struct {
	name       string // the participant name (fabric.LocalParticipant)
	database   string
	namespace  string
	engine     storage.Engine
	tx         *fabric.FabricTransaction
	autoCommit bool
	begin      context.Context
	// source is the executor the handle's statements run on clones of; it
	// is never changed.
	source  *StorageExecutor
	storage *transactionStorageWrapper
	// owner runs an explicit transaction's participant transaction once it
	// began: its explicit transaction is the participant's. An autocommit
	// statement's participant transaction is an implicit one, which needs
	// none.
	owner atomic.Pointer[StorageExecutor]
}

// participantTransaction is the transaction of a participant's storage: none
// until the first operation that reaches the storage begins it (begun).
// Beginning claims nothing; the transaction's write admission claims at its
// first write.
type participantTransaction struct {
	mu    sync.Mutex
	tx    atomic.Pointer[storage.BadgerTransaction]
	begin func() (*storage.BadgerTransaction, error)
}

// begun is the transaction, begun now if it hasn't been.
func (p *participantTransaction) begun() (*storage.BadgerTransaction, error) {
	if tx := p.tx.Load(); tx != nil {
		return tx, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if tx := p.tx.Load(); tx != nil {
		return tx, nil
	}
	tx, err := p.begin()
	if err != nil {
		return nil, err
	}
	p.tx.Store(tx)
	return tx, nil
}

// newParticipantStorage is the one construction of lazily begun transaction
// storage: a composite statement's local participant's (h), whose
// transaction (participantHandle.open) claims the participant at its first
// write (participantHandle.claim).
func newParticipantStorage(h *participantHandle, underlying storage.Engine, namespace string) *transactionStorageWrapper {
	separator := ":"
	if namespace == "" {
		separator = ""
	}
	lazy := &participantTransaction{begin: h.open}
	wrapper := &transactionStorageWrapper{
		lazy:           lazy,
		underlying:     underlying,
		namespace:      namespace,
		separator:      separator,
		mutatedNodeIDs: make(map[string]struct{}),
	}
	wrapper.initNamespacePrefix()
	return wrapper
}

// claim claims the handle's database as the statement's write participant:
// the write admission of the participant's transaction.
func (h *participantHandle) claim() error {
	if _, err := h.tx.ClaimWrite(h.name); err != nil {
		return err
	}
	return nil
}

// remote is remote constituent name's engine inside the statement, whose
// engine outside it is engine: every operation runs its statements in the
// statement's remote session for name, the one the statement's query
// fragments there run in (fabric.RemoteFragmentExecutor.RunInStatement). A
// statement that writes claims name first and runs in the write session, so
// nothing an operation writes is kept unless the statement commits. A
// constituent whose engine is not a remote engine cannot join the statement:
// every operation on it fails.
func (s *compositeStatement) remote(name string, engine storage.Engine) *storage.RemoteEngine {
	c := s.executor
	c.mu.Lock()
	defer c.mu.Unlock()
	if bound := s.remotes[name]; bound != nil {
		return bound
	}
	var bound *storage.RemoteEngine
	if raw := remoteEngineOf(engine); raw != nil && c.remote != nil {
		bound = raw.WithStatementRunner(func(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
			return c.remote.RunInStatement(s.begin, s.tx, name, raw, ctx, statement, params)
		})
	} else {
		err := localizedError(localization.CypherCommandRoutingFabricStorageTypeInvalid(name), nil)
		bound = storage.NewStatementRemoteEngine(func(context.Context, string, map[string]interface{}) ([]string, [][]interface{}, error) {
			return nil, nil, err
		})
	}
	if s.remotes == nil {
		s.remotes = make(map[string]*storage.RemoteEngine)
	}
	s.remotes[name] = bound
	return bound
}

// joined reports whether engine is a remote constituent's engine inside the
// statement (remote).
func (s *compositeStatement) joined(engine *storage.RemoteEngine) bool {
	s.executor.mu.Lock()
	defer s.executor.mu.Unlock()
	for _, bound := range s.remotes {
		if bound == engine {
			return true
		}
	}
	return false
}

// remoteEngineOf is the remote engine engine is, or wraps, or nil.
func remoteEngineOf(engine storage.Engine) *storage.RemoteEngine {
	for engine != nil {
		if remote, ok := engine.(*storage.RemoteEngine); ok {
			return remote
		}
		inner, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			return nil
		}
		engine = inner.GetInnerEngine()
	}
	return nil
}

// open begins the handle's transaction, which the coordinator commits or
// rolls back with the statement: the participant joins the statement as a
// reader, and the transaction's write admission claims it at the first write
// (participantHandle.claim). An autocommit statement's participant
// transaction is an implicit one, begun and committed as a single-database
// statement's is; an explicit transaction's is an explicit transaction of
// its own, outside the composite statement.
func (h *participantHandle) open() (*storage.BadgerTransaction, error) {
	if _, err := h.tx.GetOrOpen(h.name, false); err != nil {
		return nil, err
	}
	var (
		tx       *storage.BadgerTransaction
		commit   fabric.CommitCallback
		rollback fabric.RollbackCallback
	)
	terminal := context.WithoutCancel(h.begin)
	if h.autoCommit {
		engines := h.source.resolveImplicitTxEngines()
		if engines.txEngine == nil {
			return nil, localizedError(localization.CypherTransactionsEngineUnsupported(), nil)
		}
		started, err := h.source.beginImplicitTransaction(h.begin, engines.txEngine, h.namespace)
		if err != nil {
			return nil, localizedError(localization.CypherCommandRoutingFabricShardTransactionFailed(h.database, err), err)
		}
		tx = started.tx
		commit = func(*fabric.SubTransaction) error {
			_, err := h.source.commitImplicitTransaction(terminal, &started, h.storage, h.source.cloneWithStorage(h.storage), "")
			return err
		}
		rollback = func(*fabric.SubTransaction) error {
			started.abort("rollback")
			return nil
		}
	} else {
		owner := h.source.cloneWithStorage(h.engine)
		owner.sharedExecutor = false
		owner.txContext = nil
		owner.hotPathTraceState = &hotPathTraceState{}
		if _, err := owner.beginTransaction(h.begin); err != nil {
			return nil, localizedError(localization.CypherCommandRoutingFabricShardTransactionFailed(h.database, err), err)
		}
		var ok bool
		if tx, ok = owner.txContext.tx.(*storage.BadgerTransaction); !ok {
			_, _ = owner.handleRollback()
			return nil, localizedError(localization.CypherTransactionsUnknownType(), nil)
		}
		// Its commit runs the storage's commit actions and search refreshes.
		owner.txContext.storageWrapper = h.storage
		commit = func(*fabric.SubTransaction) error {
			_, err := owner.handleCommit(terminal)
			return err
		}
		rollback = func(*fabric.SubTransaction) error {
			_, err := owner.handleRollback()
			return err
		}
		h.owner.Store(owner)
	}
	tx.SetWriteAdmission(h.claim)
	if err := h.tx.BindParticipantCallbacks(h.name, commit, rollback); err != nil {
		_ = rollback(nil)
		return nil, err
	}
	return tx, nil
}

// run is the executor and context a statement runs with on the handle's
// participant storage.
func (h *participantHandle) run(ctx context.Context, statement *compositeStatement) (*StorageExecutor, context.Context) {
	exec := h.source.cloneWithStorage(h.storage)
	// Its transaction is the participant's, which the context carries.
	exec.txContext = nil
	return exec, &compositeContext{Context: ctx, statement: statement, storage: h.storage}
}

// participants is the statement's local participants by database: the
// explicit transaction's, so its statements share them, or else the
// statement's own.
func (c *cypherFabricExecutor) participants() map[string]*participantHandle {
	if !c.autoCommit && c.base != nil && c.base.txContext != nil && c.base.txContext.active {
		if c.base.txContext.fabricParticipants == nil {
			c.base.txContext.fabricParticipants = make(map[string]*participantHandle)
		}
		return c.base.txContext.fabricParticipants
	}
	if c.ownParticipants == nil {
		c.ownParticipants = make(map[string]*participantHandle)
	}
	return c.ownParticipants
}

// statementFor is the composite statement c runs on tx.
func (c *cypherFabricExecutor) statementFor(ctx context.Context, tx *fabric.FabricTransaction) *compositeStatement {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.statement == nil || c.statement.tx != tx {
		c.statement = &compositeStatement{executor: c, tx: tx,
			begin: WithAuthToken(context.WithoutCancel(withoutCompositeStatement(ctx)), c.authToken)}
	}
	return c.statement
}

// participant is the handle of local database database (by its engine),
// created on first use.
func (s *compositeStatement) participant(database string, engine storage.Engine) (*participantHandle, error) {
	name, _ := s.locate(database)
	c := s.executor
	c.mu.Lock()
	defer c.mu.Unlock()
	handles := c.participants()
	if h := handles[name]; h != nil {
		return h, nil
	}
	physical := strings.TrimPrefix(name, "local:")
	source := c.base.cloneForStorage(engine)
	source.sharedExecutor = false
	source.txContext = nil
	if err := source.bindStatementCacheScope(physical); err != nil {
		return nil, err
	}
	h := &participantHandle{name: name, database: physical, engine: engine, tx: s.tx,
		autoCommit: c.autoCommit, begin: s.begin, source: source}
	h.namespace = source.resolveImplicitTxEngines().namespace
	h.storage = newParticipantStorage(h, engine, h.namespace)
	source.ensureNodeLookupCache()
	h.storage.ensureNodeLookupCacheLocked(source)
	handles[name] = h
	return h, nil
}

// locate is the participant name of database, and whether it is a remote
// constituent.
func (s *compositeStatement) locate(database string) (string, bool) {
	if catalog := s.executor.catalog; catalog != nil {
		if location, err := catalog.Resolve(database); err == nil {
			_, remote := location.(*fabric.LocationRemote)
			return fabric.ParticipantOf(location), remote
		}
	}
	return fabric.LocalParticipant(database), false
}

// on is the executor and context a statement runs with on database, whose
// executor outside the composite statement is exec: a local database's
// participant storage, the composite router, or a remote constituent's
// engine inside the statement (remote).
func (s *compositeStatement) on(ctx context.Context, database string, exec *StorageExecutor) (*StorageExecutor, context.Context, error) {
	switch store := exec.storage.(type) {
	case *transactionStorageWrapper:
		if store.lazy != nil {
			// Already a participant's storage.
			return exec, &compositeContext{Context: ctx, statement: s, storage: store}, nil
		}
	case *storage.CompositeEngine:
		routed := exec.cloneWithStorage(s.router(store))
		routed.txContext = nil
		return routed, &compositeContext{Context: ctx, statement: s}, nil
	case *storage.RemoteEngine:
		if s.joined(store) {
			return exec, &compositeContext{Context: ctx, statement: s}, nil
		}
	}
	name, remote := s.locate(database)
	if remote {
		remoteExec := exec.cloneWithStorage(s.remote(name, exec.storage))
		remoteExec.txContext = nil
		return remoteExec, &compositeContext{Context: ctx, statement: s}, nil
	}
	h, err := s.participant(database, exec.storage)
	if err != nil {
		return nil, ctx, err
	}
	runExec, runCtx := h.run(ctx, s)
	return runExec, runCtx, nil
}

// router is the composite database root inside the statement: it routes to
// its constituents' participant storage.
func (s *compositeStatement) router(root *storage.CompositeEngine) storage.Engine {
	composite := root.CompositeName()
	return root.WithConstituents(func(alias string, engine storage.Engine) storage.Engine {
		graph := composite + "." + alias
		name, remote := s.locate(graph)
		if remote {
			return s.remote(name, engine)
		}
		h, err := s.participant(graph, engine)
		if err != nil {
			// A constituent the statement cannot join: every operation on it
			// fails.
			return storage.NewStatementRemoteEngine(func(context.Context, string, map[string]interface{}) ([]string, [][]interface{}, error) {
				return nil, nil, err
			})
		}
		return h.storage
	})
}

// executorFor is the executor and context a statement e runs inside the
// composite statement runs with (on): e's database's participant storage.
func (s *compositeStatement) executorFor(ctx context.Context, e *StorageExecutor) (*StorageExecutor, context.Context, error) {
	return s.on(ctx, e.currentDatabaseName(), e)
}

// inCompositeStatement runs statement with run: inside a composite
// statement on the participant storage of e's database (on), one that
// names a composite graph as part of it (executeViaFabric), and one that
// selects a database with USE where it does (useDatabase).
func (e *StorageExecutor) inCompositeStatement(ctx context.Context, statement string, run func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) {
	composite := compositeStatementFrom(ctx)
	if composite == nil || e.shouldUseFabricPlanner(statement) {
		return run(ctx, e)
	}
	if _, _, hasUse, _ := parseUseClause(statement, false); hasUse {
		return run(ctx, e)
	}
	target, ctx, err := composite.executorFor(ctx, e)
	if err != nil {
		return nil, err
	}
	return run(ctx, target)
}
