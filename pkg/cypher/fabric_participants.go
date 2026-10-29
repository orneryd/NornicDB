package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Composite statements issue owned native, router and remote storage views.
// Native participant creation and terminal callbacks remain with the shard
// executor; storage write admission selects the sole write database.

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
	source  *StorageExecutor
	storage *transactionStorageWrapper
}

// compositeStatementOf is the composite statement e runs in, and ctx inside
// it: the owner discovered from e's storage, with ctx used only when the
// storage is unowned (membership). An executor on storage the statement issued runs in
// that statement whatever context it is given (a procedure that calls
// Execute with a context of its own): the storage is the statement's, not
// the context. Every entrance that runs a statement, or decides how one
// runs (Execute, a nested statement, USE, a transactional CALL), asks it.
func (e *StorageExecutor) compositeStatementOf(ctx context.Context) (*compositeStatement, context.Context) {
	member := e.membership(ctx)
	if member.statement != nil {
		return member.statement, &compositeContext{Context: ctx, statement: member.statement, storage: member.storage}
	}
	return nil, ctx
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
		bound = raw.WithStatementRunner(s, func(ctx context.Context, statement string, params map[string]interface{}, mode storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
			return c.remote.RunInStatement(s.begin, s.tx, name, raw, ctx, statement, params, mode)
		})
	} else {
		err := localizedError(localization.CypherCommandRoutingFabricStorageTypeInvalid(name), nil)
		bound = storage.NewStatementRemoteEngine(s, func(context.Context, string, map[string]interface{}, storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
			return nil, nil, err
		})
	}
	if s.remotes == nil {
		s.remotes = make(map[string]*storage.RemoteEngine)
	}
	s.remotes[name] = bound
	return bound
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

// run is the executor and context a statement runs with on the handle's
// participant storage.
func (h *participantHandle) run(ctx context.Context, statement *compositeStatement) (*StorageExecutor, context.Context) {
	exec := h.source.cloneWithStorage(h.storage)
	// Its transaction is the participant's, which the context carries.
	exec.txContext = nil
	return exec, &compositeContext{Context: ctx, statement: statement, storage: h.storage}
}

// statementFor is the composite statement c runs on tx.
func (c *cypherFabricExecutor) statementFor(ctx context.Context, tx *fabric.FabricTransaction) *compositeStatement {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.transaction == nil {
		c.transaction = tx
	}
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
	sub, err := s.tx.GetOrOpen(name, false)
	if err != nil {
		return nil, err
	}
	source, err := s.executor.ensureLocalShardTxExecutor(s.begin, sub, strings.TrimPrefix(name, "local:"), engine)
	if err != nil {
		return nil, err
	}
	s.executor.mu.Lock()
	defer s.executor.mu.Unlock()
	view := source.transactionStorageFor(source.txContext.tx.(*storage.BadgerTransaction))
	view.statement.Store(s)
	return &participantHandle{source: source, storage: view}, nil
}

// locate is the participant name of database, and whether it is a remote
// constituent.
func (s *compositeStatement) locate(database string) (string, bool) {
	if catalog := s.executor.catalog; catalog != nil {
		if location, err := catalog.Resolve(database); err == nil {
			_, remote := location.(*fabric.LocationRemote)
			return fabric.ParticipantKeyFromLocation(location), remote
		}
	}
	return fabric.ParticipantKeyFromLocation(&fabric.LocationLocal{DBName: database}), false
}

// on is the executor and context a statement runs with on database, whose
// executor outside the composite statement is exec: a local database's
// participant storage, the composite router, or a remote constituent's
// engine inside the statement (remote).
func (s *compositeStatement) on(ctx context.Context, database string, exec *StorageExecutor, engine storage.Engine) (*StorageExecutor, context.Context, error) {
	if member := membershipOfStorage(engine); member.statement == s && member.storage == nil {
		view := exec.cloneWithStorage(engine, selectedDatabaseStorage)
		view.txContext = nil
		return view, &compositeContext{Context: ctx, statement: s}, nil
	}
	// Transparent wrappers may surround the router as well as a participant.
	if root := compositeEngineOf(engine); root != nil {
		routed := exec.cloneWithStorage(s.router(root), selectedDatabaseStorage)
		routed.txContext = nil
		return routed, &compositeContext{Context: ctx, statement: s}, nil
	}
	name, remote := s.locate(database)
	if remote {
		remoteExec := exec.cloneWithStorage(s.remote(name, engine), selectedDatabaseStorage)
		remoteExec.txContext = nil
		return remoteExec, &compositeContext{Context: ctx, statement: s}, nil
	}
	h, err := s.participant(database, engine)
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
	return root.WithConstituentAccess(s, func(alias string, engine storage.Engine, _ bool) (storage.Engine, error) {
		graph := composite + "." + alias
		name, remote := s.locate(graph)
		if remote {
			return s.remote(name, engine), nil
		}
		h, err := s.participant(graph, engine)
		if err != nil {
			return nil, err
		}
		return h.storage, nil
	})
}
