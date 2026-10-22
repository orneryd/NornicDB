package cypher

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func init() {
	fabric.SetAccessMode(fragmentWrites)
}

// fragmentWrites is the access mode of a statement run inside a composite
// statement, from its text, as Neo4j decides a query's access mode: it
// writes when it has a write clause or a schema command, or calls a
// procedure that writes. It decides which session the statement opens when
// it runs (fabricScope.executorFor, the remote participant); the write is
// claimed then, not before.
//
// On a local database the procedures are this server's own: one registered
// as WRITE or SCHEMA writes, and a name it doesn't know counts as a write
// (QueryPermissionRequirements). On a remote constituent a procedure is the
// remote's own definition, so only a built-in's mode says what it does: a
// user or plugin procedure counts as a write, and so does a READ built-in
// that runs a statement it is given (ProcedureSpec.ExecutesStatements),
// since a NornicDB remote lets that statement write.
func fragmentWrites(query string, remote bool) bool {
	return fragmentAccessModes.decide(query, remote)
}

// accessModeMemo keeps the access mode of recent statement texts, for the
// procedure registry generation it was decided in: a fragment is
// dispatched once per input row.
type accessModeMemo struct {
	mu         sync.RWMutex
	generation uint64
	modes      map[accessModeKey]bool
}

type accessModeKey struct {
	query  string
	remote bool
}

// accessModeMemoLimit bounds the memo; it starts over when full.
const accessModeMemoLimit = 4096

var fragmentAccessModes = &accessModeMemo{}

func (m *accessModeMemo) decide(query string, remote bool) bool {
	key := accessModeKey{query: query, remote: remote}
	generation := globalProcedureRegistry.generation.Load()
	m.mu.RLock()
	writes, known := m.modes[key]
	known = known && m.generation == generation
	m.mu.RUnlock()
	if known {
		return writes
	}
	writes = statementWrites(query, remote)
	m.mu.Lock()
	if m.generation != generation || len(m.modes) >= accessModeMemoLimit {
		m.generation, m.modes = generation, make(map[accessModeKey]bool)
	}
	m.modes[key] = writes
	m.mu.Unlock()
	return writes
}

// statementWrites is fragmentWrites, undecided.
func statementWrites(query string, remote bool) bool {
	query = classificationText(query)
	requirements := permissionRequirements(query)
	if requirements.Write || requirements.Schema {
		return true
	}
	for _, name := range calledProcedureNames(query) {
		procedure, known := globalProcedureRegistry.Get(name)
		if !known || remote && (procedure.User || procedure.Spec.ExecutesStatements) {
			return true
		}
	}
	return false
}

// fabricScope is the composite statement a statement runs in, on one of its
// local databases: a fragment, a statement a procedure runs there, or one
// that selects another database with USE. Each write such a statement
// opens claims its database as the composite statement's write participant
// first (executorFor, openWrite), so a write to a second database is
// refused before it runs, and runs in that database's sub-transaction, so it
// commits or rolls back with the whole statement.
type fabricScope struct {
	executor *cypherFabricExecutor // the statement's local participants
	tx       *fabric.FabricTransaction
	catalog  *fabric.Catalog // names the participant a USE selects
	dbName   string          // the database the statement runs on
	engine   storage.Engine  // its storage
	// joins: a write runs in the participant's sub-transaction; false for
	// CALL { ... } IN TRANSACTIONS in an autocommit statement, whose batches
	// commit on their own, as on a single database.
	joins bool
}

type fabricScopeKey struct{}

// participantHeldKey marks a statement that runs while its caller holds the
// participants' lock (cypherFabricExecutor.participantMu): a statement a
// procedure of a joined fragment runs, nested in it.
type participantHeldKey struct{}

func withFabricScope(ctx context.Context, scope *fabricScope) context.Context {
	return context.WithValue(ctx, fabricScopeKey{}, scope)
}

func fabricScopeFrom(ctx context.Context) *fabricScope {
	scope, _ := ctx.Value(fabricScopeKey{}).(*fabricScope)
	return scope
}

// withoutFabricScope is ctx outside any composite statement.
func withoutFabricScope(ctx context.Context) context.Context {
	if fabricScopeFrom(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, fabricScopeKey{}, (*fabricScope)(nil))
}

// participant is the participant name of the scope's database, and whether
// it is a local one, which has a sub-transaction here.
func (s *fabricScope) participant() (string, bool) {
	if s.catalog != nil {
		if location, err := s.catalog.Resolve(s.dbName); err == nil {
			_, remote := location.(*fabric.LocationRemote)
			return fabric.ParticipantOf(location), !remote
		}
	}
	return fabric.LocalParticipant(s.dbName), true
}

// on is the scope of a statement that selects database dbName (USE) and
// runs on engine.
func (s *fabricScope) on(dbName string, engine storage.Engine) *fabricScope {
	selected := *s
	selected.dbName, selected.engine = dbName, engine
	return &selected
}

// open opens statement on the scope's database as it runs: a statement
// that writes claims the database as the write participant
// (FabricTransaction.ClaimWrite), then runs in its sub-transaction; so does
// every statement there of an explicit transaction, and every later one on
// the write participant, which reads what the statement wrote. join reports
// whether it runs in the sub-transaction; a statement already in a
// transaction stays in it.
func (s *fabricScope) open(statement string, inTransaction bool) (participant string, join bool, err error) {
	participant, local := s.participant()
	if fragmentWrites(statement, !local) {
		if _, err := s.tx.ClaimWrite(participant); err != nil {
			return "", false, err
		}
	}
	join = local && !inTransaction && s.joins && (!s.executor.autoCommit || s.tx.WriteShard() == participant)
	return participant, join, nil
}

// executorFor is the executor a statement of the scope runs on, which e
// would run (open), and the context it runs with; release ends its use of
// the sub-transaction.
func (s *fabricScope) executorFor(ctx context.Context, e *StorageExecutor, statement string) (*StorageExecutor, context.Context, func(), error) {
	participant, join, err := s.open(statement, e.inTransaction(ctx))
	if err != nil {
		return nil, ctx, nil, err
	}
	if !join {
		return e, ctx, func() {}, nil
	}
	return s.joined(ctx, participant)
}

// fragmentExecutor is the executor of the sub-transaction a fragment of the
// scope runs in (open), or nil when it runs outside one, on a view of its
// database; release ends its use of the sub-transaction.
func (s *fabricScope) fragmentExecutor(ctx context.Context, query string) (*StorageExecutor, context.Context, func(), error) {
	participant, join, err := s.open(query, false)
	if err != nil {
		return nil, ctx, nil, err
	}
	if !join {
		return nil, ctx, func() {}, nil
	}
	return s.joined(ctx, participant)
}

// openWrite opens a write that a statement of the scope starts outside a
// transaction (an implicit transaction): it claims the scope's database
// (FabricTransaction.ClaimWrite) and returns the executor of its
// sub-transaction, or nil when the write commits on its own (CALL { ... }
// IN TRANSACTIONS batches).
func (s *fabricScope) openWrite(ctx context.Context) (*StorageExecutor, context.Context, func(), error) {
	participant, local := s.participant()
	if _, err := s.tx.ClaimWrite(participant); err != nil {
		return nil, ctx, nil, err
	}
	if !local || !s.joins {
		return nil, ctx, func() {}, nil
	}
	return s.joined(ctx, participant)
}

// joined is the executor of the scope database's sub-transaction, which the
// caller uses until release.
func (s *fabricScope) joined(ctx context.Context, participant string) (*StorageExecutor, context.Context, func(), error) {
	sub, err := s.tx.GetOrOpen(participant, false)
	if err != nil {
		return nil, ctx, nil, err
	}
	c := s.executor
	release := func() {}
	if _, held := ctx.Value(participantHeldKey{}).(bool); !held {
		c.participantMu.Lock()
		release = c.participantMu.Unlock
		ctx = context.WithValue(ctx, participantHeldKey{}, true)
	}
	joined, err := c.ensureLocalShardTxExecutor(ctx, s.tx, sub, s.dbName, s.engine)
	if err != nil {
		release()
		return nil, ctx, nil, err
	}
	return joined, ctx, release, nil
}

// runScopedStatement runs statement, which a procedure was given or a
// plugin passed to Execute, with run. Inside a composite statement one that
// names a composite graph runs as part of it (executeViaFabric) and one that
// selects a database with USE is routed where it does (useDatabase); any
// other runs on the scope's database (fabricScope.executorFor).
func (e *StorageExecutor) runScopedStatement(ctx context.Context, statement string, run func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) {
	scope := fabricScopeFrom(ctx)
	if scope == nil || e.shouldUseFabricPlanner(statement) {
		return run(ctx, e)
	}
	if _, _, hasUse, _ := parseUseClause(statement, false); hasUse {
		return run(ctx, e)
	}
	target, ctx, release, err := scope.executorFor(ctx, e, statement)
	if err != nil {
		return nil, err
	}
	defer release()
	return run(ctx, target)
}
