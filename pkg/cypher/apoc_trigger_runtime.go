package cypher

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// APOC triggers (Neo4j 5 APOC apoc.trigger.*): a statement a database runs
// at the commit of each of its transactions.
//
// The runtime registers one storage commit hook per database that has an
// active trigger (storage.CommitHook, pkg/storage/commit_hooks.go); a
// database without one has no hook and its commits pay nothing. The phases
// follow APOC 5's TriggerHandler:
//
//   - before (the default): run in the committing transaction, first thing
//     in its commit, over that transaction's state, so the statement reads
//     the transaction's final values and what it writes is part of the same
//     commit (and of its change events). Every trigger of the pass runs; if
//     any failed, the commit fails with Neo.ClientError.Transaction.
//     TransactionHookFailed and nothing is stored. The hook runs once per
//     commit, so a trigger's own writes never fire it again.
//   - after: after a successful commit, in one new transaction tagged with
//     the metadata {apoc.trigger: true}; a tagged transaction's commit runs
//     no after or afterAsync triggers, so they don't cascade (it does run
//     before triggers, once).
//   - afterAsync: the same, on a worker.
//   - rollback: after a transaction that changed something rolled back, in
//     a new untagged transaction.
//
// Every trigger of a pass gets the same transaction-state parameters
// ($createdNodes … $commitTime, apocTriggerParams), then its config params
// over them, then $trigger, its name.

// TriggerRuntimeConfig configures the trigger runtime of a storage engine.
type TriggerRuntimeConfig struct {
	// Engine is the storage engine whose databases' commits run triggers
	// (any wrapper of it).
	Engine storage.Engine
	// System is the system database's storage, where triggers are kept.
	System storage.Engine
	// ExecutorFor returns a database's executor: its triggers run there.
	ExecutorFor func(database string) (*StorageExecutor, error)
	// DatabaseExists reports whether a user database exists (install's
	// check); nil accepts any name ExecutorFor accepts.
	DatabaseExists func(database string) bool
	// SystemDatabase is the system database's name ("system" when empty).
	SystemDatabase string
	// Enabled is apoc.trigger.enabled: when false the trigger procedures
	// fail, and no trigger fires.
	Enabled bool
	// AsyncWorkers runs afterAsync triggers (4 when zero).
	AsyncWorkers int
	// Logger receives the errors of after, afterAsync and rollback
	// triggers, which have no caller to fail.
	Logger *slog.Logger
}

// TriggerRuntime runs the APOC triggers of a storage engine's databases.
type TriggerRuntime struct {
	config TriggerRuntimeConfig
	hooks  storage.CommitHookEngine
	store  apocTriggerStore

	// mu serializes changes; triggers is every database's triggers by name.
	mu       sync.Mutex
	triggers map[string]map[string]*apocTrigger

	async     chan func()
	asyncDone sync.WaitGroup
	closeOnce sync.Once
}

// triggerRuntimes finds the runtime of the engine an executor's storage is
// over: storage.CommitHookEngine -> *TriggerRuntime.
var triggerRuntimes sync.Map

// NewTriggerRuntime loads the installed triggers and registers the commit
// hooks of the databases that have active ones.
func NewTriggerRuntime(ctx context.Context, config TriggerRuntimeConfig) (*TriggerRuntime, error) {
	hooks, ok := storage.CommitHookEngineOf(config.Engine)
	if !ok {
		return nil, fmt.Errorf("apoc triggers: the storage engine runs no commit hooks")
	}
	if config.System == nil || config.ExecutorFor == nil {
		return nil, fmt.Errorf("apoc triggers: the system storage and an executor source are required")
	}
	if config.SystemDatabase == "" {
		config.SystemDatabase = "system"
	}
	if config.AsyncWorkers <= 0 {
		config.AsyncWorkers = 4
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	runtime := &TriggerRuntime{
		config: config,
		hooks:  hooks,
		store:  apocTriggerStore{system: config.System},
		async:  make(chan func(), 1024),
	}
	triggers, err := runtime.store.load(ctx)
	if err != nil {
		return nil, localizedError(localization.CypherProceduresTriggerStoreFailed(err), err)
	}
	runtime.triggers = triggers
	for i := 0; i < config.AsyncWorkers; i++ {
		runtime.asyncDone.Add(1)
		go func() {
			defer runtime.asyncDone.Done()
			for run := range runtime.async {
				run()
			}
		}()
	}
	if config.Enabled {
		for database := range triggers {
			runtime.publishLocked(database)
		}
	}
	triggerRuntimes.Store(hooks, runtime)
	return runtime, nil
}

// Close removes the runtime's hooks and waits for its afterAsync work.
func (r *TriggerRuntime) Close() {
	r.closeOnce.Do(func() {
		triggerRuntimes.CompareAndDelete(r.hooks, r)
		r.mu.Lock()
		for database := range r.triggers {
			r.hooks.SetCommitHook(database, nil)
		}
		r.mu.Unlock()
		close(r.async)
		r.asyncDone.Wait()
	})
}

// DropDatabase deletes a dropped database's triggers, so a database created
// later with the same name doesn't inherit them (APOC keeps them).
func (r *TriggerRuntime) DropDatabase(database string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name := range r.triggers[database] {
		if err := r.store.remove(database, name); err != nil {
			return localizedError(localization.CypherProceduresTriggerStoreFailed(err), err)
		}
	}
	delete(r.triggers, database)
	r.hooks.SetCommitHook(database, nil)
	return nil
}

// triggerRuntimeFor returns the runtime of the engine storage is over.
func triggerRuntimeFor(engine storage.Engine) *TriggerRuntime {
	if view, ok := engine.(systemGraphView); ok {
		engine = view.Engine
	}
	hooks, ok := storage.CommitHookEngineOf(engine)
	if !ok {
		return nil
	}
	runtime, _ := triggerRuntimes.Load(hooks)
	found, _ := runtime.(*TriggerRuntime)
	return found
}

// put installs or replaces a trigger.
func (r *TriggerRuntime) put(trigger *apocTrigger) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.store.save(trigger); err != nil {
		return localizedError(localization.CypherProceduresTriggerStoreFailed(err), err)
	}
	if r.triggers[trigger.Database] == nil {
		r.triggers[trigger.Database] = make(map[string]*apocTrigger)
	}
	r.triggers[trigger.Database][trigger.Name] = trigger
	r.publishLocked(trigger.Database)
	return nil
}

// remove deletes triggers (all of the database's when names is nil) and
// returns them, sorted by name.
func (r *TriggerRuntime) remove(database string, names []string) ([]*apocTrigger, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var removed []*apocTrigger
	if names == nil {
		removed = apocTriggersByName(r.triggers[database])
	} else {
		for _, name := range names {
			if trigger := r.triggers[database][name]; trigger != nil {
				removed = append(removed, trigger)
			}
		}
	}
	for _, trigger := range removed {
		if err := r.store.remove(database, trigger.Name); err != nil {
			return nil, localizedError(localization.CypherProceduresTriggerStoreFailed(err), err)
		}
		delete(r.triggers[database], trigger.Name)
	}
	r.publishLocked(database)
	return removed, nil
}

// setPaused stops or starts a trigger; nil when it isn't installed.
func (r *TriggerRuntime) setPaused(database, name string, paused bool) (*apocTrigger, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.triggers[database][name]
	if current == nil {
		return nil, nil
	}
	next := *current
	next.Paused = paused
	if err := r.store.save(&next); err != nil {
		return nil, localizedError(localization.CypherProceduresTriggerStoreFailed(err), err)
	}
	r.triggers[database][name] = &next
	r.publishLocked(database)
	return &next, nil
}

// list returns a database's triggers, sorted by name.
func (r *TriggerRuntime) list(database string) []*apocTrigger {
	r.mu.Lock()
	defer r.mu.Unlock()
	return apocTriggersByName(r.triggers[database])
}

// publishLocked sets a database's commit hook from its active triggers:
// none when it has no active one.
func (r *TriggerRuntime) publishLocked(database string) {
	if len(r.triggers[database]) == 0 {
		delete(r.triggers, database)
	}
	if !r.config.Enabled {
		return
	}
	hook := &apocTriggerHook{runtime: r, database: database}
	for _, trigger := range apocTriggersByName(r.triggers[database]) {
		if trigger.Paused {
			continue
		}
		hook.byPhase[trigger.phase] = append(hook.byPhase[trigger.phase], trigger)
		hook.phases |= trigger.phase.storagePhase()
	}
	if hook.phases == 0 {
		r.hooks.SetCommitHook(database, nil)
		return
	}
	r.hooks.SetCommitHook(database, hook)
}

// apocTriggerPhase is a trigger's selector phase.
type apocTriggerPhase uint8

const (
	apocTriggerBefore apocTriggerPhase = iota
	apocTriggerAfter
	apocTriggerRollback
	apocTriggerAfterAsync
	apocTriggerPhases
)

func (p apocTriggerPhase) String() string {
	return [...]string{"before", "after", "rollback", "afterAsync"}[p]
}

func (p apocTriggerPhase) storagePhase() storage.CommitHookPhases {
	switch p {
	case apocTriggerBefore:
		return storage.CommitHookBefore
	case apocTriggerRollback:
		return storage.CommitHookRollback
	default:
		return storage.CommitHookAfter
	}
}

// apocTriggerPhaseOf is a selector's phase: before when it has none; its
// name is case-sensitive, as APOC's Phase.valueOf.
func apocTriggerPhaseOf(selector map[string]any) (apocTriggerPhase, error) {
	value, ok := selector["phase"]
	if !ok || value == nil {
		return apocTriggerBefore, nil
	}
	name := fmt.Sprint(value)
	for phase := apocTriggerBefore; phase < apocTriggerPhases; phase++ {
		if phase.String() == name {
			return phase, nil
		}
	}
	return 0, localizedStatusError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed",
		localization.CypherProceduresTriggerPhaseInvalid(name))
}

// apocTriggerHook is a database's commit hook: its active triggers by
// phase, in name order. It is replaced, never changed, when they change.
type apocTriggerHook struct {
	runtime  *TriggerRuntime
	database string
	byPhase  [apocTriggerPhases][]*apocTrigger
	phases   storage.CommitHookPhases
}

func (h *apocTriggerHook) Phases() storage.CommitHookPhases { return h.phases }

// BeforeCommit runs the before triggers in the committing transaction, over
// a storage view of it: the statement's own, when ctx carries it.
func (h *apocTriggerHook) BeforeCommit(ctx context.Context, tx *storage.BadgerTransaction, changes *storage.ChangeSet) error {
	triggers := h.byPhase[apocTriggerBefore]
	if len(triggers) == 0 {
		return nil
	}
	exec, err := h.runtime.config.ExecutorFor(h.database)
	if err != nil {
		return apocTriggerFailedError(map[string]string{triggers[0].Name: err.Error()}, []string{triggers[0].Name})
	}
	if !storageIsLeader(exec.storage) {
		// A replica applies what its leader committed, the leader's trigger
		// writes included.
		return nil
	}
	wrapper, ok := ctx.Value(ctxKeyTxStorage).(*transactionStorageWrapper)
	if !ok || wrapper.tx != tx {
		separator := ":"
		wrapper = &transactionStorageWrapper{
			tx:             tx,
			underlying:     exec.storage,
			namespace:      h.database,
			separator:      separator,
			mutatedNodeIDs: make(map[string]struct{}),
		}
	}
	return h.runtime.run(context.WithValue(ctx, ctxKeyTxStorage, wrapper), exec, wrapper, triggers, changes, apocTriggerBefore)
}

func (h *apocTriggerHook) AfterCommit(changes *storage.ChangeSet) {
	if apocTriggerTagged(changes.Metadata) {
		return
	}
	if triggers := h.byPhase[apocTriggerAfter]; len(triggers) > 0 {
		h.runtime.runInNewTransaction(h.database, triggers, changes, apocTriggerAfter)
	}
	if triggers := h.byPhase[apocTriggerAfterAsync]; len(triggers) > 0 {
		h.runtime.enqueue(func() {
			h.runtime.runInNewTransaction(h.database, triggers, changes, apocTriggerAfterAsync)
		})
	}
}

func (h *apocTriggerHook) AfterRollback(changes *storage.ChangeSet) {
	if triggers := h.byPhase[apocTriggerRollback]; len(triggers) > 0 {
		h.runtime.runInNewTransaction(h.database, triggers, changes, apocTriggerRollback)
	}
}

// apocTriggerTagged reports a transaction an after-phase trigger run made.
func apocTriggerTagged(metadata map[string]any) bool {
	tagged, _ := metadata["apoc.trigger"].(bool)
	return tagged
}

func (r *TriggerRuntime) enqueue(run func()) {
	defer func() {
		// Closed: the runtime is shutting down; the work is dropped.
		_ = recover()
	}()
	r.async <- run
}

// runInNewTransaction runs after, afterAsync or rollback triggers in one new
// transaction of the database; after and afterAsync tag it so its commit
// runs no after triggers of its own.
func (r *TriggerRuntime) runInNewTransaction(database string, triggers []*apocTrigger, changes *storage.ChangeSet, phase apocTriggerPhase) {
	exec, err := r.config.ExecutorFor(database)
	if err != nil {
		r.config.Logger.Warn("apoc trigger: no executor", "database", database, "phase", phase.String(), "error", err)
		return
	}
	if !storageIsLeader(exec.storage) {
		return
	}
	ctx := context.Background()
	if phase != apocTriggerRollback {
		ctx = WithTransactionMetadata(ctx, map[string]any{"apoc.trigger": true})
	}
	_, err = exec.executeWithImplicitTransactionCallback(ctx, "", "", func(txCtx context.Context, txExec *StorageExecutor) (*ExecuteResult, error) {
		return &ExecuteResult{}, r.run(txCtx, txExec, txExec.storage, triggers, changes, phase)
	})
	if err != nil {
		r.config.Logger.Warn("apoc trigger failed", "database", database, "phase", phase.String(), "error", err)
	}
}

// run runs triggers in order over view: each with the pass's parameters,
// its own params and $trigger. Every trigger runs; the error names each
// one that failed.
func (r *TriggerRuntime) run(ctx context.Context, exec *StorageExecutor, view storage.Engine, triggers []*apocTrigger, changes *storage.ChangeSet, phase apocTriggerPhase) error {
	state := newApocTriggerParams(changes, phase)
	failures := make(map[string]string)
	var failed []string
	ctx = withQueryParams(ctx, nil) // the triggering statement's parameters aren't the trigger's
	for _, trigger := range triggers {
		params := state.params(view)
		for name, value := range trigger.Params {
			params[name] = value
		}
		params["trigger"] = trigger.Name
		runner := exec.cloneWithStorage(view)
		runner.txContext = nil
		runCtx := context.WithValue(ctx, expressionFailureKey{}, &expressionFailure{})
		_, err := runner.executeUserStatement(runCtx, trigger.Statement, params)
		if err == nil {
			err = getExpressionFailure(runCtx)
		}
		if err != nil {
			failures[trigger.Name] = err.Error()
			failed = append(failed, trigger.Name)
		}
	}
	if len(failed) > 0 {
		return apocTriggerFailedError(failures, failed)
	}
	return nil
}

// apocTriggerFailedError is a failed trigger pass: APOC's
// "Error executing triggers {name=error, …}", with Neo4j's code for a
// transaction listener that failed the commit.
func apocTriggerFailedError(failures map[string]string, order []string) error {
	parts := make([]string, 0, len(order))
	for _, name := range order {
		parts = append(parts, name+"="+failures[name])
	}
	return localizedStatusError("Neo.ClientError.Transaction.TransactionHookFailed", "TransactionHookFailed",
		localization.CypherProceduresTriggerFailed("{"+strings.Join(parts, ", ")+"}"))
}

// storageIsLeader reports whether the engine under storage takes writes:
// false on a replication follower.
func storageIsLeader(engine storage.Engine) bool {
	for engine != nil {
		if replica, ok := engine.(interface{ IsLeader() bool }); ok {
			return replica.IsLeader()
		}
		wrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			return true
		}
		inner := wrapper.GetInnerEngine()
		if inner == engine {
			return true
		}
		engine = inner
	}
	return true
}
