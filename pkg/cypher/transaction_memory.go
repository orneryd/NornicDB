package cypher

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
	"weak"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Transaction memory limits (#1343), with Neo4j's settings and errors:
//
//   - db.memory.transaction.max limits one transaction of a database;
//   - db.memory.transaction.total.max limits all transactions of a database;
//   - dbms.memory.transaction.total.max limits all transactions of the process.
//
// Zero is unlimited for each. A statement's memory is an estimate in bytes,
// as Neo4j's MemoryTracker keeps one: the places where a statement's working
// set grows (materialized rows, aggregation groups and their collected
// lists, DISTINCT keys, path expansions, CALL subquery results) add the
// estimated size of what they keep (estimateValueBytes), not Go runtime
// statistics. A statement that would go over a limit fails with
// Neo.TransientError.General.TransactionMemoryLimit (its own limit) or
// Neo.TransientError.General.MemoryPoolOutOfMemoryError (a pool); its
// statement context is cancelled with that error, so every loop that
// already stops on cancellation stops, and the statement's memory goes back
// to the pools when it ends, whether it committed, rolled back or failed.
//
// A statement's memory is released when the statement ends: a result handed
// to the caller and an explicit transaction's uncommitted writes are not
// counted. With no limit configured nothing is tracked (transactionMemoryActive).

const (
	transactionMemoryMaxSetting       = "db.memory.transaction.max"
	databaseTransactionMemorySetting  = "db.memory.transaction.total.max"
	globalTransactionMemorySetting    = "dbms.memory.transaction.total.max"
	transactionMemoryLimitCode        = "Neo.TransientError.General.TransactionMemoryLimit"
	memoryPoolOutOfMemoryCode         = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"
	transactionMemoryLimitGQLStatus   = "50N42"
	transactionMemoryReservationChunk = 1 << 20
)

// TransactionMemoryLimits are byte limits, 0 for unlimited.
type TransactionMemoryLimits struct {
	// TransactionMax is db.memory.transaction.max.
	TransactionMax int64
	// DatabaseTotalMax is db.memory.transaction.total.max.
	DatabaseTotalMax int64
}

// memoryPool is a byte pool transactions reserve from.
type memoryPool struct {
	setting string
	limit   atomic.Int64
	used    atomic.Int64
}

// reserve takes n bytes, or reports the pool's use when n bytes would go
// over limit, the limit the reserving statement started with.
func (p *memoryPool) reserve(n, limit int64) (ok bool, used int64) {
	for {
		used = p.used.Load()
		if limit > 0 && used+n > limit {
			return false, used
		}
		if p.used.CompareAndSwap(used, used+n) {
			return true, used
		}
	}
}

func (p *memoryPool) release(n int64) {
	if n != 0 {
		p.used.Add(-n)
	}
}

// databaseMemory is one database's limits: its own (explicit) or the
// process defaults.
type databaseMemory struct {
	explicit       bool // guarded by transactionMemoryRegistry.mu
	transactionMax atomic.Int64
	pool           memoryPool
}

type transactionMemoryRegistry struct {
	global memoryPool

	mu        sync.RWMutex
	defaults  TransactionMemoryLimits
	databases map[string]*databaseMemory
	// globalSet is set once dbms.setConfigValue set the global limit.
	globalSet bool
}

var transactionMemory = &transactionMemoryRegistry{
	global:    memoryPool{setting: globalTransactionMemorySetting},
	databases: make(map[string]*databaseMemory),
}

// transactionMemoryActive is set while any limit is configured; without one
// statements start tracking nothing.
var transactionMemoryActive atomic.Bool

// trackedStatements counts the running statements that started while a
// limit was configured: they stay counted to their end under the limits
// they started with, whatever the configuration becomes meanwhile.
var trackedStatements atomic.Int64

// transactionMemoryTracking reports whether any statement may be tracked:
// a limit is configured, or a statement that started under one still runs.
// While it is false nothing sizes or charges anything.
func transactionMemoryTracking() bool {
	return transactionMemoryActive.Load() || trackedStatements.Load() > 0
}

// ConfigureTransactionMemory sets dbms.memory.transaction.total.max (global)
// and the limits of every database that has none of its own.
func ConfigureTransactionMemory(global int64, defaults TransactionMemoryLimits) {
	r := transactionMemory
	r.mu.Lock()
	defer r.mu.Unlock()
	r.global.limit.Store(nonNegativeBytes(global))
	r.defaults = TransactionMemoryLimits{TransactionMax: nonNegativeBytes(defaults.TransactionMax), DatabaseTotalMax: nonNegativeBytes(defaults.DatabaseTotalMax)}
	for _, database := range r.databases {
		if !database.explicit {
			database.transactionMax.Store(r.defaults.TransactionMax)
			database.pool.limit.Store(r.defaults.DatabaseTotalMax)
		}
	}
	r.updateActiveLocked()
}

// SetDatabaseTransactionMemoryLimits sets a database's own limits
// (db.memory.transaction.max, db.memory.transaction.total.max); they apply
// to the statements that start after the call.
func SetDatabaseTransactionMemoryLimits(database string, limits TransactionMemoryLimits) {
	r := transactionMemory
	r.mu.Lock()
	defer r.mu.Unlock()
	memory := r.databaseLocked(database)
	memory.explicit = true
	memory.transactionMax.Store(nonNegativeBytes(limits.TransactionMax))
	memory.pool.limit.Store(nonNegativeBytes(limits.DatabaseTotalMax))
	r.updateActiveLocked()
}

// TransactionMemoryPoolUsage is the bytes the transactions of the process
// (global) and of database hold now.
func TransactionMemoryPoolUsage(database string) (global, databaseUsed int64) {
	r := transactionMemory
	r.mu.RLock()
	memory := r.databases[normalizedMemoryDatabase(database)]
	r.mu.RUnlock()
	if memory != nil {
		databaseUsed = memory.pool.used.Load()
	}
	return r.global.used.Load(), databaseUsed
}

func (r *transactionMemoryRegistry) databaseLocked(database string) *databaseMemory {
	database = normalizedMemoryDatabase(database)
	memory := r.databases[database]
	if memory == nil {
		memory = &databaseMemory{pool: memoryPool{setting: databaseTransactionMemorySetting}}
		memory.transactionMax.Store(r.defaults.TransactionMax)
		memory.pool.limit.Store(r.defaults.DatabaseTotalMax)
		r.databases[database] = memory
	}
	return memory
}

func (r *transactionMemoryRegistry) database(database string) *databaseMemory {
	r.mu.RLock()
	memory := r.databases[normalizedMemoryDatabase(database)]
	r.mu.RUnlock()
	if memory != nil {
		return memory
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.databaseLocked(database)
}

func (r *transactionMemoryRegistry) updateActiveLocked() {
	active := r.global.limit.Load() > 0 || r.defaults.TransactionMax > 0 || r.defaults.DatabaseTotalMax > 0
	for _, database := range r.databases {
		if active {
			break
		}
		active = database.transactionMax.Load() > 0 || database.pool.limit.Load() > 0
	}
	transactionMemoryActive.Store(active)
}

func normalizedMemoryDatabase(database string) string {
	if database == "" {
		return "nornic"
	}
	return database
}

func nonNegativeBytes(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

// transactionMemoryTracker is a running transaction's estimate: the bytes
// its running statement holds and the bytes it reserved from its pools.
type transactionMemoryTracker struct {
	used     atomic.Int64
	reserved atomic.Int64
	limit    int64
	database *databaseMemory
	// databaseLimit and globalLimit are the pool limits the statement
	// started with; counted is set while it is in trackedStatements.
	databaseLimit int64
	globalLimit   int64
	counted       bool
	// statement is cancelled with the failure, so the statement stops.
	statement *statementContext
	root      memoryScope

	mu      sync.Mutex // serializes pool reservations
	failure atomic.Pointer[transactionMemoryFailure]

	// known are the lists collect() built and counted, with their counted
	// size: rows that keep one size it from here instead of walking it
	// again (knownListBytes).
	knownMu   sync.Mutex
	known     [knownLists]knownList
	knownNext int
	// sizes caches the estimated size of the large lists the statement's
	// rows hold: a list every row of an operator, or of every per-row CALL
	// run, carries is walked once (guarded by knownMu; sizedValue). Like
	// known, it holds no reference to a list (heldListKey).
	sizes map[heldListKey]heldListSize

	// ownedRows and ownedCount identify the last rows an operator charged in
	// full (memoryScope.ownRows): the clause pipeline gives back the charge
	// of the rows they replace (rowGenerations).
	ownedRows  atomic.Uintptr
	ownedCount atomic.Int64
}

// knownList is a list collect() counted: its key, the list itself (held
// without keeping it alive, heldList) and its size. Only lists of
// knownListMin values or more are recorded; shorter ones are cheap to walk.
type knownList struct {
	key   heldListKey
	list  heldList
	bytes int64
}

const (
	knownLists   = 32
	knownListMin = 16
)

// noteList records a list collect() built, of bytes estimated bytes.
func (t *transactionMemoryTracker) noteList(values []interface{}, bytes int64) {
	key, _ := heldListKeyOf(values)
	list := heldListOf(unsafe.Pointer(unsafe.SliceData(values)))
	t.knownMu.Lock()
	t.known[t.knownNext] = knownList{key: key, list: list, bytes: bytes}
	t.knownNext = (t.knownNext + 1) % knownLists
	t.knownMu.Unlock()
}

// knownListBytes is the counted size of values when collect() built it.
func (t *transactionMemoryTracker) knownListBytes(values []interface{}) (int64, bool) {
	if t == nil || len(values) < knownListMin {
		return 0, false
	}
	key, _ := heldListKeyOf(values)
	data := unsafe.Pointer(unsafe.SliceData(values))
	t.knownMu.Lock()
	defer t.knownMu.Unlock()
	for _, known := range t.known {
		if known.key == key && known.list.holds(data) {
			return known.bytes, true
		}
	}
	return 0, false
}

// valueBytes is estimateValueBytes, with the lists collect() counted sized
// from what it counted.
func (t *transactionMemoryTracker) valueBytes(value interface{}) int64 {
	if list, ok := value.([]interface{}); ok {
		if bytes, known := t.knownListBytes(list); known {
			return bytes
		}
	}
	return estimateValueBytes(value)
}

type transactionMemoryFailure struct{ err error }

var transactionMemoryTrackers = sync.Pool{New: func() any {
	tracker := &transactionMemoryTracker{}
	tracker.root.tracker = tracker
	return tracker
}}

// beginTransactionMemory starts tracking the statement tx runs, when a
// limit is configured.
func (e *StorageExecutor) beginTransactionMemory(tx *runningTransaction, statement *statementContext) {
	if !transactionMemoryActive.Load() {
		if tx.memory.Load() != nil {
			tx.memory.Store(nil)
		}
		return
	}
	tracker := tx.memory.Load()
	if tracker == nil {
		// Auto-commit statements return their tracker when they end
		// (runningStatement.done), so tracking allocates nothing per
		// statement.
		tracker = transactionMemoryTrackers.Get().(*transactionMemoryTracker)
	}
	database := transactionMemory.database(tx.database)
	tracker.mu.Lock()
	tracker.database = database
	// The statement keeps the limits it starts with (dynamic settings apply
	// to the statements that start after a change).
	tracker.limit = database.transactionMax.Load()
	tracker.databaseLimit = database.pool.limit.Load()
	tracker.globalLimit = transactionMemory.global.limit.Load()
	tracker.statement = statement
	if !tracker.counted {
		tracker.counted = true
		trackedStatements.Add(1)
	}
	tracker.mu.Unlock()
	tracker.failure.Store(nil)
	tx.memory.Store(tracker)
}

// memoryFailure is the memory-limit error of the statement s registered,
// nil when it stayed within its limits.
func (s runningStatement) memoryFailure() error {
	if s.memory != nil {
		return s.memory.tracker.err()
	}
	if s.tx != nil {
		if tracker := s.tx.memory.Load(); tracker != nil {
			return tracker.err()
		}
	}
	return nil
}

// end returns the statement's memory to the pools.
func (t *transactionMemoryTracker) end() {
	t.mu.Lock()
	reserved := t.reserved.Swap(0)
	t.used.Store(0)
	t.statement = nil
	if t.counted {
		t.counted = false
		trackedStatements.Add(-1)
	}
	t.mu.Unlock()
	t.ownedRows.Store(0)
	t.ownedCount.Store(0)
	t.knownMu.Lock()
	t.known = [knownLists]knownList{}
	t.knownNext = 0
	t.sizes = nil
	t.knownMu.Unlock()
	if reserved != 0 {
		t.database.pool.release(reserved)
		transactionMemory.global.release(reserved)
	}
}

func (t *transactionMemoryTracker) err() error {
	if failure := t.failure.Load(); failure != nil {
		return failure.err
	}
	return nil
}

// fail records the statement's failure (the first one stands) and cancels
// the statement with it.
func (t *transactionMemoryTracker) fail(err error) error {
	if !t.failure.CompareAndSwap(nil, &transactionMemoryFailure{err: err}) {
		return t.err()
	}
	t.mu.Lock()
	statement := t.statement
	t.mu.Unlock()
	if statement != nil {
		statement.cancel(err)
	}
	return err
}

// reserve covers used bytes with reservations from the database and global
// pools: a chunk ahead when the pools have room, or exactly what is needed.
func (t *transactionMemoryTracker) reserve(allocation int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	need := t.used.Load() - t.reserved.Load()
	if need <= 0 {
		return nil
	}
	chunk := need
	if chunk < transactionMemoryReservationChunk {
		chunk = transactionMemoryReservationChunk
	}
	pool, ok, limit, poolUsed := t.reservePools(chunk)
	if !ok && chunk > need {
		pool, ok, limit, poolUsed = t.reservePools(need)
		chunk = need
	}
	if !ok {
		t.mu.Unlock()
		err := t.fail(memoryLimitExceededError(memoryPoolOutOfMemoryCode, allocation, limit, poolUsed, pool.setting))
		t.mu.Lock()
		return err
	}
	t.reserved.Add(chunk)
	return nil
}

func (t *transactionMemoryTracker) reservePools(n int64) (failed *memoryPool, ok bool, limit, used int64) {
	if ok, used = t.database.pool.reserve(n, t.databaseLimit); !ok {
		return &t.database.pool, false, t.databaseLimit, used
	}
	if ok, used = transactionMemory.global.reserve(n, t.globalLimit); !ok {
		t.database.pool.release(n)
		return &transactionMemory.global, false, t.globalLimit, used
	}
	return nil, true, 0, 0
}

// memoryScope is what a statement holds; its memory goes back when the
// statement ends. An execution nested in the statement (a subquery run per
// row) gives back what it took when it ends (mark, releaseSince): the
// statement runs its nested executions one after another, so what the
// statement took since the nested execution started is that execution's.
type memoryScope struct {
	tracker *transactionMemoryTracker
}

// knownLists is the scope's record of the lists collect() counted, nil when
// nothing is tracked.
func (s *memoryScope) knownLists() *transactionMemoryTracker {
	if s == nil {
		return nil
	}
	return s.tracker
}

// mark is the scope's use now, for releaseSince.
func (s *memoryScope) mark() int64 {
	if s == nil {
		return 0
	}
	return s.tracker.used.Load()
}

// releaseSince gives back what the scope took since mark.
func (s *memoryScope) releaseSince(mark int64) {
	if s == nil {
		return
	}
	if n := s.tracker.used.Load() - mark; n > 0 {
		s.give(n)
	}
}

type memoryScopeKey struct{}

// memoryScopeFromContext is the scope a statement's memory grows in, nil
// when nothing is tracked.
func memoryScopeFromContext(ctx context.Context) *memoryScope {
	if !transactionMemoryTracking() || ctx == nil {
		return nil
	}
	scope, _ := ctx.Value(memoryScopeKey{}).(*memoryScope)
	return scope
}

// memoryFailureFromContext is the memory-limit error of the statement ctx
// runs, nil when it stayed within its limits or nothing is tracked.
func memoryFailureFromContext(ctx context.Context) error {
	if scope := memoryScopeFromContext(ctx); scope != nil {
		return scope.tracker.err()
	}
	return nil
}

// grow adds n estimated bytes, or fails the statement when they go over a
// limit. Operators that grow per row or per value use a memoryAccount.
func (s *memoryScope) grow(n int64) error {
	if s == nil || n <= 0 {
		return nil
	}
	return s.addCharge(n, n)
}

// addCharge takes n bytes for an allocation of allocation bytes (n includes the
// credit an account takes ahead): a limit's error names the allocation.
func (s *memoryScope) addCharge(n, allocation int64) error {
	t := s.tracker
	if err := t.err(); err != nil {
		return err
	}
	used := t.used.Add(n)
	if t.limit > 0 && used > t.limit {
		return t.fail(memoryLimitExceededError(transactionMemoryLimitCode, allocation, t.limit, used-n, transactionMemoryMaxSetting))
	}
	if used > t.reserved.Load() {
		return t.reserve(allocation)
	}
	return nil
}

// give returns n bytes an account took and didn't spend.
func (s *memoryScope) give(n int64) {
	s.tracker.used.Add(-n)
}

// growRows adds the estimated size of rows (rowEstimator).
func (s *memoryScope) growRows(rows []pipelineRow) error {
	if s == nil {
		return nil
	}
	estimate := rowEstimator{known: s.tracker}
	var total int64
	for _, row := range rows {
		total += estimate.rowBytes(row)
	}
	return s.grow(total)
}

// ownRows records rows as charged in full by the operator that built them
// (memoryRetention.owned, rowGenerations.charge).
func (s *memoryScope) ownRows(rows []pipelineRow) {
	s.tracker.ownedRows.Store(uintptr(unsafe.Pointer(unsafe.SliceData(rows))))
	s.tracker.ownedCount.Store(int64(len(rows)))
}

// ownsRows reports whether rows are the last rows an operator charged in
// full.
func (s *memoryScope) ownsRows(rows []pipelineRow) bool {
	return s.tracker.ownedRows.Load() == uintptr(unsafe.Pointer(unsafe.SliceData(rows))) &&
		s.tracker.ownedCount.Load() == int64(len(rows))
}

// memoryAccountChunk is the most credit an account takes ahead at once.
const memoryAccountChunk = 64 << 10

// memoryAccount is one operator's share of its scope, used by one
// goroutine, as Neo4j's local memory tracker is: it spends a local credit
// without atomic operations and takes more from its scope only when the
// credit runs out. A refill takes ahead at most what the account spent so
// far, up to memoryAccountChunk, so an account that is never closed holds
// no more unspent credit than it spent. Near the per-transaction limit a
// refill takes exactly what the allocation needs: the limit stays exact.
// The zero value with a nil scope tracks nothing.
type memoryAccount struct {
	scope  *memoryScope
	credit int64
	spent  int64
}

// grow spends n estimated bytes, or fails the statement when they go over
// a limit.
func (a *memoryAccount) grow(n int64) error {
	if a.scope == nil || n <= 0 {
		return nil
	}
	if n <= a.credit {
		a.credit -= n
		a.spent += n
		return nil
	}
	return a.refill(n)
}

// growValue spends the estimated size of value.
func (a *memoryAccount) growValue(value interface{}) error {
	if a.scope == nil {
		return nil
	}
	// The common values first, without estimateValueBytes' whole switch.
	switch v := value.(type) {
	case int64, float64, bool:
		return a.grow(valueSlotBytes)
	case string:
		return a.grow(valueSlotBytes + stringHeaderBytes + int64(len(v)))
	}
	return a.grow(estimateValueBytes(value))
}

func (a *memoryAccount) refill(n int64) error {
	need := n - a.credit
	ahead := a.spent
	if ahead > memoryAccountChunk {
		ahead = memoryAccountChunk
	}
	if limit := a.scope.tracker.limit; limit > 0 {
		if room := limit - a.scope.tracker.used.Load() - need; ahead > room {
			ahead = max(room, 0)
		}
	}
	if err := a.scope.addCharge(need+ahead, n); err != nil {
		return err
	}
	a.credit += ahead + need - n
	a.spent += n
	return nil
}

// noteList records the list this account's collect() built: what the
// account spent on its values, plus the list's own slot and capacity.
func (a *memoryAccount) noteList(values []interface{}) {
	if a.scope == nil || len(values) < knownListMin {
		return
	}
	a.scope.tracker.noteList(values, a.spent+valueSlotBytes+sliceHeaderBytes+int64(cap(values)-len(values))*valueSlotBytes)
}

// close returns the unspent credit.
func (a *memoryAccount) close() {
	if a.scope != nil && a.credit > 0 {
		a.scope.give(a.credit)
		a.credit = 0
	}
}

// handOver ends a nested execution that started at mark and whose caller
// keeps rows: what the execution took goes back, and the rows count. The
// tracker keeps its pool reservations until the statement ends.
func (s *memoryScope) handOver(mark int64, rows [][]interface{}) error {
	if s == nil {
		return nil
	}
	s.releaseSince(mark)
	var total int64
	for _, row := range rows {
		total += estimateResultRowBytes(row, s.tracker)
	}
	return s.grow(total)
}

// memoryLimitExceededError is Neo4j's error for an allocation of allocation
// bytes over limit, with used bytes in use, the limit being setting.
func memoryLimitExceededError(code string, allocation, limit, used int64, setting string) error {
	return localizedStatusError(code, transactionMemoryLimitGQLStatus, localization.CypherTransactionsMemoryLimitExceeded(
		formatMemoryBytes(allocation), formatMemoryBytes(limit), formatMemoryBytes(used), setting))
}

// formatMemoryBytes renders bytes as Neo4j's messages do: "512 B",
// "197.7 MiB", "1.0 GiB".
func formatMemoryBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	units := [...]string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	value := float64(n) / unit
	index := 0
	for value >= unit && index < len(units)-1 {
		value /= unit
		index++
	}
	return strconv.FormatFloat(value, 'f', 1, 64) + " " + units[index]
}

// sizedValueKey identifies a list or map by its backing storage and length,
// for an operator that counts what its rows share once (rowEstimator): it
// keeps the value alive while the operator runs, as the operator's rows do.
type sizedValueKey struct {
	data   unsafe.Pointer
	length int
}

const (
	// sizedValueMin is the shortest list whose size is cached: a
	// shorter one costs less to walk than to look up.
	sizedValueMin = 32
	// sizedValuesMax bounds the cache; values past it are walked each time.
	sizedValuesMax = 4096
)

// sizedValue is estimateValueBytes of value, a list or map with identity
// data and length elements: from collect()'s count or the statement's size
// cache when known, walked (and cached, a list of values or strings)
// otherwise.
func (t *transactionMemoryTracker) sizedValue(data unsafe.Pointer, length int, value interface{}) int64 {
	if t == nil || length < sizedValueMin {
		return t.valueBytes(value)
	}
	if list, ok := value.([]interface{}); ok {
		if bytes, known := t.knownListBytes(list); known {
			return bytes
		}
	}
	key, cacheable := heldListKeyOf(value)
	if !cacheable {
		return estimateValueBytes(value)
	}
	t.knownMu.Lock()
	cached, found := t.sizes[key]
	t.knownMu.Unlock()
	if found && cached.list.holds(data) {
		return cached.bytes
	}
	bytes := estimateValueBytes(value)
	t.knownMu.Lock()
	if t.sizes == nil {
		t.sizes = make(map[heldListKey]heldListSize)
	}
	if found || len(t.sizes) < sizedValuesMax {
		t.sizes[key] = heldListSize{bytes: bytes, list: heldListOf(data)}
	}
	t.knownMu.Unlock()
	return bytes
}

// heldListKey finds a list's cached size by the address of its backing
// array, its length and its capacity, without keeping it alive: a list the
// statement dropped is collected while the statement runs, whatever its
// caches recorded about it. The address can be reused once it is; the entry's
// heldList tells the list recorded there from a new one.
type heldListKey struct {
	address          uintptr
	length, capacity int
}

// heldListKeyOf is the key of a list of values or strings, the lists whose
// size takes a walk: a list of numbers is sized from its capacity, and may
// live outside the Go heap (a memory-mapped vector), where a heldList can't
// refer to it.
func heldListKeyOf(value interface{}) (heldListKey, bool) {
	switch v := value.(type) {
	case []interface{}:
		return heldListKey{address: uintptr(unsafe.Pointer(unsafe.SliceData(v))), length: len(v), capacity: cap(v)}, len(v) > 0
	case []string:
		return heldListKey{address: uintptr(unsafe.Pointer(unsafe.SliceData(v))), length: len(v), capacity: cap(v)}, len(v) > 0
	default:
		return heldListKey{}, false
	}
}

// heldList refers to the backing array of a list a cache recorded without
// keeping it alive. It holds that array while the array lives; once it is
// collected the reference is cleared, so a new list allocated at its address
// is not taken for it.
type heldList struct{ array weak.Pointer[byte] }

func heldListOf(data unsafe.Pointer) heldList {
	return heldList{array: weak.Make((*byte)(data))}
}

// holds reports whether data is the backing array the reference was made to.
func (h heldList) holds(data unsafe.Pointer) bool {
	return h.array.Value() == (*byte)(data)
}

// heldListSize is a cached list size and the list it was taken from.
type heldListSize struct {
	bytes int64
	list  heldList
}

// SetGlobalTransactionMemoryLimit changes dbms.memory.transaction.total.max
// while the process runs (dbms.setConfigValue): a running statement keeps
// the limit it started with, and statements started later meet the new one.
// A statement that started with no limit at all stays untracked to its end,
// at no cost (transactionMemoryActive), and the pool does not count it.
func SetGlobalTransactionMemoryLimit(limit int64) {
	r := transactionMemory
	r.mu.Lock()
	defer r.mu.Unlock()
	r.global.limit.Store(nonNegativeBytes(limit))
	r.globalSet = true
	r.updateActiveLocked()
}

// liveGlobalTransactionMemoryLimit is the global limit in force once
// dbms.setConfigValue set it: read from the pool, so what SHOW SETTINGS
// reports is what the pool enforces, whichever change came last.
func liveGlobalTransactionMemoryLimit() (int64, bool) {
	r := transactionMemory
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.global.limit.Load(), r.globalSet
}
