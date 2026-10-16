package cypher

import (
	"context"
	"runtime"
	"sync"
)

// ResultMemory is the reservation statement results hold in the memory
// pools while their caller keeps them (#1343): a Bolt session until the
// client pulls or discards a result, an HTTP request until its response is
// written. A caller that keeps results makes one ResultMemory, runs its
// statements under WithResultMemory, and calls Release when it lets the
// results go. Release may be called more than once; a ResultMemory nobody
// releases gives its reservation back when it is garbage collected.
//
// Without a ResultMemory a statement's reservation goes back when the
// statement ends, as for an embedded caller that owns the result itself.
type ResultMemory struct {
	mu    sync.Mutex
	holds []resultMemoryHold
	armed bool
}

type resultMemoryHold struct {
	database *databaseMemory
	bytes    int64
}

type resultMemoryKey struct{}

// WithResultMemory makes the statements ctx runs hand their results'
// reservations to memory when they end, instead of giving them back.
func WithResultMemory(ctx context.Context, memory *ResultMemory) context.Context {
	if memory == nil {
		return ctx
	}
	return context.WithValue(ctx, resultMemoryKey{}, memory)
}

func resultMemoryFromContext(ctx context.Context) *ResultMemory {
	memory, _ := ctx.Value(resultMemoryKey{}).(*ResultMemory)
	return memory
}

// Bytes is the reservation memory holds now.
func (m *ResultMemory) Bytes() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var total int64
	for _, hold := range m.holds {
		total += hold.bytes
	}
	return total
}

// Release gives the reservation back to the pools.
func (m *ResultMemory) Release() {
	if m == nil {
		return
	}
	m.mu.Lock()
	holds := m.holds
	m.holds = nil
	m.mu.Unlock()
	for _, hold := range holds {
		hold.database.pool.release(hold.bytes)
		transactionMemory.global.release(hold.bytes)
	}
}

// hold takes over bytes of a statement's reservation from database's pool
// and the global pool.
func (m *ResultMemory) hold(database *databaseMemory, bytes int64) {
	m.mu.Lock()
	m.holds = append(m.holds, resultMemoryHold{database: database, bytes: bytes})
	arm := !m.armed
	m.armed = true
	m.mu.Unlock()
	if arm {
		runtime.SetFinalizer(m, func(memory *ResultMemory) { memory.Release() })
	}
}

// keepResult makes the statement's tracker keep the result's share of its
// reservation for the caller's ResultMemory when the statement ends
// (transactionMemoryTracker.end), when the caller keeps results.
func (s runningStatement) keepResult(ctx context.Context, result *ExecuteResult) {
	if s.tx == nil || result == nil || len(result.Rows) == 0 {
		return
	}
	tracker := s.tx.memory.Load()
	if tracker == nil {
		return
	}
	memory := resultMemoryFromContext(ctx)
	if memory == nil {
		return
	}
	var bytes int64
	for _, row := range result.Rows {
		bytes += estimateResultRowBytes(row, tracker)
	}
	tracker.mu.Lock()
	tracker.resultMemory, tracker.resultBytes = memory, bytes
	tracker.mu.Unlock()
}

// TransactionMemoryLimited reports whether any transaction memory limit is
// configured: without one no statement has a reservation to hand over, and
// callers need no ResultMemory.
func TransactionMemoryLimited() bool {
	return transactionMemoryActive.Load()
}
