package fabric

import "sync"

// panicRelay carries the first panic of a statement's worker goroutines to
// the goroutine that waits for them, which re-panics with it (rethrow). A
// panic in a worker then unwinds the statement, whose owner rolls back every
// participant, instead of ending the process.
type panicRelay struct {
	once  sync.Once
	value interface{}
	// stop, when set, stops the other workers.
	stop func()
}

// capture records a worker's panic and stops the other workers. A worker
// defers it.
func (p *panicRelay) capture() {
	if recovered := recover(); recovered != nil {
		p.once.Do(func() { p.value = recovered })
		if p.stop != nil {
			p.stop()
		}
	}
}

// panicked reports whether a worker panicked; read it after the workers end.
func (p *panicRelay) panicked() bool { return p.value != nil }

// rethrow re-panics with the first worker panic; call it after the workers
// end.
func (p *panicRelay) rethrow() {
	if p.value != nil {
		panic(p.value)
	}
}
