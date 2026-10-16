//go:build antlr.nomutex
// +build antlr.nomutex

package antlr

type Mutex struct{}

func (m *Mutex) Lock() {
	// No-op
}

func (m *Mutex) Unlock() {
	// No-op
}

type RWMutex struct{}

func (m *RWMutex) Lock() {
	// No-op
}

func (m *RWMutex) Unlock() {
	// No-op
}

func (m *RWMutex) RLock() {
	// No-op
}

func (m *RWMutex) RUnlock() {
	// No-op
}

func (m *Mutex) lockWithWork(check func())    { chargeWork(check) }
func (m *RWMutex) lockWithWork(check func())  { chargeWork(check) }
func (m *RWMutex) rLockWithWork(check func()) { chargeWork(check) }
