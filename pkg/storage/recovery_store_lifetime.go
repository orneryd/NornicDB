package storage

// recoveredStore owns the allocation until recovery returns it successfully.
// Keeping ownership separate from the result pointer also covers errors that
// deliberately return nil, and panics before the caller receives the store.
type recoveredStore struct {
	engine *MemoryEngine
}

func newRecoveredStore() recoveredStore {
	return recoveredStore{engine: NewMemoryEngine()}
}

func (store *recoveredStore) close() {
	if store.engine != nil {
		_ = store.engine.Close()
	}
}

func (store *recoveredStore) release() *MemoryEngine {
	engine := store.engine
	store.engine = nil
	return engine
}
