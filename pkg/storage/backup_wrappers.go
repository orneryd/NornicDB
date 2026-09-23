package storage

// Backup delegates to the wrapped storage engine while preserving the
// namespace view used by query execution. The native Badger backup contains
// the complete physical store, including system and user databases.
func (n *NamespacedEngine) Backup(path string) error {
	if n == nil || n.inner == nil {
		return ErrStorageClosed
	}
	return backupEngine(n.inner, path)
}

// Restore delegates native restore to the wrapped storage engine.
func (n *NamespacedEngine) Restore(path string) error {
	if n == nil || n.inner == nil {
		return ErrStorageClosed
	}
	return restoreEngine(n.inner, path)
}

// Backup flushes acknowledged async writes before opening the native backup.
func (e *AsyncEngine) Backup(path string) error {
	if e == nil || e.engine == nil {
		return ErrStorageClosed
	}
	return e.FlushBeforeSnapshot(func() error {
		return backupEngine(e.engine, path)
	})
}

// Restore flushes pending writes before loading the native backup.
func (e *AsyncEngine) Restore(path string) error {
	if e == nil || e.engine == nil {
		return ErrStorageClosed
	}
	return e.FlushBeforeSnapshot(func() error {
		return restoreEngine(e.engine, path)
	})
}

// Backup holds the WAL mutation barrier while Badger creates its consistent
// streaming snapshot.
func (w *WALEngine) Backup(path string) error {
	if w == nil || w.engine == nil {
		return ErrStorageClosed
	}
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()
	return backupEngine(w.engine, path)
}

// Restore holds the WAL mutation barrier while Badger loads the backup.
func (w *WALEngine) Restore(path string) error {
	if w == nil || w.engine == nil {
		return ErrStorageClosed
	}
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()
	return restoreEngine(w.engine, path)
}
