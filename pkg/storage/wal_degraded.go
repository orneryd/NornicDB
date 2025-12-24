package storage

// IsDegraded reports whether this WAL has detected corruption and performed
// best-effort recovery actions. When true, callers should consider the
// durability state degraded and surface it in health endpoints.
func (w *WAL) IsDegraded() bool {
	return w.degraded.Load()
}

// LastCorruptionDiagnostics returns the last corruption diagnostics captured by this WAL.
// Returns nil if no corruption has been observed.
func (w *WAL) LastCorruptionDiagnostics() *CorruptionDiagnostics {
	if v := w.lastCorruption.Load(); v != nil {
		if diag, ok := v.(*CorruptionDiagnostics); ok {
			return diag
		}
	}
	return nil
}

