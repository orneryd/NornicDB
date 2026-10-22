package storage

// ReadCapabilityForEngine finds an optional read capability through tracing and
// WAL decorators. Neither decorator owns a separate read view, so its method set
// must not hide a capability or advertise an unsupported forwarding method.
//
// Namespace, transaction, composite and other view-changing engines are
// boundaries: the capability must belong to that view, never its inner engine.
// T should be a read interface, for example ProjectedLabelNodeReader. Writes and
// decorator-owned operations continue through the original Engine.
func ReadCapabilityForEngine[T any](engine Engine) (T, bool) {
	for engine != nil {
		switch engine.(type) {
		case *TracedEngine, *WALEngine:
			engine = engine.(EngineUnwrapper).GetInnerEngine()
		default:
			capability, ok := engine.(T)
			return capability, ok
		}
	}
	var absent T
	return absent, false
}
