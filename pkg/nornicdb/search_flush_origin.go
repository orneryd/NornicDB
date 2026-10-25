package nornicdb

// queuedVectorWorkOrigin transports the oldest origin already admitted to a
// pending flush, including a retained retry. It makes no retirement decision.
func (e *dbSearchService) queuedVectorWorkOrigin() uint64 {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	var origin uint64
	found := false
	include := func(value uint64) {
		if !found || value < origin {
			origin = value
			found = true
		}
	}
	for _, work := range e.pendingOps {
		include(work.VectorWorkOrigin())
	}
	for _, work := range e.pendingEdgeOps {
		include(work.VectorWorkOrigin())
	}
	for _, retry := range e.refreshRetries {
		if !retry.work.Node.IsZero() {
			include(retry.work.Node.VectorWorkOrigin())
		} else {
			include(retry.work.Edge.VectorWorkOrigin())
		}
	}
	return origin
}
