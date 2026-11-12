package search

import "github.com/orneryd/nornicdb/pkg/lifecycle"

// hnswConstructionCoordinates owns the immutable construction source until
// publication. Catch-up changes coordinates only when it changes the graph,
// never when a newer canonical write is merely accepted. After construction,
// every access to this owner is protected by the target graph's existing mutex.
// No distance calculation acquires a service or catch-up queue lock.
type hnswConstructionCoordinates struct {
	baseline   VectorLookup
	applied    map[string][]float32
	release    func() error
	graph      *HNSWIndex
	completion lifecycle.OnceCompletion
}

func (coordinates *hnswConstructionCoordinates) lookup(id string) ([]float32, bool) {
	if value, ok := coordinates.applied[id]; ok {
		return value, true
	}
	if coordinates.baseline == nil {
		return nil, false
	}
	return coordinates.baseline(id)
}

// publish changes only the lookup's ownership; no float array is copied or
// traversed under the service's bounded publication locks. Quiescence has
// finished all catch-up inserts before the caller reaches this boundary.
func (coordinates *hnswConstructionCoordinates) publish(lookup VectorLookup) {
	graph := coordinates.graph
	graph.mu.Lock()
	graph.vectorLookup = lookup
	graph.construction = nil
	coordinates.baseline, coordinates.applied = nil, nil
	graph.mu.Unlock()
}

// Close retires the source under the graph lock before releasing a mapped
// lease. Cancellation can detach catch-up with an insert still in flight: this
// lock waits for any current coordinate consumer, and clearing the lookup makes
// future work on that discarded graph unable to read the released mapping.
func (coordinates *hnswConstructionCoordinates) Close() error {
	return coordinates.completion.Do(func() error {
		if graph := coordinates.graph; graph != nil {
			graph.mu.Lock()
			if graph.construction == coordinates {
				graph.vectorLookup, graph.construction = nil, nil
			}
			coordinates.baseline, coordinates.applied = nil, nil
			graph.mu.Unlock()
		} else {
			coordinates.baseline, coordinates.applied = nil, nil
		}
		if coordinates.release != nil {
			return coordinates.release()
		}
		return nil
	})
}
