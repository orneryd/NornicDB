package cypher

import "sync/atomic"

// Scoped executors share publication, rather than copying the model visible
// when their scope was created. Disposal remains with the DB provider owner.
type inferenceManagerBinding struct {
	current atomic.Pointer[inferenceManagerState]
}
type inferenceManagerState struct{ manager InferenceManager }

// ShareInferenceManagerFrom connects this scope to its parent's current and
// future model publication. Configure before exposing the scoped executor.
func (e *StorageExecutor) ShareInferenceManagerFrom(parent *StorageExecutor) {
	if parent != nil {
		e.inferenceManager = parent.inferenceManager
	}
}
