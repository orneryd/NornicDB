package search

import (
	"errors"
	"fmt"
)

// Malformed stored vectors keep their existing skip semantics. Operational
// failures must reach the live caller or the build that accepted its write.
func vectorInstallError(id string, err error) error {
	if errors.Is(err, ErrDimensionMismatch) || errors.Is(err, ErrVectorDisabled) {
		return nil
	}
	return fmt.Errorf("index vector %q: %w", id, err)
}

func (write deferredIndexWrite) replayError(err error) error {
	kind, id := "node", write.nodeID
	if write.node != nil {
		id = string(write.node.ID)
	} else if write.edge != nil {
		kind, id = "edge", string(write.edge.ID)
	} else if write.edgeID != "" {
		kind, id = "edge", write.edgeID
	}
	return fmt.Errorf("apply queued %s %q: %w", kind, id, err)
}
