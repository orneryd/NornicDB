package nornicdb

import (
	"errors"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// SetFailureErrorRedactor attaches the credential owner for this worker's
// active embedder configuration. It is consulted only on provider failure and
// when a stored failure is read, never on successful query or embed paths.
func (ew *EmbedWorker) SetFailureErrorRedactor(redact func(storage.NodeID, error) error) {
	if ew == nil {
		return
	}
	ew.mu.Lock()
	ew.failureErrorRedactor = redact
	ew.mu.Unlock()
}

func (ew *EmbedWorker) redactFailure(nodeID storage.NodeID, failure error) error {
	if failure == nil {
		return nil
	}
	if ew != nil {
		ew.mu.Lock()
		redact := ew.failureErrorRedactor
		ew.mu.Unlock()
		if redact != nil {
			if safe := redact(nodeID, failure); safe != nil {
				return safe
			}
		}
	}
	return observability.RedactError(failure)
}

func (ew *EmbedWorker) redactFailureText(nodeID storage.NodeID, text string) string {
	if text == "" {
		return ""
	}
	return ew.redactFailure(nodeID, errors.New(text)).Error()
}
