package replication

import (
	"errors"
	"fmt"
)

// ErrRetiredCommand identifies historical raw commands that must be migrated
// to a typed entity commit or the normal query API, never silently replayed.
var ErrRetiredCommand = errors.New("retired raw replication command")

func isRetiredCommand(kind CommandType) bool {
	return kind == cmdRetiredSetProperty || kind == cmdRetiredBatchWrite || kind == cmdRetiredCypher
}
func retiredCommandError(kind CommandType) error {
	if !isRetiredCommand(kind) {
		return nil
	}
	return fmt.Errorf("%w: wire slot %d; use typed entity commits or the normal query API", ErrRetiredCommand, kind)
}
