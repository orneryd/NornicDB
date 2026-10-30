package replication

import "github.com/orneryd/nornicdb/pkg/localization"

func localizedError(message localization.Message, cause error) error {
	return localization.NewLocalizedError(string(message.ID), message, cause)
}

// Errors returned by replication operations.
var (
	// ErrNotLeader is returned when a write is attempted on a non-leader node.
	ErrNotLeader = localizedError(localization.ReplicationNotLeader(), nil)

	// ErrNoLeader is returned when no leader is available in the cluster.
	ErrNoLeader = localizedError(localization.ReplicationNoLeader(), nil)

	// ErrTimeout is returned when an operation times out.
	ErrTimeout = localizedError(localization.ReplicationOperationTimedOut(), nil)

	// ErrClosed is returned when operating on a closed replicator.
	ErrClosed = localizedError(localization.ReplicationClosed(), nil)

	// ErrStandbyMode is returned when writes are attempted on a standby node.
	ErrStandbyMode = localizedError(localization.ReplicationStandbyMode(), nil)

	// ErrNotReady is returned when the replicator hasn't finished initialization.
	ErrNotReady = localizedError(localization.ReplicationNotReady(), nil)
)
