package replication

import (
	"context"
	"path/filepath"
	"time"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// StartedRuntime is a running replication stack. Its owner stops Replicator,
// closes Transport and Adapter, then closes the underlying storage engine.
// Engine routes graph operations through the initialized replicator.
type StartedRuntime struct {
	Engine     storage.Engine
	Replicator Replicator
	Adapter    *StorageAdapter
	Transport  Transport
}

// StartRuntime constructs and starts replication from the complete loaded
// configuration, including authentication and TLS. On failure it releases every
// replication resource created so far; ownership of base stays with the caller.
// The caller must validate or upgrade persisted WALs before opening the primary
// store. config.DataDir selects the replication directory, whose WAL is in wal/.
func StartRuntime(ctx context.Context, base storage.Engine, config *Config) (StartedRuntime, error) {
	adapter, err := NewStorageAdapterWithWAL(base, filepath.Join(config.DataDir, "wal"))
	if err != nil {
		return StartedRuntime{}, localizedError(localization.NornicDBCoreReplicationStorageAdapterCreateFailed(err), err)
	}

	// Every failed-start branch releases what it created, in reverse order, and
	// returns those cleanup outcomes with the start failure to its caller.
	replicator, err := NewReplicator(config, adapter)
	if err != nil {
		return StartedRuntime{}, compose.Outcomes(localizedError(localization.NornicDBCoreReplicationReplicatorCreateFailed(err), err), adapter.Close())
	}

	// The transport is built from the whole loaded config, so the
	// replication secret and TLS settings apply. If they can't be applied
	// (an unreadable certificate, say), replication doesn't start rather
	// than run plaintext or unsigned.
	transport, err := NewDefaultTransportFromConfig(config)
	if err != nil {
		return StartedRuntime{}, compose.Outcomes(localizedError(localization.NornicDBCoreReplicationStartFailed(err), err), replicator.Shutdown(), adapter.Close())
	}

	// Wire transport into modes that support it.
	if r, ok := replicator.(interface{ SetTransport(Transport) }); ok {
		r.SetTransport(transport)
	}
	// If we're using the default ClusterTransport, register message handlers for the replicator.
	if ct, ok := transport.(*ClusterTransport); ok {
		RegisterClusterHandlers(ct, replicator)
	}

	if err := replicator.Start(ctx); err != nil {
		return StartedRuntime{}, compose.Outcomes(localizedError(localization.NornicDBCoreReplicationStartFailed(err), err), transport.Close(), replicator.Shutdown(), adapter.Close())
	}

	return StartedRuntime{Engine: NewReplicatedEngine(base, replicator, 30*time.Second), Replicator: replicator, Adapter: adapter, Transport: transport}, nil
}
