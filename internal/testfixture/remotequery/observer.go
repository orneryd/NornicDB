// Package remotequery provides shared test instrumentation for remote query
// engines. It is a test fixture package only and is not part of any public
// product API.
package remotequery

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Observer wraps a *storage.RemoteEngine and records the most recent
// QueryCypher access mode and source, plus the total number of QueryCypher
// calls, before delegating to the embedded engine. It is intended solely for
// test observation and must not be used in production paths.
type Observer struct {
	*storage.RemoteEngine
	Mode   storage.RemoteAccessMode
	Source string
	Calls  int
}

// QueryCypher records the access mode and source, increments the call count,
// and delegates to the embedded concrete *storage.RemoteEngine.
func (o *Observer) QueryCypher(ctx context.Context, source string, params map[string]interface{}, mode storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
	o.Mode, o.Source = mode, source
	o.Calls++
	return o.RemoteEngine.QueryCypher(ctx, source, params, mode)
}

// EngineCapabilityPolicy classifies this test-only query observer explicitly.
// It retains the remote read/membership view and owns QueryCypher observation.
// Author: Lane #1644 (Codex); the remote transport remains the maintainer's owner.
func (o *Observer) EngineCapabilityPolicy() storage.EngineBoundaryPolicy {
	policy := o.RemoteEngine.EngineCapabilityPolicy()
	policy.Kind = storage.EngineDecorator
	return policy
}
func (*Observer) StoredReadPolicy() storage.StoredReadPolicy {
	return storage.RefusedStoredReadPolicy()
}
