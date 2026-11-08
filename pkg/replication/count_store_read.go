package replication

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var _ storage.CountStoreReadEngine = (*ReplicatedEngine)(nil)

// ReadCountStore forwards a count-store read to the local engine that holds the
// counters and the label counts its visibility half reads, as the namespace
// relationship counters are forwarded. An inner engine without the capability
// leaves the read unproven rather than answering a count it cannot describe.
func (r *ReplicatedEngine) ReadCountStore(ctx context.Context, request storage.CountStoreReadRequest) (storage.CountStoreRead, error) {
	if reader, ok := r.Engine.(storage.CountStoreReadEngine); ok {
		return reader.ReadCountStore(ctx, request)
	}
	return storage.CountStoreRead{}, nil
}
