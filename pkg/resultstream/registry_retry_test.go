package resultstream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamLifecycleRetryRequestFailure(t *testing.T) {
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, point := range []string{"before_pull", "producer"} {
			t.Run(failure.Error()+"/"+point, func(t *testing.T) {
				r, err := NewRegistry(Config{MaxStreams: 1, MaxStreamsPerOwner: 1, TTL: time.Hour})
				require.NoError(t, err)
				t.Cleanup(r.Close)
				scope := Scope{Owner: "alice", Database: "one"}
				stream := &lifecycleStream{}
				first, err := r.Start(context.Background(), scope, stream, 1)
				require.NoError(t, err)
				retained := r.retained.Load()
				ctx := context.Background()
				if point == "producer" {
					stream.err = fmt.Errorf("search step: %w", failure)
				} else if failure == context.Canceled {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				} else {
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
					defer cancel()
				}
				// Transport cleanup after an unsuccessful pull has no produced page
				// to discard; a previously delivered qid remains the client's.
				delivery := &Delivery{}
				_, err = r.Pull(WithDelivery(ctx, delivery), scope, first.QID, 1)
				require.ErrorIs(t, err, failure)
				delivery.Fail()
				require.EqualValues(t, 1, r.count.Load())
				require.Equal(t, retained, r.retained.Load())
				require.Len(t, r.owners, 1)
				require.Zero(t, stream.closes.Load())
				_, err = r.Start(context.Background(), scope, &lifecycleStream{}, 1)
				require.ErrorIs(t, err, ErrCapacity, "the retained cursor still owns its admission")
				stream.err = nil
				last, err := r.Pull(context.Background(), scope, first.QID, 1)
				require.NoError(t, err)
				require.Equal(t, [][]any{{uint64(1)}}, last.Rows)
				require.False(t, last.HasMore)
				require.Zero(t, r.count.Load())
				require.Zero(t, r.retained.Load())
				require.Empty(t, r.owners)
				require.NoError(t, r.Discard(scope, first.QID))
				r.Close()
				require.EqualValues(t, 1, stream.closes.Load())
			})
		}
	}
}
