package resultstream

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStreamDeliveryBatchBoundaries(t *testing.T) {
	for _, end := range []string{"disconnect", "complete_first", "partial_records", "discard_first", "failure_before_publish", "next_batch"} {
		t.Run(end, func(t *testing.T) {
			r, err := NewRegistry(Config{MaxStreams: 2})
			require.NoError(t, err)
			t.Cleanup(r.Close)
			connection := &Delivery{}
			first, second := connection.Fork(), connection.Fork()
			if end == "partial_records" || end == "discard_first" {
				second = first
			}
			if end == "failure_before_publish" {
				connection.Fail()
			}
			scope := Scope{Owner: "alice", Database: "one"}
			a, b := &lifecycleStream{}, &lifecycleStream{}
			pa, err := r.Start(WithDelivery(context.Background(), first), scope, a, 1)
			require.NoError(t, err)
			pb, err := r.Start(WithDelivery(context.Background(), second), scope, b, 1)
			require.NoError(t, err)
			records := [][]any{{map[string]any{"page": map[string]any{"qid": pa.QID}}}}
			switch end {
			case "complete_first", "next_batch":
				first.Delivered()
			case "partial_records":
				first.DeliveredValues(records)
			case "discard_first":
				first.DiscardValues(records)
				require.EqualValues(t, 1, a.closes.Load())
			}
			connection.Fail()
			if end == "complete_first" || end == "partial_records" || end == "next_batch" {
				require.EqualValues(t, 1, r.count.Load())
				require.Zero(t, a.closes.Load())
				require.EqualValues(t, 1, b.closes.Load())
				require.NoError(t, r.Discard(scope, pa.QID))
			} else {
				require.Zero(t, r.count.Load())
				require.EqualValues(t, 1, a.closes.Load())
				require.EqualValues(t, 1, b.closes.Load())
			}
			require.NoError(t, r.Discard(scope, pb.QID))
			require.Zero(t, r.retained.Load())
			if end == "next_batch" {
				late := &lifecycleStream{}
				_, err = r.Start(WithDelivery(context.Background(), first), scope, late, 1)
				require.NoError(t, err)
				require.EqualValues(t, 1, late.closes.Load(), "disconnected parent owns late lazy publication")
				require.Zero(t, r.count.Load())
			}
		})
	}
}
