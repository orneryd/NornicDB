package resultstream

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStreamDeliveryWrittenReferences(t *testing.T) {
	for _, kind := range []string{"whole", "split", "partial", "omitted", "mixed"} {
		t.Run(kind, func(t *testing.T) {
			r, err := NewRegistry(Config{MaxStreams: 2})
			require.NoError(t, err)
			t.Cleanup(r.Close)
			delivery := &Delivery{}
			scope := Scope{Owner: "alice", Database: "one"}
			a, b := &lifecycleStream{}, &lifecycleStream{}
			first, err := r.Start(WithDelivery(context.Background(), delivery), scope, a, 1)
			require.NoError(t, err)
			if kind == "mixed" {
				_, err = r.Start(WithDelivery(context.Background(), delivery), scope, b, 1)
				require.NoError(t, err)
			}
			switch kind {
			case "whole", "mixed":
				require.True(t, delivery.ObserveWritten([]byte(first.QID)))
			case "split":
				midpoint := len(first.QID) / 2
				require.False(t, delivery.ObserveWritten([]byte(first.QID[:midpoint])))
				require.True(t, delivery.ObserveWritten([]byte(first.QID[midpoint:])))
			case "partial":
				require.False(t, delivery.ObserveWritten([]byte(first.QID[:len(first.QID)-1])))
			case "omitted":
				require.False(t, delivery.ObserveWritten([]byte(`{"rowCount":1}`)))
			}
			require.EqualValues(t, 0, a.closes.Load(), "writes alone do not acknowledge a complete response")
			delivery.DeliveredWritten()
			delivery.Fail()
			if kind == "whole" || kind == "split" || kind == "mixed" {
				require.EqualValues(t, 1, r.count.Load())
				require.Zero(t, a.closes.Load())
				require.NoError(t, r.Discard(scope, first.QID))
			} else {
				require.Zero(t, r.count.Load())
				require.EqualValues(t, 1, a.closes.Load())
			}
			if kind == "mixed" {
				require.EqualValues(t, 1, b.closes.Load(), "unexposed sibling pages are retired")
			}
		})
	}
}
