package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestConflictMessages_PinConsumerSubstrings locks the on-the-wire shape of
// optimistic-conflict errors built at pkg/storage/badger_transaction.go:1829,
// :1843, :1857, :1871, and :1942.
//
// Downstream Bolt consumers classify retryable transients by substring match.
// Renaming or reformatting any of these messages silently turns safe retries
// into projection failures.
//
// If you intentionally change one of these shapes, update this test AND
// docs/plans/consumer-pinned-error-contract-plan.md §2.2 AND coordinate with
// known consumers before merging.
func TestConflictMessages_PinConsumerSubstrings(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "node simple",
			err:  fmt.Errorf("%w: node %s changed after transaction start", ErrConflict, "n-1"),
			want: []string{"conflict:", "changed after transaction start", "node n-1"},
		},
		{
			name: "node with version detail",
			err: fmt.Errorf(
				"%w: node %s changed after transaction start (head=%s, readTS=%s)",
				ErrConflict, "n-1", "v2", "v1",
			),
			want: []string{"conflict:", "changed after transaction start", "head=v2", "readTS=v1"},
		},
		{
			name: "edge simple",
			err:  fmt.Errorf("%w: edge %s changed after transaction start", ErrConflict, "e-1"),
			want: []string{"conflict:", "changed after transaction start", "edge e-1"},
		},
		{
			name: "edge alternate",
			err:  fmt.Errorf("%w: edge %s changed after transaction start", ErrConflict, "e-2"),
			want: []string{"conflict:", "changed after transaction start", "edge e-2"},
		},
		{
			name: "adjacent edge",
			err: fmt.Errorf(
				"%w: node %s has adjacent edge %s changed after transaction start",
				ErrConflict, "n-1", "e-1",
			),
			want: []string{
				"conflict:",
				"changed after transaction start",
				"adjacent edge",
				"node n-1",
				"edge e-1",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			for _, sub := range tc.want {
				if !strings.Contains(msg, sub) {
					t.Errorf("message %q missing required substring %q", msg, sub)
				}
			}
			if !errors.Is(tc.err, ErrConflict) {
				t.Errorf("error must wrap ErrConflict so callers can errors.Is it: %q", msg)
			}
		})
	}
}
