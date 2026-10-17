package antlr

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// boundaryWork is a statement owner whose deadline passes after a given
// number of boundary checks.
type boundaryWork struct {
	ctx    context.Context
	cancel context.CancelFunc
	after  int
	checks int
}

func (work *boundaryWork) Err() error { return work.ctx.Err() }
func (work *boundaryWork) CheckBoundary() bool {
	work.checks++
	if work.checks > work.after {
		work.cancel()
	}
	return work.ctx.Err() == nil
}

func newBoundaryWork(after int) *boundaryWork {
	ctx, cancel := context.WithCancel(context.Background())
	return &boundaryWork{ctx: ctx, cancel: cancel, after: after}
}

func TestValidateWithCancellationHealthyMatchesValidate(t *testing.T) {
	for _, query := range []string{"RETURN 1", "MATCH (n) RETURN n", "RETURN (", ""} {
		want := Validate(query)
		got := ValidateWithCancellation(query, newBoundaryWork(1<<30))
		if want == nil {
			require.NoError(t, got, query)
		} else {
			require.EqualError(t, got, want.Error(), query)
		}
	}
}

func TestValidateWithCancellationChecksEveryBoundary(t *testing.T) {
	// A valid statement: before and after the parse. A statement the SLL
	// pass rejects: before, between the passes, and after.
	for _, test := range []struct {
		query      string
		boundaries int
	}{{"RETURN 1", 2}, {"RETURN (", 3}} {
		for after := 0; after < test.boundaries; after++ {
			work := newBoundaryWork(after)
			err := ValidateWithCancellation(test.query, work)
			require.ErrorIs(t, err, context.Canceled, "%q cancelled at boundary %d", test.query, after)
			require.Equal(t, after+1, work.checks, "%q stopped at the first cancelled boundary", test.query)
		}
		work := newBoundaryWork(test.boundaries)
		_ = ValidateWithCancellation(test.query, work)
		require.Equal(t, test.boundaries, work.checks, "%q boundaries", test.query)
	}
}

func TestValidateWithCancellationNeverReportsSyntaxAfterDeadline(t *testing.T) {
	// A cancelled statement returns its cancellation, not the verdict of a
	// parse that finished after the deadline.
	work := newBoundaryWork(1)
	err := ValidateWithCancellation("RETURN "+strings.Repeat("(", 64), work)
	require.ErrorIs(t, err, context.Canceled)
}
