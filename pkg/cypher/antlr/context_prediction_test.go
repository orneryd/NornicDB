package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"

	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanceledValidationReleasesRetiredPrediction(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	during := &cancelDuringLookahead{Context: ctx, cancel: func() {
		retireCurrentPredictionForTest()
		cancel()
	}}
	require.ErrorIs(t, ValidateContext(during, "MATCH (n) WHERE n.x > 1 RETURN n", cyphertext.RawSource), context.Canceled)
	checkLedger(t)
	predictions.mu.Lock()
	for generation := range predictions.retired {
		if generation.readers != 0 {
			t.Errorf("cancelled validation retained %d readers", generation.readers)
		}
	}
	if predictions.current != nil && predictions.current.readers != 0 {
		t.Errorf("cancelled validation retained a current reader")
	}
	predictions.mu.Unlock()
	require.NoError(t, Validate("RETURN 1"))
	checkLedger(t)
}
