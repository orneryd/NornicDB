package testfixture

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCurrentGoroutineIDSeparatesConcurrentCallers(t *testing.T) {
	id := CurrentGoroutineID()
	require.Positive(t, id)
	require.Equal(t, id, CurrentGoroutineID())
	other := make(chan uint64, 1)
	go func() { other <- CurrentGoroutineID() }()
	observed := <-other
	require.Positive(t, observed)
	require.NotEqual(t, id, observed)
}
