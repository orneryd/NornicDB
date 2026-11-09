package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Shutdown matrix S2, HTTP Server: concurrent Stop callers all join the one
// stop and receive its outcome; a repeat Stop and WaitStopped report the same.
func TestOwnerMatrixServerConcurrentStop(t *testing.T) {
	s, _ := setupTestServer(t)
	const callers = 4
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- s.Stop(context.Background()) }()
	}
	var first error
	for i := 0; i < callers; i++ {
		select {
		case err := <-results:
			if i == 0 {
				first = err
			} else {
				require.Equal(t, fmt.Sprint(first), fmt.Sprint(err), "Stop caller %d got a different outcome", i)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("concurrent Stop did not complete")
		}
	}
	require.Equal(t, fmt.Sprint(first), fmt.Sprint(s.Stop(context.Background())), "repeat Stop must join the same stop")
	require.Equal(t, fmt.Sprint(first), fmt.Sprint(s.WaitStopped(context.Background())))
	select {
	case <-s.shutdown.work.Stopped():
	default:
		t.Fatal("Stop returned before the server's retirement completed")
	}
}
