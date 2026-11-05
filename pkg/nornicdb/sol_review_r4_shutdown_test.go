package nornicdb

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

type solR4CloseStarter struct {
	db     *DB
	cancel context.CancelFunc
}

func (s solR4CloseStarter) ResultStreamStarted() {
	fmt.Println("SOL_R4_CALLBACK_ENTERED_CLOSE")
	_ = s.db.Close()
	s.cancel()
}

func TestSolR4CloseFromQueryCallback(t *testing.T) {
	if os.Getenv("SOL_R4_CALLBACK_CHILD") == "1" {
		db, err := Open("", nil)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream := cypher.NewResultStream(1, solR4CloseStarter{db: db, cancel: cancel})
		_, _ = db.ExecuteCypher(cypher.WithResultStream(ctx, stream), "UNWIND range(1, 100) AS x RETURN x", nil)
		fmt.Println("SOL_R4_CALLBACK_RETURNED")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSolR4CloseFromQueryCallback$", "-test.timeout=8s", "-test.v")
	cmd.Env = append(os.Environ(), "SOL_R4_CALLBACK_CHILD=1")
	output, err := cmd.CombinedOutput()
	if !strings.Contains(string(output), "SOL_R4_CALLBACK_ENTERED_CLOSE") {
		t.Fatalf("callback was not exercised: %v\n%s", err, output)
	}
	if ctx.Err() != nil {
		t.Fatalf("Close called by query callback deadlocked until isolated child was killed: %v\n%s", ctx.Err(), output)
	}
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "SOL_R4_CALLBACK_RETURNED")
}

func TestSolR4ConcurrentCloseJoinsActiveProvider(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	e := &uncancellableForegroundEmbedder{
		solReviewForegroundEmbedder: solReviewForegroundEmbedder{
			mockEmbedder: mockEmbedder{dims: 4}, entered: make(chan struct{}), proceed: make(chan struct{}), closeCalled: make(chan struct{}),
		}, cancelled: make(chan struct{}),
	}
	require.True(t, db.SetEmbedder(e))
	db.GetEmbedQueue().Close()
	queried := make(chan error, 1)
	go func() { _, err := db.EmbedQuery(context.Background(), "held"); queried <- err }()
	<-e.entered
	first := make(chan error, 1)
	go func() { first <- db.Close() }()
	<-e.cancelled
	second := make(chan error, 1)
	go func() { second <- db.Close() }()
	returnedEarly := false
	select {
	case err := <-second:
		require.NoError(t, err)
		returnedEarly = true
	case <-time.After(100 * time.Millisecond):
	}
	close(e.proceed)
	require.NoError(t, <-queried)
	require.NoError(t, <-first)
	if !returnedEarly {
		require.NoError(t, <-second)
	}
	require.False(t, returnedEarly, "concurrent DB.Close returned while the provider was still active and the owning Close had not joined it")
}

func TestSolR4NormalQueryAdmissionAllocations(t *testing.T) {
	db, err := Open("", nil)
	require.NoError(t, err)
	defer db.Close()
	binding := db.GetCypherExecutor().QueryEmbedderBinding()
	run := func() {
		_, err := db.ExecuteCypher(context.Background(), "RETURN 1 AS n", nil)
		if err != nil {
			panic(err)
		}
	}
	run()
	guarded := testing.AllocsPerRun(1000, run)
	binding.SetLifetime(nil)
	baseline := testing.AllocsPerRun(1000, run)
	binding.SetLifetime(db.beginProviderUse)
	t.Logf("same-query guard enabled=%.0f allocations; guard bypassed=%.0f; admission delta=%.0f", guarded, baseline, guarded-baseline)
}
