package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltApplicationMemoryReclaimsCompletedWork(t *testing.T) {
	store := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = store.Close() })
	executor := &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store)}
	_, err := executor.Execute(context.Background(), "RETURN 1 AS ok", nil)
	require.NoError(t, err, "warm the selected parser before measuring the idle baseline")
	server := New(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 8192, WriteBufferSize: 8192}, executor)
	policy := memorypressure.New(t)
	require.NoError(t, policy.Refresh())
	work, err := policy.Reserve(64 << 10)
	require.NoError(t, err)
	memorypressure.Garbage()
	work.Release()
	require.NoError(t, policy.Refresh())
	server.SetMemoryPolicy(policy)
	port := serveBoltIntegrationServer(t, server)
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	query := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		defer session.Close(ctx)
		result, err := session.Run(ctx, "RETURN 1 AS ok", nil)
		if err != nil {
			return err
		}
		records, err := result.Collect(ctx)
		if err == nil && (len(records) != 1 || records[0].Values[0] != int64(1)) {
			return fmt.Errorf("unexpected records: %v", records)
		}
		return err
	}
	err = query()
	var neoErr *neo4j.Neo4jError
	require.ErrorAs(t, err, &neoErr)
	require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", neoErr.Code)
	ctx, cancel := context.WithCancel(context.Background())
	ticks := time.NewTicker(25 * time.Millisecond)
	done := make(chan struct{})
	go func() { defer close(done); policy.Run(ctx, ticks.C) }()
	t.Cleanup(func() { cancel(); ticks.Stop(); <-done })
	deadline := time.Now().Add(3 * time.Second)
	for {
		err = query()
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	require.NoError(t, err, "small Bolt queries must recover without manual GC")
	require.Zero(t, policy.Reserved())
}

func TestBoltRunRejectsMemoryPressureBeforeDecodeAndRecoversAfterReset(t *testing.T) {
	var used atomic.Int64
	used.Store(100 << 20)
	policy := memory.NewPolicy(100<<20, 0, 2*time.Second, func() (memory.Sample, error) {
		return memory.Sample{Used: used.Load(), Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	var executions atomic.Int64
	exec := &mockExecutor{executeFunc: func(context.Context, string, map[string]any) (*QueryResult, error) {
		executions.Add(1)
		return &QueryResult{Columns: []string{"x"}, Rows: [][]any{{int64(1)}}}, nil
	}}
	conn := &mockConn{}
	session := newTestSession(conn, exec)
	session.server.SetMemoryPolicy(policy)

	// Malformed PackStream must not be decoded when pressure already bars RUN.
	require.NoError(t, session.dispatchMessage(MsgRun, []byte{0xff}))
	require.Zero(t, executions.Load())
	require.True(t, session.failedUntilReset)
	require.Contains(t, string(conn.writeData), "Neo.TransientError.General.MemoryPoolOutOfMemoryError")
	require.Zero(t, policy.Reserved())

	used.Store(1 << 20)
	require.NoError(t, policy.Refresh())
	require.NoError(t, session.dispatchMessage(MsgReset, nil))
	conn.writeData = nil
	require.NoError(t, session.dispatchMessage(MsgRun, gh769RunBody(t, "RETURN 1 AS x")))
	require.EqualValues(t, 1, executions.Load())
	require.True(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgSuccess}))
	require.Positive(t, policy.Reserved())
	require.NoError(t, session.handlePull(nil))
	require.Zero(t, policy.Reserved())
}

func TestBoltResultReservationLivesUntilPullOrReset(t *testing.T) {
	policy := memory.NewPolicy(100<<20, 0, 2*time.Second, func() (memory.Sample, error) {
		return memory.Sample{Used: 1 << 20, Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	exec := &mockExecutor{executeFunc: func(context.Context, string, map[string]any) (*QueryResult, error) {
		return &QueryResult{Columns: []string{"x"}, Rows: [][]any{{"owned-row"}}}, nil
	}}
	session := newTestSession(&mockConn{}, exec)
	session.server.SetMemoryPolicy(policy)
	require.NoError(t, session.handleRun(gh769RunBody(t, "RETURN 'owned-row' AS x")))
	require.Positive(t, policy.Reserved(), "RUN result must retain its reservation until PULL")
	require.NoError(t, session.handlePull(nil))
	require.Zero(t, policy.Reserved())

	require.NoError(t, session.handleRun(gh769RunBody(t, "RETURN 'owned-row' AS x")))
	require.Positive(t, policy.Reserved())
	require.NoError(t, session.handleReset(nil))
	require.Zero(t, policy.Reserved(), "RESET must release an abandoned result")
}

func TestBoltOutOfOrderExplicitPullReleasesOnlyItsResult(t *testing.T) {
	policy := memory.NewPolicy(100<<20, 0, time.Second, func() (memory.Sample, error) {
		return memory.Sample{Used: 1 << 20, Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	first, err := policy.Reserve(100)
	require.NoError(t, err)
	second, err := policy.Reserve(200)
	require.NoError(t, err)
	session := newTestSession(&mockConn{}, &mockExecutor{})
	session.inTransaction = true
	session.latestStatementID = 1
	session.lastResult = &QueryResult{Rows: [][]any{{int64(2)}}}
	session.lastResultMemoryLease = second
	session.resultStreams = map[int64]*resultStream{
		0: {result: &QueryResult{Rows: [][]any{{int64(1)}}}},
		1: {result: session.lastResult},
	}
	session.resultMemoryLeases = map[int64]*memory.Lease{0: first, 1: second}

	require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"qid": int64(0)})))
	require.EqualValues(t, 200, policy.Reserved(), "newer statement must retain its rows")
	require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"qid": int64(1)})))
	require.Zero(t, policy.Reserved())
}
