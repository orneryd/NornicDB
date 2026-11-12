package storage

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

var errRemoteNativeTransportFailure = errors.New("remote native transport failure")

// Native entries own one bounded lifetime and retire it on return.
// These transport controls assert decoded results, access mode, caller deadlines,
// cancellation and cleanup. The existing compiled census reviews caller ownership.

// nativeContextEntry is one Engine entry and the reply its remote statement
// returns. want is what the entry must decode from that reply.
type nativeContextEntry struct {
	name    string
	mode    RemoteAccessMode
	columns []string
	rows    [][]interface{}
	want    interface{}
	run     func(*RemoteEngine) (interface{}, error)
}

func remoteNativeContextEntries() []nativeContextEntry {
	node := map[string]interface{}{"id": "node:one", "labels": []interface{}{"Person"}, "properties": map[string]interface{}{"id": "node:one"}}
	edge := map[string]interface{}{"id": "edge:one", "type": "LINK", "startNodeElementId": "node:a", "endNodeElementId": "node:b", "properties": map[string]interface{}{"id": "edge:one"}}
	scalar := [][]interface{}{{int64(2)}}
	return []nativeContextEntry{
		{name: "GetNode", mode: RemoteAccessRead, columns: []string{"n"}, rows: [][]interface{}{{node}}, want: NodeID("node:one"), run: func(engine *RemoteEngine) (interface{}, error) {
			value, err := engine.GetNode(context.Background(), "node:one")
			if err != nil {
				return nil, err
			}
			return value.ID, nil
		}},
		{name: "GetEdge", mode: RemoteAccessRead, columns: []string{"r"}, rows: [][]interface{}{{edge}}, want: EdgeID("edge:one"), run: func(engine *RemoteEngine) (interface{}, error) {
			value, err := engine.GetEdge(context.Background(), "edge:one")
			if err != nil {
				return nil, err
			}
			return value.ID, nil
		}},
		{name: "GetFirstNodeByLabel", mode: RemoteAccessRead, columns: []string{"n"}, rows: [][]interface{}{{node}}, want: NodeID("node:one"), run: func(engine *RemoteEngine) (interface{}, error) {
			value, err := engine.GetFirstNodeByLabel(context.Background(), "Person")
			if err != nil {
				return nil, err
			}
			return value.ID, nil
		}},
		{name: "GetEdgeBetween", mode: RemoteAccessRead, columns: []string{"r"}, rows: [][]interface{}{{edge}}, want: EdgeID("edge:one"), run: func(engine *RemoteEngine) (interface{}, error) {
			value, err := engine.GetEdgeBetween(context.Background(), "node:a", "node:b", "LINK")
			if err != nil {
				return nil, err
			}
			if value == nil {
				return nil, nil
			}
			return value.ID, nil
		}},
		{name: "GetEdgesBetween", mode: RemoteAccessRead, columns: []string{"r"}, rows: [][]interface{}{{edge}}, want: int64(1), run: func(engine *RemoteEngine) (interface{}, error) {
			values, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node:a", Other: "node:b"})
			if err != nil {
				return nil, err
			}
			return int64(len(values)), nil
		}},
		{name: "GetInDegree", mode: RemoteAccessRead, columns: []string{"count(r)"}, rows: scalar, want: 2, run: func(engine *RemoteEngine) (interface{}, error) {
			return engine.GetInDegree(context.Background(), "node:one")
		}},
		{name: "GetOutDegree", mode: RemoteAccessRead, columns: []string{"count(r)"}, rows: scalar, want: 2, run: func(engine *RemoteEngine) (interface{}, error) {
			return engine.GetOutDegree(context.Background(), "node:one")
		}},
		{name: "NodeCount", mode: RemoteAccessRead, columns: []string{"count(n)"}, rows: scalar, want: int64(2), run: func(engine *RemoteEngine) (interface{}, error) {
			return engine.NodeCount(context.Background())
		}},
		{name: "EdgeCount", mode: RemoteAccessRead, columns: []string{"count(r)"}, rows: scalar, want: int64(2), run: func(engine *RemoteEngine) (interface{}, error) {
			return engine.EdgeCount(context.Background())
		}},
		{name: "EdgeCountByType", mode: RemoteAccessRead, columns: []string{"count(r)"}, rows: scalar, want: int64(2), run: func(engine *RemoteEngine) (interface{}, error) {
			return engine.EdgeCountByType(context.Background(), "LINK")
		}},
		{name: "EdgeCountByStartLabel", mode: RemoteAccessRead, columns: []string{"count(r)"}, rows: scalar, want: int64(2), run: func(engine *RemoteEngine) (interface{}, error) {
			return engine.EdgeCountByStartLabel(context.Background(), "Person", "LINK")
		}},
		{name: "EdgeCountByEndLabel", mode: RemoteAccessRead, columns: []string{"count(r)"}, rows: scalar, want: int64(2), run: func(engine *RemoteEngine) (interface{}, error) {
			return engine.EdgeCountByEndLabel(context.Background(), "Person", "LINK")
		}},
		{name: "UpdateNode", mode: RemoteAccessWrite, columns: []string{"count(n)"}, rows: [][]interface{}{{int64(1)}}, want: nil, run: func(engine *RemoteEngine) (interface{}, error) {
			return nil, engine.UpdateNode(context.Background(), NodeUpdateRequest{After: &Node{ID: "node:one"}})
		}},
		{name: "UpdateEdge", mode: RemoteAccessWrite, columns: []string{"count(r)"}, rows: [][]interface{}{{int64(1)}}, want: nil, run: func(engine *RemoteEngine) (interface{}, error) {
			return nil, engine.UpdateEdge(context.Background(), EdgeUpdateRequest{After: &Edge{ID: "edge:one"}})
		}},
		{name: "DeleteNode", mode: RemoteAccessWrite, columns: nil, rows: nil, want: nil, run: func(engine *RemoteEngine) (interface{}, error) {
			return nil, engine.DeleteNode(context.Background(), "node:one")
		}},
		{name: "DeleteEdge", mode: RemoteAccessWrite, columns: nil, rows: nil, want: nil, run: func(engine *RemoteEngine) (interface{}, error) {
			return nil, engine.DeleteEdge(context.Background(), "edge:one")
		}},
	}
}

// requireOwnedContextRecorded asserts a context the transport observed is the
// entry's own bounded, non-nil context, using the liveness sampled at that
// moment so the post-return check cannot observe the entry's own release.
func requireOwnedContextRecorded(t *testing.T, ctx context.Context, liveErr error) {
	t.Helper()
	require.NotNil(t, ctx, "the transport must receive a non-nil context")
	deadline, bounded := ctx.Deadline()
	require.True(t, bounded, "the transport must receive a bounded context")
	require.WithinDuration(t, time.Now().Add(RemoteRequestTimeout), deadline, 5*time.Second,
		"the entry must own exactly one RemoteRequestTimeout deadline")
	require.NoError(t, liveErr, "the owned context must still be live at the transport")
}

// requireOwnedContext asserts the same contract for a context observed while the
// call is still running.
func requireOwnedContext(t *testing.T, ctx context.Context) {
	t.Helper()
	requireOwnedContextRecorded(t, ctx, ctx.Err())
}

func TestRemoteNativeEntryContextLifetime(t *testing.T) {
	for _, entry := range remoteNativeContextEntries() {
		t.Run(entry.name, func(t *testing.T) {
			var calls int
			var modes []RemoteAccessMode
			var observed context.Context
			engine := &RemoteEngine{transport: &fakeRemoteTransport{queryModeFn: func(ctx context.Context, _ string, _ map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error) {
				calls++
				modes = append(modes, mode)
				observed = ctx
				requireOwnedContext(t, ctx)
				return entry.columns, entry.rows, nil
			}}}
			got, err := entry.run(engine)
			require.NoError(t, err)
			require.Equal(t, entry.want, got)
			require.Equal(t, 1, calls, "a native entry issues exactly one statement")
			require.Equal(t, []RemoteAccessMode{entry.mode}, modes, "the native entry keeps its access mode")
			require.ErrorIs(t, observed.Err(), context.Canceled,
				"the owning entry releases its context when it returns")
		})
	}
}

func TestRemoteNativeEntryTransportErrorIsUnchanged(t *testing.T) {
	transportErr := errRemoteNativeTransportFailure
	for _, entry := range remoteNativeContextEntries() {
		t.Run(entry.name, func(t *testing.T) {
			engine := &RemoteEngine{transport: &fakeRemoteTransport{queryModeFn: func(ctx context.Context, _ string, _ map[string]interface{}, _ RemoteAccessMode) ([]string, [][]interface{}, error) {
				requireOwnedContext(t, ctx)
				return nil, nil, transportErr
			}}}
			_, err := entry.run(engine)
			if entry.name == "GetInDegree" || entry.name == "GetOutDegree" || entry.name == "GetEdgeBetween" {
				// These Engine signatures cannot report an error; they keep
				// returning their zero result.
				require.NoError(t, err)
				return
			}
			require.Same(t, transportErr, err, "the owned context must not change error propagation")
		})
	}
}

func TestRemoteNativeEntryCallerValuesSurviveDeadline(t *testing.T) {
	type callerValueKey struct{}
	key := callerValueKey{}
	const value = "remote-call-correlation"
	for _, operation := range []struct {
		name string
		mode RemoteAccessMode
		run  func(*RemoteEngine, context.Context) error
	}{
		{"count", RemoteAccessRead, func(engine *RemoteEngine, ctx context.Context) error {
			_, err := engine.NodeCount(ctx)
			return err
		}},
		{"mutation", RemoteAccessWrite, func(engine *RemoteEngine, ctx context.Context) error {
			return engine.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "node:one"}})
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			engine := &RemoteEngine{transport: &fakeRemoteTransport{queryModeFn: func(ctx context.Context, _ string, _ map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error) {
				require.Equal(t, value, ctx.Value(key), "the caller's context value must survive the owned deadline")
				require.Equal(t, operation.mode, mode)
				requireOwnedContext(t, ctx)
				return nil, [][]interface{}{{int64(1)}}, nil
			}}}
			ctx := context.WithValue(context.Background(), key, value)
			require.NoError(t, operation.run(engine, ctx))
		})
	}
}

func TestRemoteNativeEntryCallerCancellationIsNotSuppressed(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(*RemoteEngine, context.Context) error
	}{
		{"typed edge lookup", func(engine *RemoteEngine, ctx context.Context) error {
			_, err := engine.GetEdgeBetween(ctx, "node:a", "node:b", "LINK")
			return err
		}},
		{"in degree", func(engine *RemoteEngine, ctx context.Context) error {
			_, err := engine.GetInDegree(ctx, "node:one")
			return err
		}},
		{"out degree", func(engine *RemoteEngine, ctx context.Context) error {
			_, err := engine.GetOutDegree(ctx, "node:one")
			return err
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			var calls int
			engine := &RemoteEngine{transport: &fakeRemoteTransport{queryModeFn: func(context.Context, string, map[string]interface{}, RemoteAccessMode) ([]string, [][]interface{}, error) {
				calls++
				return nil, nil, nil
			}}}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := operation.run(engine, ctx)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, calls, "a cancelled caller must not reach the transport")
		})
	}
}

func TestRemoteHTTPNativeEntryContextLifetime(t *testing.T) {
	for _, entry := range remoteNativeContextEntries() {
		t.Run(entry.name, func(t *testing.T) {
			var requests int
			var observed context.Context
			engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
				requests++
				var request remoteTxRequest
				require.NoError(t, json.NewDecoder(req.Body).Decode(&request))
				require.Len(t, request.Statements, 1)
				statement := request.Statements[0]
				observed = req.Context()
				requireOwnedContext(t, observed)
				return remoteEngineMethodReplyHTTPServed(statement.Statement, statement.Parameters), nil
			})
			t.Cleanup(func() { _ = engine.Close() })
			_, err := entry.run(engine)
			require.NoError(t, err)
			require.Equal(t, 1, requests, "a native entry issues exactly one HTTP request")
			require.ErrorIs(t, observed.Err(), context.Canceled,
				"the owning entry releases its context when it returns")
		})
	}
}

func TestRemoteHTTPCallerContextSurvivesOneDerivation(t *testing.T) {
	t.Run("earlier deadline retained", func(t *testing.T) {
		callerDeadline := time.Now().Add(50 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
		defer cancel()
		var observed context.Context
		engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
			observed = req.Context()
			return remoteEngineMethodReplyHTTPServed("MATCH (a)-[r]->(b) WHERE a.id = $start AND b.id = $end RETURN r", map[string]interface{}{"start": "node:a", "end": "node:b"}), nil
		})
		t.Cleanup(func() { _ = engine.Close() })
		edges, err := engine.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node:a", Other: "node:b"})
		require.NoError(t, err)
		require.Len(t, edges, 1)
		deadline, bounded := observed.Deadline()
		require.True(t, bounded)
		require.WithinDuration(t, callerDeadline, deadline, time.Millisecond,
			"the caller's earlier deadline must survive the single read derivation")
	})

	t.Run("caller cancellation ends the read before the transport", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var requests int
		engine := makeEngineWithTransport(t, "http://remote.example", func(*http.Request) (*http.Response, error) {
			requests++
			return remoteEngineMethodReplyHTTPServed("MATCH ()-[r]->() RETURN r", nil), nil
		})
		t.Cleanup(func() { _ = engine.Close() })
		_, err := engine.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node:a", Other: "node:b"})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, requests, "a cancelled caller must not reach the transport")
	})

	t.Run("expired caller deadline ends the read before the transport", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
		defer cancel()
		var requests int
		engine := makeEngineWithTransport(t, "http://remote.example", func(*http.Request) (*http.Response, error) {
			requests++
			return remoteEngineMethodReplyHTTPServed("MATCH ()-[r]->() RETURN r", nil), nil
		})
		t.Cleanup(func() { _ = engine.Close() })
		_, err := engine.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node:a", Other: "node:b"})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, requests, "an expired caller deadline must not reach the transport")
	})

	t.Run("caller cancellation reaches the transport while the read runs", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
			cancel()
			select {
			case <-req.Context().Done():
			default:
				t.Error("the caller's cancellation must reach the transport through the one derived context")
			}
			return nil, context.Canceled
		})
		t.Cleanup(func() { _ = engine.Close() })
		_, err := engine.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node:a", Other: "node:b"})
		require.ErrorIs(t, err, context.Canceled)
	})
}

// nativeLifetimeBoltDriver and nativeLifetimeBoltSession record the context the
// Bolt transport hands to the driver, which is the context the canonical helper
// received.
type nativeLifetimeBoltDriver struct {
	neo4j.DriverWithContext
	sessions int
	session  *nativeLifetimeBoltSession
	liveErr  error
}

func (d *nativeLifetimeBoltDriver) NewSession(ctx context.Context, config neo4j.SessionConfig) neo4j.SessionWithContext {
	d.sessions++
	d.session.mode = config.AccessMode
	d.session.sessionCtx = ctx
	d.liveErr = ctx.Err()
	return d.session
}

type nativeLifetimeBoltSession struct {
	neo4j.SessionWithContext
	mode       neo4j.AccessMode
	sessionCtx context.Context
	runCtx     []context.Context
	runErr     error
	columns    []string
	rows       [][]interface{}
	closed     bool
}

func (s *nativeLifetimeBoltSession) ExecuteRead(_ context.Context, work neo4j.ManagedTransactionWork, _ ...func(*neo4j.TransactionConfig)) (interface{}, error) {
	return work(&nativeLifetimeBoltTx{session: s})
}

func (s *nativeLifetimeBoltSession) ExecuteWrite(_ context.Context, work neo4j.ManagedTransactionWork, _ ...func(*neo4j.TransactionConfig)) (interface{}, error) {
	return work(&nativeLifetimeBoltTx{session: s})
}

func (s *nativeLifetimeBoltSession) Close(context.Context) error { s.closed = true; return nil }

type nativeLifetimeBoltTx struct {
	neo4j.ManagedTransaction
	session *nativeLifetimeBoltSession
}

func (tx *nativeLifetimeBoltTx) Run(ctx context.Context, _ string, _ map[string]any) (neo4j.ResultWithContext, error) {
	tx.session.runCtx = append(tx.session.runCtx, ctx)
	tx.session.runErr = ctx.Err()
	return &nativeLifetimeBoltResult{columns: tx.session.columns, rows: tx.session.rows}, nil
}

type nativeLifetimeBoltResult struct {
	neo4j.ResultWithContext
	columns []string
	rows    [][]interface{}
	index   int
}

func (result *nativeLifetimeBoltResult) Keys() ([]string, error) { return result.columns, nil }

func (result *nativeLifetimeBoltResult) Next(context.Context) bool {
	if result.index >= len(result.rows) {
		return false
	}
	result.index++
	return true
}

func (result *nativeLifetimeBoltResult) Record() *neo4j.Record {
	return &neo4j.Record{Keys: result.columns, Values: result.rows[result.index-1]}
}

func (result *nativeLifetimeBoltResult) Err() error { return nil }

func newNativeLifetimeBoltEngine(entry nativeContextEntry) (*RemoteEngine, *nativeLifetimeBoltDriver) {
	driver := &nativeLifetimeBoltDriver{session: &nativeLifetimeBoltSession{columns: entry.columns, rows: entry.rows}}
	return &RemoteEngine{transport: &boltTransport{driver: driver, database: "remote"}}, driver
}

func TestRemoteBoltNativeEntryContextLifetime(t *testing.T) {
	for _, entry := range remoteNativeContextEntries() {
		t.Run(entry.name, func(t *testing.T) {
			engine, driver := newNativeLifetimeBoltEngine(entry)
			_, err := entry.run(engine)
			require.NoError(t, err)
			require.Equal(t, 1, driver.sessions, "a native entry opens exactly one Bolt session")
			require.Equal(t, map[RemoteAccessMode]neo4j.AccessMode{
				RemoteAccessRead:  neo4j.AccessModeRead,
				RemoteAccessWrite: neo4j.AccessModeWrite,
			}[entry.mode], driver.session.mode, "the native entry keeps its declared access mode")
			requireOwnedContextRecorded(t, driver.session.sessionCtx, driver.liveErr)
			require.Len(t, driver.session.runCtx, 1, "a native entry runs exactly one statement")
			requireOwnedContextRecorded(t, driver.session.runCtx[0], driver.session.runErr)
			require.Same(t, driver.session.sessionCtx, driver.session.runCtx[0],
				"the session and its statement share the one owned context")
			require.True(t, driver.session.closed, "the Bolt session is released when the entry returns")
			require.ErrorIs(t, driver.session.sessionCtx.Err(), context.Canceled,
				"the owning entry releases its context when it returns")
		})
	}
}

func TestRemoteBoltCallerContextSurvivesOneDerivation(t *testing.T) {
	t.Run("cancelled caller", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		engine, driver := newNativeLifetimeBoltEngine(remoteNativeContextEntries()[0])
		_, err := engine.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node:a", Other: "node:b"})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, driver.sessions, "a cancelled caller must not open a Bolt session")
	})

	t.Run("expired caller deadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
		defer cancel()
		engine, driver := newNativeLifetimeBoltEngine(remoteNativeContextEntries()[0])
		_, err := engine.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: "node:a", Other: "node:b"})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, driver.sessions, "an expired caller deadline must not open a Bolt session")
	})
}

// BenchmarkRemoteNativeEntryContextOwnership records the allocation cost of the
// native entries: each call derives exactly one bounded context, so the
// before/after comparison of this benchmark shows whether the ownership
// migration added a second timeout allocation.
func BenchmarkRemoteNativeEntryContextOwnership(b *testing.B) {
	for _, entry := range remoteNativeContextEntries() {
		engine := &RemoteEngine{transport: &fakeRemoteTransport{queryModeFn: func(context.Context, string, map[string]interface{}, RemoteAccessMode) ([]string, [][]interface{}, error) {
			return entry.columns, entry.rows, nil
		}}}
		b.Run(entry.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := entry.run(engine); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
