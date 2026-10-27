package storage

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
	"testing"
)

type f18Driver struct {
	neo4j.DriverWithContext
	mode    neo4j.AccessMode
	session *f18Session
}

func (d *f18Driver) NewSession(_ context.Context, c neo4j.SessionConfig) neo4j.SessionWithContext {
	d.mode = c.AccessMode
	return d.session
}

type f18Session struct {
	neo4j.SessionWithContext
	write     bool
	closed    bool
	statement string
	params    map[string]any
}

func (s *f18Session) ExecuteRead(ctx context.Context, w neo4j.ManagedTransactionWork, _ ...func(*neo4j.TransactionConfig)) (any, error) {
	s.write = false
	return w(&f18Tx{s: s})
}
func (s *f18Session) ExecuteWrite(ctx context.Context, w neo4j.ManagedTransactionWork, _ ...func(*neo4j.TransactionConfig)) (any, error) {
	s.write = true
	return w(&f18Tx{s: s})
}
func (s *f18Session) Close(context.Context) error { s.closed = true; return nil }

type f18Tx struct {
	neo4j.ManagedTransaction
	s *f18Session
}

func (t *f18Tx) Run(_ context.Context, q string, p map[string]any) (neo4j.ResultWithContext, error) {
	t.s.statement = q
	t.s.params = p
	return &f18Result{}, nil
}

type f18Result struct{ neo4j.ResultWithContext }

func (*f18Result) Keys() ([]string, error)   { return []string{"marker"}, nil }
func (*f18Result) Next(context.Context) bool { return false }
func (*f18Result) Err() error                { return nil }
func TestRemoteBoltExplicitAccessMode(t *testing.T) {
	for _, c := range []struct {
		q     string
		write bool
	}{{"MATCH (n) SET\tn.x=1 RETURN n", true}, {"MATCH (n) SET/*gap*/n.x=1 RETURN n", true}, {"MATCH (n) REMOVE\tn.x RETURN n", true}, {"MATCH (n) REMOVE/*gap*/n.x RETURN n", true}, {"RETURN 'CREATE' AS value", false}, {"RETURN 'SET x=1' AS value", false}, {"MATCH(n) RETURN n", false}, {"CREATE (n) RETURN n", true},
		// Routing is caller-owned; execution validity remains with the remote.
		{"RETURN 1", true}, {"CREATE (n)", false}, {"CALL remote.unknown()", true}, {"CALL db.labels()", true}, {"FOREIGN DIALECT STATEMENT", true}, {"EXPLAIN CREATE (n)", false}, {"PROFILE CREATE (n)", true}, {"RETURN {CREATE: 1} AS value", false}, {"WITH 1 AS `CREATE` RETURN `CREATE`", false}, {"CALL { MATCH (n) SET n.x=1 RETURN n } RETURN n", true}, {"CREATE INDEX example FOR (n:Example) ON (n.id)", true}, {"SHOW DATABASES", false}} {
		t.Run(c.q, func(t *testing.T) {
			s := &f18Session{}
			d := &f18Driver{session: s}
			engine := &RemoteEngine{transport: &boltTransport{driver: d, database: "selected"}}
			params := map[string]any{"ordinary": int64(17)}
			columns, rows, err := engine.QueryCypher(t.Context(), c.q, params, map[bool]RemoteAccessMode{true: RemoteAccessWrite, false: RemoteAccessRead}[c.write])
			require.NoError(t, err)
			require.Equal(t, []string{"marker"}, columns)
			require.Empty(t, rows)
			require.Equal(t, c.q, s.statement)
			require.Equal(t, params, s.params)
			require.True(t, s.closed)
			require.Equal(t, c.write, s.write, "actual managed transaction policy")
			require.Equal(t, map[bool]neo4j.AccessMode{true: neo4j.AccessModeWrite, false: neo4j.AccessModeRead}[c.write], d.mode, "actual driver session policy")
		})
	}
}

func TestRemoteBoltCanceledExplicitQueryDoesNotForward(t *testing.T) {
	s := &f18Session{}
	d := &f18Driver{session: s}
	engine := &RemoteEngine{transport: &boltTransport{driver: d}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := engine.QueryCypher(ctx, "RETURN 1", nil, RemoteAccessRead)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, s.statement)
	require.False(t, s.closed)
}

func TestRemoteBoltRejectsUnspecifiedAccessMode(t *testing.T) {
	for _, mode := range []RemoteAccessMode{0, 3, 255} {
		s := &f18Session{}
		d := &f18Driver{session: s}
		engine := &RemoteEngine{transport: &boltTransport{driver: d}}
		_, _, err := engine.QueryCypher(t.Context(), "RETURN 1", nil, mode)
		require.Error(t, err)
		require.Empty(t, s.statement)
		require.False(t, s.closed)
	}
}

func TestRemoteStorageOperationsSupplyDeclaredAccessMode(t *testing.T) {
	node := map[string]interface{}{"id": "n", "properties": map[string]interface{}{"id": "n"}}
	edge := map[string]interface{}{"id": "e", "type": "LINK", "startNodeElementId": "a", "endNodeElementId": "b", "properties": map[string]interface{}{"id": "e"}}
	for _, tc := range []struct {
		name  string
		mode  RemoteAccessMode
		value interface{}
		run   func(*RemoteEngine) error
	}{
		{"create node", RemoteAccessWrite, node, func(e *RemoteEngine) error { _, err := e.CreateNode(&Node{ID: "n"}); return err }},
		{"update node", RemoteAccessWrite, int64(1), func(e *RemoteEngine) error { return e.UpdateNode(&Node{ID: "n"}) }},
		{"delete node", RemoteAccessWrite, int64(1), func(e *RemoteEngine) error { return e.DeleteNode("n") }},
		{"create edge", RemoteAccessWrite, edge, func(e *RemoteEngine) error {
			return e.CreateEdge(&Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"})
		}},
		{"update edge", RemoteAccessWrite, int64(1), func(e *RemoteEngine) error { return e.UpdateEdge(&Edge{ID: "e"}) }},
		{"delete edge", RemoteAccessWrite, int64(1), func(e *RemoteEngine) error { return e.DeleteEdge("e") }},
		{"get node", RemoteAccessRead, node, func(e *RemoteEngine) error { _, err := e.GetNode("n"); return err }},
		{"get edge", RemoteAccessRead, edge, func(e *RemoteEngine) error { _, err := e.GetEdge("e"); return err }},
		{"node count", RemoteAccessRead, int64(1), func(e *RemoteEngine) error { _, err := e.NodeCount(); return err }},
		{"edge count", RemoteAccessRead, int64(1), func(e *RemoteEngine) error { _, err := e.EdgeCount(); return err }},
		{"positional count", RemoteAccessRead, int64(1), func(e *RemoteEngine) error { _, err := e.EdgeCountByStartLabel("L", "LINK"); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			engine := &RemoteEngine{transport: &fakeRemoteTransport{queryModeFn: func(ctx context.Context, source string, params map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error) {
				calls++
				require.Equal(t, tc.mode, mode)
				require.NotEmpty(t, source)
				return []string{"value"}, [][]interface{}{{tc.value}}, nil
			}}}
			require.NoError(t, tc.run(engine))
			require.Equal(t, 1, calls)
		})
	}
}
