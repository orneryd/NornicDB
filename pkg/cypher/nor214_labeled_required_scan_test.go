package cypher

// NOR-214 generated unindexed labelled lookup: a labelled MATCH restricted by a
// property equality decodes only the properties the row and the equality need
// and lets the engine skip the nodes that can't match. The label stays an
// engine hint, so every residual check remains the caller's: these tests assert
// the exact rows for a string equality, an integer equality, a missing property
// and an extra required label, the scan the public path runs, that the row's
// node is the one the scan supplied, and that a failing or canceled scan fails
// the statement instead of returning the rows read so far.

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor214ScanProbe records the node-scan options a statement runs and the point
// reads it makes while delegating every read to the namespaced engine it wraps.
// A matching row costs no point read: the scan supplies its node.
type nor214ScanProbe struct {
	*storage.NamespacedEngine
	mu         sync.Mutex
	options    []storage.StreamNodesOptions
	labelScans int
	pointReads int
}

func (p *nor214ScanProbe) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(*storage.Node) error) error {
	p.mu.Lock()
	p.options = append(p.options, opts)
	p.mu.Unlock()
	return p.NamespacedEngine.StreamNodesWithOptions(ctx, opts, fn)
}

func (p *nor214ScanProbe) StreamNodesByLabelProjected(ctx context.Context, label string, properties []string, fn func(*storage.Node) error) error {
	p.mu.Lock()
	p.labelScans++
	p.mu.Unlock()
	return p.NamespacedEngine.StreamNodesByLabelProjected(ctx, label, properties, fn)
}

func (p *nor214ScanProbe) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	p.countPointRead()
	return p.NamespacedEngine.GetNode(ctx, id)
}

func (p *nor214ScanProbe) GetQueryNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	p.countPointRead()
	return p.NamespacedEngine.GetQueryNode(ctx, id)
}

func (p *nor214ScanProbe) countPointRead() {
	p.mu.Lock()
	p.pointReads++
	p.mu.Unlock()
}

func (p *nor214ScanProbe) take() ([]storage.StreamNodesOptions, int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	options, labelScans, pointReads := p.options, p.labelScans, p.pointReads
	p.options, p.labelScans, p.pointReads = nil, 0, 0
	return options, labelScans, pointReads
}

func newNOR214Executor(t *testing.T) (*StorageExecutor, *nor214ScanProbe) {
	t.Helper()
	probe := &nor214ScanProbe{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor214")}
	exec := NewStorageExecutor(probe)
	_, err := exec.Execute(context.Background(), `CREATE
		(:Reading {id: 'reading-1', source: 'repository documentation', content: 'one'}),
		(:Reading {id: 'reading-2', source: 'repository documentation', content: 'two'}),
		(:Reading:Mood {id: 'reading-9', source: 'mood documentation', content: 'nine'}),
		(:Record {id: 7, reading_id: 'reading-1'})`, nil)
	require.NoError(t, err)
	probe.take()
	return exec, probe
}

func nor214Rows(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}) [][]interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	return result.Rows
}

// TestNOR214LabelledLookupRows asserts the exact rows of a labelled equality
// lookup for each equality shape, including the extra required label that the
// engine's hint cannot enforce.
func TestNOR214LabelledLookupRows(t *testing.T) {
	exec, _ := newNOR214Executor(t)
	for _, test := range []struct {
		name   string
		query  string
		params map[string]interface{}
		rows   [][]interface{}
	}{
		{
			name:  "string equality",
			query: "MATCH (r:Reading {id: 'reading-1'}) RETURN r.source AS source",
			rows:  [][]interface{}{{"repository documentation"}},
		},
		{
			name:  "string equality on a second node",
			query: "MATCH (r:Reading {id: 'reading-2'}) RETURN r.content AS content",
			rows:  [][]interface{}{{"two"}},
		},
		{
			name:  "string equality that matches nothing",
			query: "MATCH (r:Reading {id: 'reading-404'}) RETURN count(r)",
			rows:  [][]interface{}{{int64(0)}},
		},
		{
			name:  "integer equality",
			query: "MATCH (d:Record {id: 7}) RETURN d.reading_id AS reading",
			rows:  [][]interface{}{{"reading-1"}},
		},
		{
			name:  "integer equality is not a string equality",
			query: "MATCH (d:Record {id: '7'}) RETURN count(d)",
			rows:  [][]interface{}{{int64(0)}},
		},
		{
			name:  "missing property",
			query: "MATCH (r:Reading {nope: 'reading-1'}) RETURN count(r)",
			rows:  [][]interface{}{{int64(0)}},
		},
		{
			name:  "missing extra label",
			query: "MATCH (r:Reading:Unused {id: 'reading-1'}) RETURN count(r)",
			rows:  [][]interface{}{{int64(0)}},
		},
		{
			name:  "present extra label",
			query: "MATCH (r:Reading:Mood {id: 'reading-9'}) RETURN r.content AS content",
			rows:  [][]interface{}{{"nine"}},
		},
		{
			name:  "second required label narrows the same equality",
			query: "MATCH (r:Reading:Mood {id: 'reading-1'}) RETURN count(r)",
			rows:  [][]interface{}{{int64(0)}},
		},
		{
			name:  "parameter equality",
			query: "MATCH (r:Reading {id: $id}) RETURN r.content AS content",
			params: map[string]interface{}{
				"id": "reading-2",
			},
			rows: [][]interface{}{{"two"}},
		},
		{
			name:  "WHERE equality keeps its rows",
			query: "MATCH (r:Reading) WHERE r.id = 'reading-2' RETURN count(r)",
			rows:  [][]interface{}{{int64(1)}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.rows, nor214Rows(t, exec, test.query, test.params), test.query)
		})
	}
}

// TestNOR214LabelledLookupUsesEqualityHint proves the public path routes the
// labelled equality through the StreamNodesWithOptions options owner: the scan
// carries the pattern's label as its hint, the row's properties plus the
// equality's as its projection, and the required string for stored-bytes
// comparison. The label-indexed stream is not used for the restricted scan, and
// the matching row costs no point read: the node the scan supplies is the row.
func TestNOR214LabelledLookupUsesEqualityHint(t *testing.T) {
	exec, probe := newNOR214Executor(t)
	require.Equal(t, [][]interface{}{{"repository documentation"}},
		nor214Rows(t, exec, "MATCH (r:Reading {id: 'reading-1'}) RETURN r.source AS source", nil))

	options, labelScans, pointReads := probe.take()
	require.Zero(t, labelScans, "the equality hint scan replaced the unrestricted label scan")
	require.Zero(t, pointReads, "the matching node comes from the scan, not a second read")
	var hinted []storage.StreamNodesOptions
	for _, opts := range options {
		if opts.Label == "Reading" && opts.Projection != nil {
			hinted = append(hinted, opts)
		}
	}
	require.Len(t, hinted, 1, "one labelled projected scan carries the equality: %#v", options)
	require.Equal(t, []string{"id", "source"}, hinted[0].Projection,
		"the scan decodes the row's properties and the equality's")
	require.True(t, hinted[0].QueryNodes, "the row keeps its embedding metadata from the same scan")
	require.Equal(t, map[string]string{"id": "reading-1"}, hinted[0].PropertyStringEquals)
	require.NotNil(t, hinted[0].PropertyFilter)
	require.True(t, hinted[0].PropertyFilter(map[string]interface{}{"id": "reading-1"}))
	require.False(t, hinted[0].PropertyFilter(map[string]interface{}{"id": "reading-2"}))
	require.False(t, hinted[0].PropertyFilter(map[string]interface{}{}))
	require.Empty(t, hinted[0].Prefix, "the namespace scope belongs to the store")
}

// A labelled MATCH with no equality keeps the label-indexed stream.
func TestNOR214LabelledScanWithoutEqualityKeepsLabelStream(t *testing.T) {
	exec, probe := newNOR214Executor(t)
	require.Equal(t, [][]interface{}{{"reading-1"}, {"reading-2"}, {"reading-9"}},
		nor214Rows(t, exec, "MATCH (r:Reading) RETURN r.id AS id ORDER BY id", nil))
	options, labelScans, _ := probe.take()
	require.Positive(t, labelScans, "an unrestricted labelled scan is the label-indexed stream: %#v", options)
	require.Empty(t, options, "no options-driven scan runs for it")
}

// Without a row projection the equality scan decodes every property, so a
// label-less match returns the whole node the scan supplied and reads no
// second copy of it.
func TestNOR214LabellessEqualityRowIsTheWholeNode(t *testing.T) {
	probe := &nor214ScanProbe{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor214")}
	exec := NewStorageExecutor(probe)
	_, err := exec.Execute(context.Background(), "CREATE ({id: 'plain', name: 'one', extra: 'x'})", nil)
	require.NoError(t, err)
	probe.take()

	result, err := exec.Execute(context.Background(), "MATCH (n {id: 'plain'}) RETURN n AS node", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	node, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, map[string]interface{}{"id": "plain", "name": "one", "extra": "x"}, node.Properties,
		"a scan without a projection carries the whole node")

	options, _, pointReads := probe.take()
	require.Zero(t, pointReads, "the row costs no point read")
	require.Len(t, options, 1)
	require.Nil(t, options[0].Projection, "the row needs every property")
	require.NotNil(t, options[0].PropertyFilter)
}

// requiredScanFailureEngine fails the equality scan after it has streamed the
// nodes it keeps, so the statement's reaction to a late scan failure is
// observable through public Execute. Other scans are untouched.
type requiredScanFailureEngine struct {
	*storage.NamespacedEngine
	failure error
}

func (e *requiredScanFailureEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if opts.PropertyFilter == nil {
		return e.NamespacedEngine.StreamNodesWithOptions(ctx, opts, visit)
	}
	if err := e.NamespacedEngine.StreamNodesWithOptions(ctx, opts, visit); err != nil {
		return err
	}
	return e.failure
}

// A scan that fails is the statement's error: the rows it already streamed are
// not returned as a successful result.
func TestNOR214EqualityScanFailureFailsStatement(t *testing.T) {
	failure := errors.New("scan failed after the first match")
	engine := &requiredScanFailureEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor214"), failure: failure}
	exec := NewStorageExecutor(engine)
	_, err := exec.Execute(context.Background(), "CREATE ({id: 'a'}), ({id: 'b'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(context.Background(), "MATCH (n {id: 'a'}) RETURN n.name AS name", nil)
	require.ErrorIs(t, err, failure)
	require.Nil(t, result, "no partial result is returned")
}

// requiredScanCancelEngine cancels the statement's context once the equality
// scan has delivered a node, so the scan's own cancellation check is what ends
// it.
type requiredScanCancelEngine struct {
	*storage.NamespacedEngine
	cancel context.CancelFunc
}

func (e *requiredScanCancelEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if opts.PropertyFilter == nil {
		return e.NamespacedEngine.StreamNodesWithOptions(ctx, opts, visit)
	}
	return e.NamespacedEngine.StreamNodesWithOptions(ctx, opts, func(node *storage.Node) error {
		if err := visit(node); err != nil {
			return err
		}
		e.cancel()
		return nil
	})
}

// A canceled statement ends the equality scan with the caller's context error.
func TestNOR214EqualityScanStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	engine := &requiredScanCancelEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor214"), cancel: cancel}
	exec := NewStorageExecutor(engine)
	_, err := exec.Execute(context.Background(), "CREATE ({id: 'a'}), ({id: 'b'}), ({id: 'c'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (n {id: 'a'}) RETURN n.id AS id", nil)
	require.ErrorIs(t, err, context.Canceled)
}

// The reported NOR-214 shape: each scoped subquery resolves one supplied id
// through the labelled equality.
func TestNOR214ScopedCallLookupShape(t *testing.T) {
	exec, _ := newNOR214Executor(t)
	ids := []interface{}{"reading-1", "reading-2", "reading-404"}
	result, err := exec.Execute(context.Background(),
		"CALL { UNWIND $ids AS id MATCH (r:Reading {id: id}) RETURN r.id AS id, r.source AS source } RETURN id, source", map[string]interface{}{"ids": ids})
	require.NoError(t, err)
	seen := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 2)
		require.Equal(t, "repository documentation", row[1])
		seen = append(seen, row[0].(string))
	}
	sort.Strings(seen)
	require.Equal(t, []string{"reading-1", "reading-2"}, seen)
}
