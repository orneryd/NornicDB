package cypher

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The fixture injects an arbitrary read failure at the read a MATCH issues on
// a store that exposes only storage.Engine (no label reader or query-node
// read): a labelled pattern restricted by a property equality reads through the
// options scan, and an unrestricted label pattern through GetNodesByLabel, so
// the fixture fails whichever of the two the pipeline performs for that label.
// The statement's own transaction never calls such a store, so the injected
// error can only be observed at the clause pipeline that builds the MATCH
// rows - the component these tests drive. writes counts the write calls the
// pipeline makes, so "the statement wrote nothing" is observed directly
// instead of inferred from a count.
type createMatchErrEngine struct {
	storage.Engine
	labelErrs       map[string]error
	allErr          error
	edgesBetweenErr error
	writes          int
}

func (e *createMatchErrEngine) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	if err, ok := e.labelErrs[label]; ok && err != nil {
		return nil, err
	}
	return e.Engine.GetNodesByLabel(ctx, label)
}

func (e *createMatchErrEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if err, ok := e.labelErrs[opts.Label]; ok && err != nil {
		return err
	}
	return e.Engine.StreamNodesWithOptions(ctx, opts, visit)
}

func (e *createMatchErrEngine) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	return fixtureowner.ReadWithFailure(ctx, e.allErr, func(ctx context.Context) ([]*storage.Node, error) {
		return e.Engine.AllNodes(ctx)
	})
}

func (e *createMatchErrEngine) GetOutgoingEdges(ctx context.Context, startID storage.NodeID) ([]*storage.Edge, error) {
	return fixtureowner.ReadWithFailure(ctx, e.edgesBetweenErr, func(ctx context.Context) ([]*storage.Edge, error) {
		return e.Engine.GetOutgoingEdges(ctx, startID)
	})
}

func (e *createMatchErrEngine) GetIncomingEdges(ctx context.Context, endID storage.NodeID) ([]*storage.Edge, error) {
	return fixtureowner.ReadWithFailure(ctx, e.edgesBetweenErr, func(ctx context.Context) ([]*storage.Edge, error) {
		return e.Engine.GetIncomingEdges(ctx, endID)
	})
}

func (e *createMatchErrEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	e.writes++
	return e.Engine.CreateNode(ctx, node)
}

func (e *createMatchErrEngine) CreateEdge(ctx context.Context, edge *storage.Edge) error {
	e.writes++
	return e.Engine.CreateEdge(ctx, edge)
}

// readerEntryCancelContext is a real context created by context.WithCancel that
// fires its cancellation from the first observation of the query's context
// made inside the named owner function. The observation is only accepted while
// the public Execute frame is on the stack, so the cancellation cannot be
// scheduled before the query (or by an unrelated read): everything else -
// Deadline, Done, Value and error identity - is delegated to the real context.
// The accepted stack is kept so a test can prove the owner and the pipeline
// that ran.
type readerEntryCancelContext struct {
	context.Context
	cancel  context.CancelFunc
	owner   string
	once    sync.Once
	reached bool
	frames  []string
}

func newReaderEntryCancelContext(owner string) (*readerEntryCancelContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &readerEntryCancelContext{Context: ctx, cancel: cancel, owner: owner}, cancel
}

func (c *readerEntryCancelContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	var pcs [96]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var names []string
	public, owner := false, false
	for {
		frame, more := frames.Next()
		names = append(names, frame.Function)
		public = public || strings.HasSuffix(frame.Function, ".(*StorageExecutor).Execute")
		owner = owner || strings.HasSuffix(frame.Function, c.owner)
		if !more {
			break
		}
	}
	if public && owner {
		c.once.Do(func() {
			c.reached = true
			c.frames = names
			c.cancel()
		})
	}
	return c.Context.Err()
}

// requireFrameContaining fails unless the recorded cancellation stack contains
// a function whose name has the given suffix.
func requireFrameContaining(t *testing.T, frames []string, suffix string) {
	t.Helper()
	for _, name := range frames {
		if strings.HasSuffix(name, suffix) {
			return
		}
	}
	require.Failf(t, "recorded reader stack is missing an owner", "no frame ends with %q in %v", suffix, frames)
}

// A store read failure while the clause pipeline builds MATCH rows is the
// statement's error and the write clause is never reached. This is a COMPONENT
// proof through the canonical pipeline entry: the injected arbitrary error is
// the fixture's, not an error the public transaction path can inject (its
// storage reads are answered by BadgerTransaction; the public native
// counterparts below prove that path with a real cancellation at the same
// pipeline read owner instead).
func TestComponentMatchCreateSurfacesLabelReadFailure(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "create_match_err_label")
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"id": "a1"}})
	require.NoError(t, err)
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"id": "b1"}})
	require.NoError(t, err)

	query := "MATCH (a:A),(b:B) WHERE NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n"
	lookupErr := errors.New("label lookup failed")
	failing := &createMatchErrEngine{Engine: store, labelErrs: map[string]error{"A": lookupErr}}
	_, err = NewStorageExecutor(failing).executeRequiredPipeline(ctx, query)
	require.ErrorIs(t, err, lookupErr, "the store read failure is the statement's error")
	require.Zero(t, failing.writes, "the failed statement reached no write clause")

	// Sensitivity control: the same fixture and pipeline write the statement's
	// relationship when no fault is injected, so the assertions above are not
	// satisfied by a statement that can never write.
	healthy := &createMatchErrEngine{Engine: store}
	result, err := NewStorageExecutor(healthy).executeRequiredPipeline(ctx, query)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	require.Positive(t, healthy.writes)
}

// The label-less counterpart: the pipeline reads the whole node population and
// surfaces its failure without writing. Component proof as above.
func TestComponentMatchCreateSurfacesAllNodesReadFailure(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "create_match_err_all")
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"id": "a1"}})
	require.NoError(t, err)

	query := "MATCH (a),(b) WHERE NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n"
	allErr := errors.New("all nodes failed")
	failing := &createMatchErrEngine{Engine: store, allErr: allErr}
	_, err = NewStorageExecutor(failing).executeRequiredPipeline(ctx, query)
	require.ErrorIs(t, err, allErr)
	require.Zero(t, failing.writes, "the failed statement reached no write clause")

	healthy := &createMatchErrEngine{Engine: store}
	result, err := NewStorageExecutor(healthy).executeRequiredPipeline(ctx, query)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	require.Positive(t, healthy.writes)
}

// The predicate's adjacency read failing while the pipeline evaluates the
// WHERE is the statement's error, and no relationship is created. Component
// proof through the canonical pipeline entry.
func TestComponentMatchCreateSurfacesRelationshipCheckReadFailure(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "create_match_err_rel")
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"id": "a1"}})
	require.NoError(t, err)
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"id": "b1"}})
	require.NoError(t, err)
	query := "MATCH (a:A),(b:B) WHERE a.id = 'a1' AND b.id = 'b1' AND NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n"

	outErr := errors.New("edges-between lookup failed")
	failing := &createMatchErrEngine{Engine: store, edgesBetweenErr: outErr}
	_, err = NewStorageExecutor(failing).executeRequiredPipeline(ctx, query)
	require.ErrorIs(t, err, outErr)
	require.Zero(t, failing.writes, "the failed statement reached no write clause")
	edges, err := store.GetOutgoingEdges(ctx, "a1")
	require.NoError(t, err)
	require.Empty(t, edges, "the failed statement wrote no relationship")

	healthy := &createMatchErrEngine{Engine: store}
	result, err := NewStorageExecutor(healthy).executeRequiredPipeline(ctx, query)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	require.Positive(t, healthy.writes)
	edges, err = store.GetOutgoingEdges(ctx, "a1")
	require.NoError(t, err)
	require.Len(t, edges, 1)
}

// A predicate adjacency read failure in any direction stops the statement
// before its CREATE: no created node exists afterwards. Component proof as
// above, with the healthy control proving the statement shape writes.
func TestComponentMatchCreatePredicateReadFailureStopsWrites(t *testing.T) {
	ctx := context.Background()
	for _, direction := range []string{"(a)-[:R]->(b)", "(a)<-[:R]-(b)", "(a)-[:R]-(b)"} {
		t.Run(direction, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "predicate_failure")
			for _, identifier := range []storage.NodeID{"a", "b"} {
				_, err := store.CreateNode(t.Context(), &storage.Node{ID: identifier, Labels: []string{"Endpoint"}, Properties: map[string]interface{}{"id": string(identifier)}})
				require.NoError(t, err)
			}
			query := "MATCH (a:Endpoint {id:'a'}), (b:Endpoint {id:'b'}) WHERE NOT " + direction + " CREATE (:Unexpected)"
			lookupErr := errors.New("predicate adjacency failed")
			failing := &createMatchErrEngine{Engine: store, edgesBetweenErr: lookupErr}
			_, err := NewStorageExecutor(failing).executeRequiredPipeline(ctx, query)
			require.ErrorIs(t, err, lookupErr)
			require.Zero(t, failing.writes, "the failed statement reached no write clause")
			created, err := store.GetNodesByLabel(ctx, "Unexpected")
			require.NoError(t, err)
			require.Empty(t, created)

			healthy := &createMatchErrEngine{Engine: store}
			_, err = NewStorageExecutor(healthy).executeRequiredPipeline(ctx, query)
			require.NoError(t, err)
			require.Positive(t, healthy.writes)
			created, err = store.GetNodesByLabel(ctx, "Unexpected")
			require.NoError(t, err)
			require.Len(t, created, 1)
		})
	}
}

// The public native counterpart for the MATCH row reads: the same statements
// as their component tests, executed by public Execute so the statement runs in
// its own implicit transaction, with the caller's real cancellation scheduled
// from the first observation of the statement's context made inside the named
// transaction reader. The recorded stack proves the pipeline read owner that
// ran, the graph proves nothing was written, and the healthy retry proves the
// statement shape and the same owner work on their own.
func TestPublicMatchCreateReadFailureRollsBackOnCancellation(t *testing.T) {
	cases := []struct {
		name        string
		seed        []string
		query       string
		reader      string
		owner       string
		wrote       string
		wroteBefore int64
		wroteAfter  int64
		rows        [][]interface{}
	}{
		{
			name:        "label scan",
			seed:        []string{"CREATE (:A {id: 'a1'})", "CREATE (:B {id: 'b1'})"},
			query:       "MATCH (a:A),(b:B) WHERE NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n",
			reader:      ".(*BadgerTransaction).streamNodesByLabelProjected",
			owner:       ".(*StorageExecutor).visitNodesWithStreamingProjection",
			wrote:       "MATCH ()-[r:R]->() RETURN count(r) AS c",
			wroteBefore: 0,
			wroteAfter:  1,
			rows:        [][]interface{}{{int64(1)}},
		},
		{
			name:        "label-less scan",
			seed:        []string{"CREATE (:A {id: 'a1'})"},
			query:       "MATCH (a),(b) WHERE NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n",
			reader:      ".(*BadgerTransaction).StreamNodesWithOptions",
			owner:       ".(*StorageExecutor).visitNodesWithStreamingProjection",
			wrote:       "MATCH ()-[r:R]->() RETURN count(r) AS c",
			wroteBefore: 0,
			wroteAfter:  1,
			rows:        [][]interface{}{{int64(1)}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_read_rollback")
			exec := NewStorageExecutor(store)
			for _, statement := range tc.seed {
				_, err := exec.Execute(context.Background(), statement, nil)
				require.NoError(t, err)
			}
			countRows := func() [][]interface{} {
				result, err := exec.Execute(context.Background(), tc.wrote, nil)
				require.NoError(t, err)
				return result.Rows
			}
			require.Equal(t, [][]interface{}{{tc.wroteBefore}}, countRows())

			observer, cancel := newReaderEntryCancelContext(tc.reader)
			defer cancel()
			_, err := exec.Execute(observer, tc.query, nil)
			require.True(t, observer.reached,
				"the query must reach %s inside public Execute", tc.reader)
			require.ErrorIs(t, err, context.Canceled, "the reader's own cancellation is the statement's error")
			requireFrameContaining(t, observer.frames, tc.owner)
			requireFrameContaining(t, observer.frames, tc.reader)
			require.Equal(t, [][]interface{}{{tc.wroteBefore}}, countRows(),
				"the canceled statement committed nothing")

			// Healthy retry of the same statement on the same store.
			retried, err := exec.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, tc.rows, retried.Rows)
			require.Equal(t, [][]interface{}{{tc.wroteAfter}}, countRows())
		})
	}
}

// The predicate-adjacency statements read adjacency through the transaction's
// edge header read: traversalRelationshipsNeedOnlyHeaders selects
// OutgoingEdgeHeaders/IncomingEdgeHeaders for a pattern with no relationship
// variable, properties or path, and transactionStorageWrapper.userEdgeHeaders
// projects that read on its own lifetime (no caller context). There is
// therefore no public context boundary inside the adjacency read itself, so
// this pair does not claim a public arbitrary-I/O injection there. Instead the
// public binding is proven to consult the read: the same statement writes when
// the relationship is absent and writes nothing when it is present, and a
// cancellation scheduled at the traversal that owns the read (the statement's
// nearest cancellable owner) still rolls the statement back. The arbitrary
// adjacency read error is proven at the component above.
func TestPublicMatchCreatePredicateAdjacencyIsConsultedAndRollsBack(t *testing.T) {
	cases := []struct {
		direction string
		present   string
	}{
		{direction: "(a)-[:R]->(b)", present: "MATCH (a:Endpoint {id:'a'}), (b:Endpoint {id:'b'}) CREATE (a)-[:R]->(b)"},
		{direction: "(a)<-[:R]-(b)", present: "MATCH (a:Endpoint {id:'a'}), (b:Endpoint {id:'b'}) CREATE (b)-[:R]->(a)"},
		{direction: "(a)-[:R]-(b)", present: "MATCH (a:Endpoint {id:'a'}), (b:Endpoint {id:'b'}) CREATE (a)-[:R]->(b)"},
	}
	for _, tc := range cases {
		t.Run(tc.direction, func(t *testing.T) {
			query := "MATCH (a:Endpoint {id:'a'}), (b:Endpoint {id:'b'}) WHERE NOT " + tc.direction + " CREATE (:Unexpected)"
			unexpected := func(exec *StorageExecutor) int {
				result, err := exec.Execute(context.Background(), "MATCH (n:Unexpected) RETURN count(n) AS c", nil)
				require.NoError(t, err)
				return int(result.Rows[0][0].(int64))
			}

			t.Run("absent relationship writes", func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "predicate_absent"))
				_, err := exec.Execute(context.Background(), "CREATE (:Endpoint {id: 'a'}), (:Endpoint {id: 'b'})", nil)
				require.NoError(t, err)
				_, err = exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Equal(t, 1, unexpected(exec))
			})

			t.Run("present relationship refuses the write", func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "predicate_present"))
				_, err := exec.Execute(context.Background(), "CREATE (:Endpoint {id: 'a'}), (:Endpoint {id: 'b'})", nil)
				require.NoError(t, err)
				_, err = exec.Execute(context.Background(), tc.present, nil)
				require.NoError(t, err)
				_, err = exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Zero(t, unexpected(exec),
					"the public statement must read the existing adjacency")
			})

			t.Run("cancellation at the traversal rolls back", func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "predicate_cancelled")
				exec := NewStorageExecutor(store)
				_, err := exec.Execute(context.Background(), "CREATE (:Endpoint {id: 'a'}), (:Endpoint {id: 'b'})", nil)
				require.NoError(t, err)

				observer, cancel := newReaderEntryCancelContext(".(*TraversalContext).traversalCancelled")
				defer cancel()
				_, err = exec.Execute(observer, query, nil)
				require.True(t, observer.reached, "the statement must reach the traversal that reads adjacency")
				require.ErrorIs(t, err, context.Canceled)
				requireFrameContaining(t, observer.frames, ".(*StorageExecutor).traverseGraphSequential")
				edges, err := store.AllEdges(context.Background())
				require.NoError(t, err)
				require.Empty(t, edges, "the canceled statement committed no relationship")
				require.Zero(t, unexpected(exec), "the canceled statement created no node")

				_, err = exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Equal(t, 1, unexpected(exec))
			})
		})
	}
}

func TestBuildCombinationsUsingWhereJoin_Branches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	// Single-variable component path (unconstrained `c` component).
	single, ok := exec.buildCombinationsUsingWhereJoin(t.Context(),
		[]struct {
			variable string
			nodes    []*storage.Node
		}{
			{variable: "a", nodes: []*storage.Node{{ID: "a1", Properties: map[string]interface{}{"id": "x"}}}},
			{variable: "b", nodes: []*storage.Node{{ID: "b1", Properties: map[string]interface{}{"id": "x"}}}},
			{variable: "c", nodes: []*storage.Node{{ID: "c1", Properties: map[string]interface{}{"id": "z"}}}},
		},
		"a.id = b.id",
	)
	require.True(t, ok)
	require.Len(t, single, 1)
	require.Equal(t, storage.NodeID("a1"), single[0]["a"].ID)
	require.Equal(t, storage.NodeID("b1"), single[0]["b"].ID)
	require.Equal(t, storage.NodeID("c1"), single[0]["c"].ID)

	// Multi-variable path with rows that trigger nil/missing branches before producing one valid join.
	joined, ok := exec.buildCombinationsUsingWhereJoin(t.Context(),
		[]struct {
			variable string
			nodes    []*storage.Node
		}{
			{
				variable: "a",
				nodes: []*storage.Node{
					nil,
					{ID: "a_nil_props", Properties: nil},
					{ID: "a_missing", Properties: map[string]interface{}{"other": "v"}},
					{ID: "a_ok", Properties: map[string]interface{}{"id": "k1"}},
				},
			},
			{
				variable: "b",
				nodes: []*storage.Node{
					{ID: "b_nil_props", Properties: nil},
					{ID: "b_missing", Properties: map[string]interface{}{"other": "v"}},
					{ID: "b_no_match", Properties: map[string]interface{}{"id": "k2"}},
					{ID: "b_ok", Properties: map[string]interface{}{"id": "k1"}},
				},
			},
		},
		"a.id = b.id",
	)
	require.True(t, ok)
	require.Len(t, joined, 1)
	require.Equal(t, storage.NodeID("a_ok"), joined[0]["a"].ID)
	require.Equal(t, storage.NodeID("b_ok"), joined[0]["b"].ID)
}
