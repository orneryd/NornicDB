// NOR-312 duplicate convergence boundary: the converged first-match owner must
// keep each identity family's published not-found contract. Ordinary node/edge
// reads and the update/delete routes compare the sentinel directly, MVCC reads
// and the projected read tail accept a wrapped sentinel, and a failing source's
// head payload survives a non-not-found error. These tests prove that wrapping
// ErrNotFound does not silently change which source answers, that a provider's
// not-found never retries the same source through the plain read, and that the
// writer routes never write after a non-not-found presence failure.
package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// wrappedNotFound is a source error that wraps ErrNotFound. Producers today
// return the sentinel itself, so the difference between a direct comparison and
// errors.Is is unobservable in production; these tests keep the contract
// explicit.
type wrappedNotFound struct{ source string }

func (e *wrappedNotFound) Error() string { return "source " + e.source + ": wrapped not found" }
func (e *wrappedNotFound) Unwrap() error { return ErrNotFound }

// boundarySource is a constituent whose identity reads and provider
// capabilities report the configured results.
type boundarySource struct {
	Engine
	node         *Node
	nodeErr      error
	edge         *Edge
	edgeErr      error
	projectedErr error
	providerErr  error
	head         MVCCHead
	headErr      error
	reads        int
	wrote        bool
}

func (s *boundarySource) GetNode(context.Context, NodeID) (*Node, error) {
	s.reads++
	return s.node, s.nodeErr
}

func (s *boundarySource) GetEdge(context.Context, EdgeID) (*Edge, error) {
	s.reads++
	return s.edge, s.edgeErr
}

func (s *boundarySource) GetNodeProjected(context.Context, NodeID, []string) (*Node, error) {
	s.reads++
	return s.node, s.projectedErr
}

func (s *boundarySource) GetNodeLatestVisible(context.Context, NodeID) (*Node, error) {
	return s.node, s.providerErr
}
func (s *boundarySource) GetNodeVisibleAt(context.Context, NodeID, MVCCVersion) (*Node, error) {
	return s.node, s.providerErr
}
func (s *boundarySource) GetEdgeLatestVisible(context.Context, EdgeID) (*Edge, error) {
	return s.edge, s.providerErr
}
func (s *boundarySource) GetEdgeVisibleAt(context.Context, EdgeID, MVCCVersion) (*Edge, error) {
	return s.edge, s.providerErr
}

func (s *boundarySource) GetNodeLatestEffective(context.Context, NodeID) (*Node, error) {
	return s.node, s.providerErr
}
func (s *boundarySource) GetEdgeLatestEffective(context.Context, EdgeID) (*Edge, error) {
	return s.edge, s.providerErr
}

func (s *boundarySource) GetNodeCurrentHead(context.Context, NodeID) (MVCCHead, error) {
	return s.head, s.headErr
}
func (s *boundarySource) GetEdgeCurrentHead(context.Context, EdgeID) (MVCCHead, error) {
	return s.head, s.headErr
}

func (s *boundarySource) UpdateNode(context.Context, NodeUpdateRequest) error {
	s.wrote = true
	return nil
}
func (s *boundarySource) UpdateEdge(context.Context, EdgeUpdateRequest) error {
	s.wrote = true
	return nil
}
func (s *boundarySource) DeleteNode(context.Context, NodeID) error {
	s.wrote = true
	return nil
}
func (s *boundarySource) DeleteEdge(context.Context, EdgeID) error {
	s.wrote = true
	return nil
}

func newBoundaryComposite(t *testing.T, sources map[string]*boundarySource) *CompositeEngine {
	t.Helper()
	engines := make(map[string]Engine, len(sources))
	names := make(map[string]string, len(sources))
	modes := make(map[string]string, len(sources))
	for alias, source := range sources {
		engines[alias], names[alias], modes[alias] = source, alias, "read_write"
	}
	return NewCompositeEngine(engines, names, modes)
}

// TestFirstConstituentNotFoundBoundary checks the owner directly with a fixed
// source order: a direct comparison stops at the failing source, a wrapped
// comparison advances to the next one.
func TestFirstConstituentNotFoundBoundary(t *testing.T) {
	wrapped := &wrappedNotFound{source: "a"}
	next := &boundarySource{node: &Node{ID: "n"}}
	for _, tc := range []struct {
		name string
		mode constituentNotFoundMode
		seen bool
	}{
		{"direct", constituentNotFoundDirect, false},
		{"wrapped", constituentNotFoundWrapped, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := &boundarySource{nodeErr: wrapped}
			composite := newBoundaryComposite(t, map[string]*boundarySource{"a": first, "b": next})
			value, err := readFirstConstituent(composite, []string{"a", "b"}, tc.mode, false,
				func(engine Engine) (*Node, error) { return engine.GetNode(context.Background(), "n") },
				nil,
			)
			if tc.seen {
				require.NoError(t, err)
				require.Equal(t, NodeID("n"), value.ID)
				require.Equal(t, 1, first.reads)
				require.Equal(t, 1, next.reads)
				return
			}
			var got *wrappedNotFound
			require.ErrorAs(t, err, &got)
			require.Equal(t, "a", got.source)
			require.Nil(t, value)
			require.Zero(t, next.reads, "a direct comparison must not read the next source")
		})
	}
}

// TestWriteFirstConstituentNotFoundBoundary proves the same split for the
// readable-source write routes: only a wrapped not-found advances to another
// source's writer.
func TestWriteFirstConstituentNotFoundBoundary(t *testing.T) {
	wrapped := &wrappedNotFound{source: "a"}
	for _, tc := range []struct {
		name  string
		mode  constituentNotFoundMode
		wrote bool
	}{
		{"direct", constituentNotFoundDirect, false},
		{"wrapped", constituentNotFoundWrapped, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := &boundarySource{nodeErr: wrapped}
			next := &boundarySource{}
			composite := newBoundaryComposite(t, map[string]*boundarySource{"a": first, "b": next})
			err := writeFirstConstituent(composite, []string{"a", "b"}, tc.mode, constituentWriteEntityMutation,
				func(engine Engine) error {
					_, err := engine.GetNode(context.Background(), "n")
					return err
				},
				func(engine Engine) error { return engine.UpdateNode(context.Background(), NodeUpdateRequest{}) },
				ErrNotFound,
			)
			if tc.wrote {
				require.NoError(t, err)
				require.True(t, next.wrote)
				return
			}
			var got *wrappedNotFound
			require.ErrorAs(t, err, &got)
			require.False(t, next.wrote)
		})
	}
}

// TestOrdinaryEntityReadsPropagateWrappedNotFound preserves the direct sentinel
// comparison of the ordinary node and edge reads.
func TestOrdinaryEntityReadsPropagateWrappedNotFound(t *testing.T) {
	ctx := context.Background()
	wrapped := &wrappedNotFound{source: "a"}
	composite := newBoundaryComposite(t, map[string]*boundarySource{
		"a": {nodeErr: wrapped, edgeErr: wrapped},
	})

	node, err := composite.GetNode(ctx, NodeID("4:a:n"))
	require.Nil(t, node)
	var nodeErr *wrappedNotFound
	require.ErrorAs(t, err, &nodeErr)

	edge, err := composite.GetEdge(ctx, EdgeID("5:a:e"))
	require.Nil(t, edge)
	var edgeErr *wrappedNotFound
	require.ErrorAs(t, err, &edgeErr)
}

// TestProjectedReadKeepsWrappedNotFoundContinuation records the projected read
// tail's current errors.Is contract: a wrapped sentinel means "absent here" and
// the walk ends with the sentinel, never with the wrapper.
func TestProjectedReadKeepsWrappedNotFoundContinuation(t *testing.T) {
	wrapped := &wrappedNotFound{source: "a"}
	composite := newBoundaryComposite(t, map[string]*boundarySource{
		"a": {projectedErr: wrapped},
	})
	node, err := composite.GetNodeProjected(context.Background(), NodeID("4:a:n"), []string{"k"})
	require.Nil(t, node)
	require.ErrorIs(t, err, ErrNotFound)
	var got *wrappedNotFound
	require.False(t, errors.As(err, &got), "the projected tail converts a wrapped sentinel to ErrNotFound")
}

// TestMVCCProviderNotFoundNeverRetriesSameSource proves the provider branch
// keeps moving to the next source instead of falling back to the same source's
// plain read. The plain read here would succeed, so a fallback would return it.
func TestMVCCProviderNotFoundNeverRetriesSameSource(t *testing.T) {
	ctx := context.Background()
	wrapped := &wrappedNotFound{source: "a"}
	fallbackNode := &Node{ID: "n"}
	fallbackEdge := &Edge{ID: "e"}
	reads := map[string]func(*CompositeEngine) (*Node, error){
		"GetNodeLatestVisible": func(c *CompositeEngine) (*Node, error) { return c.GetNodeLatestVisible(ctx, NodeID("4:a:n")) },
		"GetNodeVisibleAt": func(c *CompositeEngine) (*Node, error) {
			return c.GetNodeVisibleAt(ctx, NodeID("4:a:n"), MVCCVersion{})
		},
		"GetNodeLatestEffective": func(c *CompositeEngine) (*Node, error) { return c.GetNodeLatestEffective(ctx, NodeID("4:a:n")) },
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			composite := newBoundaryComposite(t, map[string]*boundarySource{
				"a": {node: fallbackNode, providerErr: wrapped},
			})
			value, err := read(composite)
			require.Nil(t, value, "a provider not-found must not reuse the same source's plain read")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
	edgeReads := map[string]func(*CompositeEngine) (*Edge, error){
		"GetEdgeLatestVisible": func(c *CompositeEngine) (*Edge, error) { return c.GetEdgeLatestVisible(ctx, EdgeID("5:a:e")) },
		"GetEdgeVisibleAt": func(c *CompositeEngine) (*Edge, error) {
			return c.GetEdgeVisibleAt(ctx, EdgeID("5:a:e"), MVCCVersion{})
		},
		"GetEdgeLatestEffective": func(c *CompositeEngine) (*Edge, error) { return c.GetEdgeLatestEffective(ctx, EdgeID("5:a:e")) },
	}
	for name, read := range edgeReads {
		t.Run(name, func(t *testing.T) {
			composite := newBoundaryComposite(t, map[string]*boundarySource{
				"a": {edge: fallbackEdge, providerErr: wrapped},
			})
			value, err := read(composite)
			require.Nil(t, value, "a provider not-found must not reuse the same source's plain read")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

// TestHeadReadsKeepFailingPayloadAndNotFoundContinuation proves the head
// adapters still publish the failing source's head beside a non-not-found
// error, while a not-found head advances past the source and leaves no payload.
func TestHeadReadsKeepFailingPayloadAndNotFoundContinuation(t *testing.T) {
	ctx := context.Background()
	payload := MVCCHead{Version: MVCCVersion{CommitSequence: 7}}
	payloadErr := errors.New("head read failed")
	composite := newBoundaryComposite(t, map[string]*boundarySource{
		"a": {head: payload, headErr: payloadErr},
	})
	nodeHead, err := composite.GetNodeCurrentHead(ctx, NodeID("4:a:n"))
	require.Equal(t, payload, nodeHead)
	require.ErrorIs(t, err, payloadErr)
	edgeHead, err := composite.GetEdgeCurrentHead(ctx, EdgeID("5:a:e"))
	require.Equal(t, payload, edgeHead)
	require.ErrorIs(t, err, payloadErr)

	wrapped := &wrappedNotFound{source: "a"}
	composite = newBoundaryComposite(t, map[string]*boundarySource{
		"a": {head: payload, headErr: wrapped},
	})
	nodeHead, err = composite.GetNodeCurrentHead(ctx, NodeID("4:a:n"))
	require.Zero(t, nodeHead)
	require.ErrorIs(t, err, ErrNotFound)
}

// TestEntityWritesPropagateWrappedNotFound preserves the direct sentinel
// comparison of the update and delete routes: a non-not-found presence failure
// reaches the caller without a write.
func TestEntityWritesPropagateWrappedNotFound(t *testing.T) {
	ctx := context.Background()
	wrapped := &wrappedNotFound{source: "a"}
	for _, tc := range []struct {
		name string
		call func(*CompositeEngine) error
	}{
		{"UpdateNode", func(c *CompositeEngine) error {
			return c.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: NodeID("4:a:n")}})
		}},
		{"DeleteNode", func(c *CompositeEngine) error { return c.DeleteNode(ctx, NodeID("4:a:n")) }},
		{"UpdateEdge", func(c *CompositeEngine) error {
			return c.UpdateEdge(ctx, EdgeUpdateRequest{After: &Edge{ID: EdgeID("5:a:e")}})
		}},
		{"DeleteEdge", func(c *CompositeEngine) error { return c.DeleteEdge(ctx, EdgeID("5:a:e")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &boundarySource{nodeErr: wrapped, edgeErr: wrapped}
			composite := newBoundaryComposite(t, map[string]*boundarySource{"a": source})
			err := tc.call(composite)
			var got *wrappedNotFound
			require.ErrorAs(t, err, &got)
			require.False(t, source.wrote, "an unavailable presence read must not fall through to a write")
		})
	}
}
