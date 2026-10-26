package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Hiding optional methods behind Engine models a terminal with only the
// mandatory node kernel. Population reads deliberately fail: they must never
// be a generic node operation's fallback.
type foundationCore struct {
	Engine
	rows, visits, full, counts, streams int
}

func (e *foundationCore) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, visit func(*Node) error) error {
	for i := 0; i < e.rows; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.visits++
		n := &Node{ID: NodeID(fmt.Sprint(i)), Labels: []string{"L"}, Properties: map[string]any{"p": i}}
		if err := visit(opts.copyResidentNode(n)); err != nil {
			return err
		}
	}
	return nil
}
func (e *foundationCore) AllNodes() ([]*Node, error) {
	e.full++
	return nil, errors.New("forbidden whole population")
}
func (e *foundationCore) GetNodesByLabel(string) ([]*Node, error) { return e.AllNodes() }
func (e *foundationCore) NodeCount() (int64, error)               { e.counts++; return int64(e.rows), nil }

// This type intentionally implements only one method of StreamingEngine.
type foundationNodeStream struct{ *foundationCore }

func (e *foundationNodeStream) StreamNodes(ctx context.Context, visit func(*Node) error) error {
	e.streams++
	return e.StreamNodesWithOptions(ctx, StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}, visit)
}

type foundationLabelCount struct{ *foundationCore }

func (e *foundationLabelCount) NodeCountByLabel(string) (int64, error) {
	e.counts++
	return int64(e.rows), nil
}

func TestFoundationOrdinaryNodeKernel(t *testing.T) {
	for _, operation := range []string{"stream", "iterate", "label", "project", "loader", "chunks"} {
		t.Run(operation, func(t *testing.T) {
			e := &foundationCore{rows: 1000}
			seen := 0
			var err error
			switch operation {
			case "stream":
				err = StreamNodesWithFallback(context.Background(), e, 8, func(*Node) error { seen++; return ErrIterationStopped })
			case "iterate":
				err = VisitNodes(e, func(*Node) bool { seen++; return false })
			case "label":
				err = VisitNodeIDsByLabel(e, "L", func(NodeID) bool { seen++; return false })
			case "project":
				err = StreamNodesByLabelProjected(e, "L", []string{}, func(n *Node) error { seen++; require.Empty(t, n.Properties); return ErrIterationStopped })
			case "loader":
				require.NotNil(t, FindNodeNeedingEmbedding(e))
				seen = e.visits
			case "chunks":
				c := NewCompositeEngine(map[string]Engine{"one": e}, nil, map[string]string{"one": "read"})
				err = c.StreamNodeChunks(context.Background(), 8, func(nodes []*Node) error { seen = len(nodes); return ErrIterationStopped })
			}
			if operation == "stream" || operation == "project" || operation == "chunks" {
				require.ErrorIs(t, err, ErrIterationStopped)
			} else {
				require.NoError(t, err)
			}
			require.Positive(t, seen)
			require.LessOrEqual(t, e.visits, 8)
			require.Zero(t, e.full)
		})
	}
}

func TestFoundationOperationMethodIndependence(t *testing.T) {
	e := &foundationNodeStream{&foundationCore{rows: 2}}
	require.NoError(t, StreamNodesWithFallback(context.Background(), e, 8, func(*Node) error { return nil }))
	require.Equal(t, 1, e.streams, "node support must not require unrelated edge/chunk methods")
	require.Zero(t, e.full)
}

func TestFoundationMixedScalarCount(t *testing.T) {
	native := &foundationLabelCount{&foundationCore{rows: 1000000}}
	remote := &foundationCore{rows: 3}
	c := NewCompositeEngine(map[string]Engine{"local": native, "remote": remote}, nil, map[string]string{"local": "read", "remote": "read"})
	count, err := CountNodesWithLabel(context.Background(), c, "L")
	require.NoError(t, err)
	require.Equal(t, int64(1000003), count)
	require.Equal(t, 1, native.counts)
	require.Zero(t, native.visits, "a missing sibling count must not scan a native counter's rows")
	require.Equal(t, 3, remote.visits)
	require.Zero(t, remote.full)
}

type foundationPrefixCount struct{ *foundationCore }

func (e *foundationPrefixCount) NodeCountByPrefix(string) (int64, error) {
	e.counts++
	return int64(e.rows), nil
}
func TestFoundationMixedPrefixCount(t *testing.T) {
	native := &foundationPrefixCount{&foundationCore{rows: 1000000}}
	other := &foundationCore{rows: 3}
	composite := NewCompositeEngine(map[string]Engine{"native": native, "other": other}, nil, map[string]string{"native": "read", "other": "read"})
	count, err := composite.NodeCountByPrefix("")
	require.NoError(t, err)
	require.Equal(t, int64(1000003), count)
	require.Equal(t, 1, native.counts)
	require.Zero(t, native.visits)
	require.Equal(t, 3, other.visits)
	require.Zero(t, other.full)
}
