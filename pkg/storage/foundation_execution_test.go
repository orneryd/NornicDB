package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestFoundationMixedNestedOperations(t *testing.T) {
	for _, rows := range []int{16, 32768} {
		for _, operation := range []string{"nodes", "chunks", "iteration", "label_ids", "projected"} {
			for _, position := range []string{"a", "z"} {
				t.Run(fmt.Sprintf("%d/%s/%s", rows, operation, position), func(t *testing.T) {
					capable := &foundationNodeStream{&foundationCore{rows: rows}}
					small := &foundationCore{rows: 2}
					nested := NewCompositeEngine(map[string]Engine{"local": capable}, nil, map[string]string{"local": "read"})
					c := NewCompositeEngine(map[string]Engine{position: nested, "incapable": small}, nil, map[string]string{position: "read", "incapable": "read"})
					seen := 0
					visit := func(*Node) error { seen++; return ErrIterationStopped }
					var err error
					switch operation {
					case "nodes":
						err = StreamNodesWithFallback(context.Background(), c, 8, visit)
					case "chunks":
						err = c.StreamNodeChunks(context.Background(), 8, func(nodes []*Node) error { seen += len(nodes); return ErrIterationStopped })
					case "iteration":
						err = VisitNodes(c, func(*Node) bool { seen++; return false })
					case "label_ids":
						err = VisitNodeIDsByLabel(c, "L", func(NodeID) bool { seen++; return false })
					case "projected":
						err = StreamNodesByLabelProjected(c, "L", []string{}, visit)
					}
					if err != nil {
						require.ErrorIs(t, err, ErrIterationStopped)
					}
					require.Positive(t, seen)
					require.LessOrEqual(t, capable.visits, 8, "early stop cannot scan a capable tail")
					require.LessOrEqual(t, small.visits, 2)
					require.Zero(t, capable.full)
					require.Zero(t, small.full)
				})
			}
		}
	}
	// Negative control: the same full-population trap rejects the old fallback.
	c := NewCompositeEngine(map[string]Engine{"capable": &foundationCore{rows: 32768}}, nil, map[string]string{"capable": "read"})
	_, err := c.AllNodes()
	require.ErrorContains(t, err, "forbidden whole population")
}

type foundationRefusingStream struct {
	*foundationNodeStream
	available     bool
	before, after error
	calls         int
}

func (e *foundationRefusingStream) ReadCapabilityAvailable(reflect.Type) bool { return e.available }
func (e *foundationRefusingStream) StreamNodes(ctx context.Context, visit func(*Node) error) error {
	e.calls++
	if e.before != nil {
		return e.before
	}
	return e.foundationNodeStream.StreamNodes(ctx, func(node *Node) error {
		if err := visit(node); err != nil {
			return err
		}
		return e.after
	})
}
func TestFoundationChildRefusalAndNoReplay(t *testing.T) {
	hard := errors.New("hard storage error")
	for _, schedule := range []string{"unavailable", "before", "after", "visitor", "hard", "cancel"} {
		t.Run(schedule, func(t *testing.T) {
			conditional := &foundationRefusingStream{foundationNodeStream: &foundationNodeStream{&foundationCore{rows: 4}}, available: true}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch schedule {
			case "unavailable":
				conditional.available = false
			case "before":
				conditional.before = ErrNotImplemented
			case "after":
				conditional.after = ErrNotImplemented
			case "hard":
				conditional.before = hard
			case "cancel":
				cancel()
			}
			c := NewCompositeEngine(map[string]Engine{"one": conditional}, nil, map[string]string{"one": "read"})
			calls := 0
			err := c.StreamNodes(ctx, func(*Node) error {
				calls++
				if schedule == "visitor" {
					return ErrNotImplemented
				}
				return nil
			})
			switch schedule {
			case "unavailable", "before":
				require.NoError(t, err)
				require.Equal(t, 4, calls)
			case "after", "visitor":
				require.ErrorIs(t, err, ErrNotImplemented)
				require.Equal(t, 1, calls)
			case "hard":
				require.ErrorIs(t, err, hard)
				require.Zero(t, calls)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, calls)
			}
			if schedule == "unavailable" {
				require.Zero(t, conditional.calls)
			}
			require.Zero(t, conditional.full)
		})
	}
}

type foundationStrictReader struct {
	Engine
	panicValue any
	ended      bool
}

func (e *foundationStrictReader) StreamParkedEmbeddingFailures(ctx context.Context, visit func(NodeID, map[string]any) error) (int, error) {
	defer func() { e.ended = true }()
	if e.panicValue != nil {
		panic(e.panicValue)
	}
	return 1, visit("row", map[string]any{"reason": "parked"})
}
func TestFoundationStrictReservationCleanup(t *testing.T) {
	for _, schedule := range []string{"refusal", "visitor", "panic"} {
		t.Run(schedule, func(t *testing.T) {
			reader := &foundationStrictReader{}
			engines := map[string]Engine{"one": reader, "two": &foundationCore{}}
			if schedule == "visitor" {
				engines = map[string]Engine{"one": reader}
			}
			if schedule == "panic" {
				reader.panicValue = "provider panic"
				engines = map[string]Engine{"one": reader}
			}
			modes := map[string]string{}
			for alias := range engines {
				modes[alias] = "read"
			}
			c := NewCompositeEngine(engines, nil, modes)
			calls := 0
			run := func() {
				count, err := c.StreamParkedEmbeddingFailures(context.Background(), func(NodeID, map[string]any) error { calls++; return ErrNotImplemented })
				require.ErrorIs(t, err, ErrNotImplemented)
				if schedule == "refusal" {
					require.Zero(t, count)
					require.Zero(t, calls)
				} else {
					require.Equal(t, 1, count)
					require.Equal(t, 1, calls)
				}
			}
			if schedule == "panic" {
				require.PanicsWithValue(t, "provider panic", run)
			} else {
				run()
			}
			// The refused child may precede the capable one. If it was started, its
			// defer must have completed when the operation returns (race checks this).
			if schedule != "refusal" {
				require.True(t, reader.ended)
			}
		})
	}
}

type foundationOnlyHistory struct {
	Engine
	version MVCCVersion
}

func (e *foundationOnlyHistory) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	e.version = version
	return []*Node{{ID: "past", Labels: []string{label}}}, nil
}
func TestFoundationHistoricalMethodDependencies(t *testing.T) {
	history := &foundationOnlyHistory{}
	c := NewCompositeEngine(map[string]Engine{"history": history}, nil, map[string]string{"history": "read"})
	nodes, err := ReadWithCapability(c, func(reader historicalLabelNodes) ([]*Node, error) {
		return reader.GetNodesByLabelVisibleAt("L", MVCCVersion{CommitSequence: 41})
	}, nil)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, NodeID("past"), nodes[0].ID)
	require.Equal(t, MVCCVersion{CommitSequence: 41}, history.version)
	_, err = ReadWithCapability(c, func(reader historicalOutgoingEdges) ([]*Edge, error) {
		return reader.GetOutgoingEdgesVisibleAt("past", MVCCVersion{CommitSequence: 41})
	}, nil)
	require.ErrorIs(t, err, ErrNotImplemented, "missing historical dependency cannot use current data")
}
