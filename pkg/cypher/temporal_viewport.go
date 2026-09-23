package cypher

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type temporalViewportKeyType struct{}

var temporalViewportKey = temporalViewportKeyType{}

type TemporalViewportMode int

const (
	TemporalViewportLive TemporalViewportMode = iota
	TemporalViewportAsOf
)

type TemporalViewport struct {
	Mode TemporalViewportMode
	AsOf time.Time
}

func CurrentTemporalViewport() TemporalViewport {
	return TemporalViewport{Mode: TemporalViewportLive}
}

func AsOfTemporalViewport(asOf time.Time) TemporalViewport {
	return TemporalViewport{Mode: TemporalViewportAsOf, AsOf: asOf.UTC()}
}

func (v TemporalViewport) Enabled() bool {
	return v.Mode == TemporalViewportAsOf && !v.AsOf.IsZero()
}

func WithTemporalViewport(ctx context.Context, viewport TemporalViewport) context.Context {
	return context.WithValue(ctx, temporalViewportKey, viewport)
}

func TemporalViewportFromContext(ctx context.Context) (TemporalViewport, bool) {
	viewport, ok := ctx.Value(temporalViewportKey).(TemporalViewport)
	return viewport, ok
}

type temporalCurrentNodeChecker interface {
	IsCurrentTemporalNode(node *storage.Node, asOf time.Time) (bool, error)
}

func filterNodesByTemporalViewport(nodes []*storage.Node, viewport TemporalViewport, checker temporalCurrentNodeChecker) ([]*storage.Node, error) {
	if !viewport.Enabled() || checker == nil {
		return nodes, nil
	}
	filtered := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if node == nil {
			continue
		}
		visible, err := checker.IsCurrentTemporalNode(node, viewport.AsOf)
		if err != nil {
			return nil, err
		}
		if !visible {
			continue
		}
		filtered = append(filtered, node)
	}
	return filtered, nil
}

func nodeVisibleInTemporalViewport(node *storage.Node, viewport TemporalViewport, checker temporalCurrentNodeChecker) (bool, error) {
	if !viewport.Enabled() || checker == nil || node == nil {
		return true, nil
	}
	return checker.IsCurrentTemporalNode(node, viewport.AsOf)
}

func filterNodesByRequiredLabels(nodes []*storage.Node, labels []string) []*storage.Node {
	if len(labels) <= 1 {
		return nodes
	}
	filtered := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if mergeNodeHasLabels(node, labels) {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

func (e *StorageExecutor) loadNodesWithTemporalViewport(ctx context.Context, labels []string) ([]*storage.Node, error) {
	// Traversal seeds use the same label-indexed, viewport-aware streaming
	// collector as node MATCH. Keeping this adapter avoids a second physical
	// scan path that materialises GetNodesByLabel inside explicit transactions.
	return e.collectNodesWithStreaming(ctx, labels, nil, "", "", -1)
}
