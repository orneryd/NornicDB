package cypher

import (
	"context"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// nodePatternAcceptance is the MATCH acceptance frontier shared by scans and
// ordered index windows. An access path may narrow which nodes are visited;
// only this predicate may certify a node before it consumes a result slot.
type nodePatternAcceptance struct {
	executor   *StorageExecutor
	labels     []string
	properties map[string]interface{}
	where      func(*storage.Node) bool
	hideSystem bool
	asOf       time.Time
	checker    temporalCurrentNodeChecker
}

func (e *StorageExecutor) compileNodePatternAcceptance(ctx context.Context, pattern nodePatternInfo, where string) nodePatternAcceptance {
	store := e.getStorage(ctx)
	// Retain only the state used while visiting candidates. Parsing metadata
	// and the viewport mode would make the callback capture a separate heap object.
	acceptance := nodePatternAcceptance{executor: e, labels: pattern.labels, properties: pattern.properties, hideSystem: shouldHideSystemNodes(store)}
	if strings.TrimSpace(where) != "" {
		acceptance.where = e.compileNodeWhereFilter(ctx, pattern.variable, where)
	}
	if viewport, present := TemporalViewportFromContext(ctx); present && viewport.Enabled() {
		acceptance.asOf = viewport.AsOf
		acceptance.checker, _ = store.(temporalCurrentNodeChecker)
	}
	return acceptance
}

func (acceptance nodePatternAcceptance) filter(nodes []*storage.Node) ([]*storage.Node, error) {
	accepted := nodes[:0]
	for _, node := range nodes {
		ok, err := acceptance.accepts(node)
		if err != nil {
			return nil, err
		}
		if ok {
			accepted = append(accepted, node)
		}
	}
	return accepted, nil
}

// nodeMatchesPatternInView adapts the same error frontier to binding/stream
// visitors whose existing protocol records expression failures on the statement.
// Pure pattern checks used by mutation and ranking remain context independent.
func (e *StorageExecutor) nodeMatchesPatternInView(ctx context.Context, node *storage.Node, pattern nodePatternInfo) bool {
	accepted, err := e.compileNodePatternAcceptance(ctx, pattern, "").accepts(node)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false
	}
	return accepted
}

func (acceptance nodePatternAcceptance) accepts(node *storage.Node) (bool, error) {
	if node == nil || (acceptance.hideSystem && isSystemNode(node)) {
		return false, nil
	}
	if !acceptance.executor.pipelineNodeMatchesPattern(node, nodePatternInfo{labels: acceptance.labels, properties: acceptance.properties}) {
		return false, nil
	}
	if acceptance.where != nil && !acceptance.where(node) {
		return false, nil
	}
	if acceptance.checker != nil {
		return acceptance.checker.IsCurrentTemporalNode(node, acceptance.asOf)
	}
	return true, nil
}

// The complete node-pattern predicate is also used when row/traversal bindings
// are already available. WHERE remains at that operator's complete-row frontier.
func (e *StorageExecutor) pipelineNodeMatchesPattern(node *storage.Node, pattern nodePatternInfo) bool {
	return node != nil && mergeNodeHasLabels(node, pattern.labels) && e.nodeMatchesProps(node, pattern.properties)
}
