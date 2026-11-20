package storage

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-composite-get-constituent).
// Test-side only. Shows (1) getConstituent produces the access error, (2) the
// public reads answer without it, and (3) a test-side copy of GetNode and
// NodeCount that returns the getConstituent error instead of `continue`
// delivers the error to the caller.

import (
	"context"
	"errors"
	"testing"
)

func rcaCompositeGetNodePropagating(c *CompositeEngine, ctx context.Context, id NodeID) (*Node, error) {
	readConstituents, local, _ := c.constituentsForEntity("4", string(id))
	id = NodeID(local)
	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err // corrected step: composite_engine.go:417-419 has `continue`
		}
		node, err := engine.GetNode(ctx, id)
		if err == nil {
			return c.sourceNode(alias, engine, node), nil
		}
		if err != ErrNotFound {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func rcaCompositeNodeCountPropagating(c *CompositeEngine, ctx context.Context) (int64, error) {
	var total int64
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return 0, err // corrected step: composite_engine.go:1585-1587 has `continue`
		}
		n, err := engine.NodeCount(ctx)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func TestRCACompositeConstituentErrorDropped(t *testing.T) {
	_, denied := newConstituentAccessAudit(t)
	ctx := t.Context()

	_, ownerErr := denied.getConstituent("b")
	var accessErr *constituentAccessError
	t.Logf("RCA_COMPOSITE getConstituent(b) err=%v isAccessError=%v", ownerErr, errors.As(ownerErr, &accessErr))

	_, pubErr := denied.GetNode(ctx, "4:b:b-2")
	t.Logf("RCA_COMPOSITE public GetNode(4:b:b-2) err=%v isAccessError=%v isNotFound=%v", pubErr, errors.As(pubErr, &accessErr), errors.Is(pubErr, ErrNotFound))
	_, fixErr := rcaCompositeGetNodePropagating(denied, ctx, "4:b:b-2")
	t.Logf("RCA_COMPOSITE corrected GetNode(4:b:b-2) err=%v isAccessError=%v", fixErr, errors.As(fixErr, &accessErr))

	n, cErr := denied.NodeCount(ctx)
	t.Logf("RCA_COMPOSITE public NodeCount=%d err=%v", n, cErr)
	n2, cErr2 := rcaCompositeNodeCountPropagating(denied, ctx)
	t.Logf("RCA_COMPOSITE corrected NodeCount=%d err=%v isAccessError=%v", n2, cErr2, errors.As(cErr2, &accessErr))
}
