package storage

// Lane L6 (closeout): G-composite-get-constituent, the members the earlier
// run left read-only: EdgeCount (composite_engine.go:1607-1609), GetSchema
// (:1200-1202) and the qualified-read selector constituentsForEntity
// (composite_entity_lookup.go:27-31). For each: the public answer with
// constituent b denied, and a test-side corrected copy that returns the
// getConstituent error instead of `continue`.

import (
	"context"
	"errors"
	"testing"
)

func zzL6EdgeCountPropagating(c *CompositeEngine, ctx context.Context) (int64, error) {
	var total int64
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return 0, err // corrected step: composite_engine.go:1607-1609 has `continue`
		}
		n, err := engine.EdgeCount(ctx)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func zzL6SchemaConstituentsPropagating(c *CompositeEngine) (int, error) {
	merged := 0
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return merged, err // corrected step: composite_engine.go:1200-1202 has `continue`
		}
		if engine.GetSchema() != nil {
			merged++
		}
	}
	return merged, nil
}

func zzL6ConstituentsForEntityPropagating(c *CompositeEngine, kind, value string) ([]string, string, error) {
	actual, database, local, qualified := ParseElementID(value)
	aliases := c.getConstituentsForRead()
	if !qualified {
		return aliases, value, nil
	}
	if actual != kind {
		return nil, local, nil
	}
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			if c.constituentNames[alias] == database {
				return nil, local, err // corrected step: composite_entity_lookup.go:28-30 has `continue`
			}
			continue
		}
		if c.constituentDatabase(alias, engine) == database {
			return []string{alias}, local, nil
		}
	}
	return nil, local, nil
}

func zzL6IndexCount(s *SchemaManager) int {
	if s == nil {
		return -1
	}
	return len(s.GetIndexes())
}

func TestL6CompositeRemainingMembers(t *testing.T) {
	healthy, denied := newConstituentAccessAudit(t)
	ctx := t.Context()
	var accessErr *constituentAccessError

	// Give each constituent one property index so a dropped constituent is visible in the merged schema.
	for alias, engine := range healthy.constituents {
		if s := engine.GetSchema(); s != nil {
			if err := s.AddPropertyIndex("l6_idx_"+alias, "Doc", []string{"value"}); err != nil {
				t.Logf("L6_COMPOSITE add index %s: %v", alias, err)
			}
		}
	}

	hEdges, hErr := healthy.EdgeCount(ctx)
	dEdges, dErr := denied.EdgeCount(ctx)
	cEdges, cErr := zzL6EdgeCountPropagating(denied, ctx)
	t.Logf("L6_COMPOSITE EdgeCount healthy=%d err=%v | denied public=%d err=%v | corrected=%d err=%v isAccessError=%v",
		hEdges, hErr, dEdges, dErr, cEdges, cErr, errors.As(cErr, &accessErr))

	hSchema := zzL6IndexCount(healthy.GetSchema())
	dSchema := zzL6IndexCount(denied.GetSchema())
	merged, sErr := zzL6SchemaConstituentsPropagating(denied)
	t.Logf("L6_COMPOSITE GetSchema indexes healthy=%d denied public=%d (no error channel) | corrected merge constituents=%d err=%v isAccessError=%v",
		hSchema, dSchema, merged, sErr, errors.As(sErr, &accessErr))

	pubSources, pubLocal, pubQualified := denied.constituentsForEntity("4", "4:b:b-2")
	corrSources, corrLocal, corrErr := zzL6ConstituentsForEntityPropagating(denied, "4", "4:b:b-2")
	ctlSources, _, ctlErr := zzL6ConstituentsForEntityPropagating(denied, "4", "4:a:a-1")
	_, pubGetErr := denied.GetNode(ctx, "4:b:b-2")
	t.Logf("L6_COMPOSITE constituentsForEntity(4:b:b-2) public sources=%q local=%q qualified=%v -> GetNode err=%v isNotFound=%v | corrected sources=%q local=%q err=%v isAccessError=%v | control 4:a:a-1 corrected sources=%q err=%v",
		pubSources, pubLocal, pubQualified, pubGetErr, errors.Is(pubGetErr, ErrNotFound), corrSources, corrLocal, corrErr, errors.As(corrErr, &accessErr), ctlSources, ctlErr)

	if dErr == nil && cErr != nil && errors.As(cErr, &accessErr) {
		t.Errorf("CAUSE EdgeCount: public answers %d with no error while the constituent access error exists (%v)", dEdges, cErr)
	}
	if sErr != nil && dSchema < hSchema {
		t.Errorf("CAUSE GetSchema: merged schema drops constituent b (%d of %d indexes) and has no error channel; corrected merge sees %v", dSchema, hSchema, sErr)
	}
	if len(pubSources) == 0 && corrErr != nil {
		t.Errorf("CAUSE constituentsForEntity: qualified read selects no source (GetNode -> %v); corrected selector returns %v", pubGetErr, corrErr)
	}
	if ctlErr != nil || len(ctlSources) != 1 {
		t.Fatalf("control: readable constituent a must still be selected, got %q err=%v", ctlSources, ctlErr)
	}
}
