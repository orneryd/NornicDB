package nornicdb

// Lane L6 (closeout): G-composite-get-constituent, DB.Stats member.
// DB.Stats (db_admin.go:52-58) propagates count errors; the composite count
// (storage composite_engine.go:1585-1587, :1607-1609) returns none. Bypass:
// the same composite view behind a seam whose counts return the constituent
// error, as the corrected composite would, makes DB.Stats return it.

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type zzL6CountSeam struct {
	storage.Engine
	err error
}

func (s zzL6CountSeam) NodeCount(ctx context.Context) (int64, error) { return 0, s.err }
func (s zzL6CountSeam) EdgeCount(ctx context.Context) (int64, error) { return 0, s.err }

func TestL6DBStatsCompositeOwner(t *testing.T) {
	ctx := t.Context()
	denied := nor287CompositeStatsView(t, true)
	n, nErr := denied.NodeCount(ctx)
	e, eErr := denied.EdgeCount(ctx)
	public, publicErr := (&DB{storage: denied}).Stats(ctx)
	bypass, bypassErr := (&DB{storage: zzL6CountSeam{Engine: denied, err: errNOR287StatsInjected}}).Stats(ctx)
	healthy, healthyErr := (&DB{storage: nor287CompositeStatsView(t, false)}).Stats(ctx)
	t.Logf("L6_DBSTATS composite denied NodeCount=%d err=%v EdgeCount=%d err=%v | DB.Stats public=%+v err=%v | DB.Stats over corrected-count seam=%+v err=%v isInjected=%v | healthy control=%+v err=%v",
		n, nErr, e, eErr, public, publicErr, bypass, bypassErr, errors.Is(bypassErr, errNOR287StatsInjected), healthy, healthyErr)
	if healthyErr != nil {
		t.Fatalf("control: %v", healthyErr)
	}
	if !errors.Is(bypassErr, errNOR287StatsInjected) {
		t.Fatalf("premise: DB.Stats did not propagate a count error given one: %v", bypassErr)
	}
	if publicErr == nil && nErr == nil {
		t.Errorf("CAUSE: composite NodeCount answers %d with no error for the denied constituent; DB.Stats propagates errors (seam) but receives none", n)
	}
}
