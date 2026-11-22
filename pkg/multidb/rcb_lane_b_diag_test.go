package multidb

// Root-cause diagnostic, v1.5.2 verification campaign, lane B (test-only).
// G-test-catalog-probe-close (NOR-360): f25CatalogReceiptProbe.Close
// (catalog_publication_test.go:36-41) closes closeStarted on every call.

import (
	"context"
	"fmt"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type rcbCountingProbe struct {
	*f25CatalogReceiptProbe
	once  bool
	guard sync.Once
	calls int
}

func (p *rcbCountingProbe) Close() error {
	p.calls++
	if p.once {
		if p.closeStarted != nil {
			p.guard.Do(func() { close(p.closeStarted) })
		}
		return p.Engine.Close()
	}
	return p.f25CatalogReceiptProbe.Close()
}

func TestRCBCatalogProbeCloseCount(t *testing.T) {
	for _, once := range []bool{false, true} {
		t.Run(fmt.Sprintf("closeOnce=%v", once), func(t *testing.T) {
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			probe := &rcbCountingProbe{f25CatalogReceiptProbe: &f25CatalogReceiptProbe{Engine: base, native: base.BadgerEngine, ownerContext: context.Background()}, once: once}
			manager, err := NewDatabaseManager(t.Context(), probe, nil)
			if err != nil {
				t.Fatal(err)
			}
			var boundary fixtureowner.Boundary
			fixtureowner.Own(&boundary, manager)
			probe.closeStarted = make(chan struct{})
			err = manager.Close()
			t.Logf("RCB360 closeOnce=%v after manager.Close err=%v probe.Close calls=%d", once, err, probe.calls)
			var recovered interface{}
			func() {
				defer func() { recovered = recover() }()
				err = boundary.Close()
			}()
			t.Logf("RCB360 closeOnce=%v after registered cleanup err=%v probe.Close calls=%d panic=%v", once, err, probe.calls, recovered)
		})
	}
}
