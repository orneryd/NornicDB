package multidb

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestNOR360CatalogProbeCloseAndRegisteredCleanup preserves the probe contract:
// an intentional manager close can precede its fixture boundary's cleanup.
func TestNOR360CatalogProbeCloseAndRegisteredCleanup(t *testing.T) {
	for _, observed := range []bool{false, true} {
		name := "without_observer"
		if observed {
			name = "with_close_observer"
		}
		t.Run(name, func(t *testing.T) {
			manager, probe := newCatalogPublicationProbe(t)
			var boundary fixtureowner.Boundary
			fixtureowner.Own(&boundary, manager)
			if observed {
				probe.closeStarted = make(chan struct{})
			}
			require.NoError(t, manager.Close())
			if observed {
				select {
				case <-probe.closeStarted:
				default:
					t.Fatal("first close did not publish observation")
				}
			}
			assert.NotPanics(t, func() { require.NoError(t, boundary.Close()) }, "registered cleanup after explicit close must not double-close the observation channel")
		})
	}
}
