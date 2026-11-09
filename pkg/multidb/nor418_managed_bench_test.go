package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkNOR418ManagedRelationshipUpdate updates one relationship through
// DatabaseManager.GetStorage. With storage limits the update runs through the
// managed (size-tracking) preparation path; "observed" supplies Before.
func BenchmarkNOR418ManagedRelationshipUpdate(b *testing.B) {
	for _, variant := range []struct {
		name              string
		limited, observed bool
	}{
		{"unlimited-blind", false, false},
		{"unlimited-observed", false, true},
		{"limited-blind", true, false},
		{"limited-observed", true, true},
	} {
		b.Run(variant.name, func(b *testing.B) {
			ctx := b.Context()
			base := fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			manager, err := NewDatabaseManager(ctx, base, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = manager.Close() })
			name := manager.DefaultDatabaseName()
			store, err := manager.GetStorage(name)
			if err != nil {
				b.Fatal(err)
			}
			for _, id := range []storage.NodeID{"a", "b"} {
				if _, err := store.CreateNode(ctx, &storage.Node{ID: id}); err != nil {
					b.Fatal(err)
				}
			}
			if err := store.CreateEdge(ctx, &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "COUNT", Properties: map[string]any{"n": int64(0)}}); err != nil {
				b.Fatal(err)
			}
			if variant.limited {
				if err := manager.SetDatabaseLimits(ctx, name, &Limits{Storage: StorageLimits{MaxNodes: 100, MaxEdges: 10}}); err != nil {
					b.Fatal(err)
				}
			}
			current, err := store.GetEdge(ctx, "r")
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			i := int64(0)
			for b.Loop() {
				i++
				after := storage.CopyEdge(current)
				after.Properties["n"] = i
				request := storage.EdgeUpdateRequest{After: after}
				if variant.observed {
					request.Before = current
				}
				if err := store.UpdateEdge(ctx, request); err != nil {
					b.Fatal(err)
				}
				current = after
			}
		})
	}
}
