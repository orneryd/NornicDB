package cypher

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkLinkPredictionIdentityBinding measures the graph view retained while
// binding native link-prediction inputs, separately from graph traversal.
func BenchmarkLinkPredictionIdentityBinding(b *testing.B) {
	base := fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed).BadgerEngine
	b.Cleanup(func() {
		if err := base.Close(); err != nil {
			b.Fatal(err)
		}
	})
	scoped := storage.NewNamespacedEngine(base, "identity_binding")
	for _, test := range []struct {
		name   string
		engine storage.Engine
	}{
		{"scoped", scoped}, {"existing_view", graphIdentityView(scoped)}, {"nil", nil},
	} {
		b.Run(test.name, func(b *testing.B) {
			ctx := b.Context()
			var config linkPredictionConfig
			b.ReportAllocs()
			for b.Loop() {
				config = linkPredictionConfig{SourceNode: "source"}
				config.bindIdentity(ctx, test.engine, nil)
			}
			b.StopTimer()
			if test.engine == nil && config.identityStore != nil {
				b.Fatal("nil backend must retain its missing-storage contract")
			}
			if test.engine != nil {
				if _, ok := config.identityStore.(*graphIdentityStorage); !ok {
					b.Fatal("binding must retain the identity read view")
				}
			}
			if id := storage.EntityIdentityFromKey(string(config.SourceNode)); id.LocalID != "source" {
				b.Fatalf("binding lost source identity: %#v", id)
			}
		})
	}
}
