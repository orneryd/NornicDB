package resolvers

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkGraphQLDatabaseReads(b *testing.B) {
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	if err != nil { b.Fatal(err) }
	resolver := NewResolver(nil, manager)
	b.Cleanup(resolver.EventBroker.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = auth.WithRequestDatabaseScope(ctx, auth.NewRequestDatabaseScope("nornic", map[string]string{"nornic": "nornic"}))
	ctx = auth.WithRequestEntitlementResolver(ctx, func(string, auth.Permission) bool { return true })
	b.Run("count", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := resolver.Query().NodeCount(ctx, nil); err != nil { b.Fatal(err) }
		}
	})
	b.Run("empty-nodes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := resolver.Query().Nodes(ctx, nil); err != nil { b.Fatal(err) }
		}
	})
	b.Run("subscription-delivery", func(b *testing.B) {
		channel, err := resolver.Subscription().NodeCreated(ctx, nil)
		if err != nil { b.Fatal(err) }
		node := &models.Node{ID: "read-bench"}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resolver.EventBroker.PublishNodeCreated(node)
			<-channel
		}
	})
}
