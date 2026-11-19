package server

// Fixture helpers for the historical prune probe.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func hl3Bulk(engine storage.Engine, nodes []*storage.Node) error {
	return engine.BulkCreateNodes(context.Background(), nodes)
}

func hl3CreateUser(a *auth.Authenticator, name string) error {
	_, err := a.CreateUser(context.Background(), name, "password123", []auth.Role{auth.RoleAdmin})
	return err
}

func hl3Setup(t testing.TB, configureDB func(*nornicdb.Config)) (*Server, *auth.Authenticator) {
	t.Helper()
	return setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
		c.Database.MVCCRetentionMaxVersions = 100
		if configureDB != nil {
			configureDB(c)
		}
	}, func(c *Config) { c.EmbeddingEnabled = false })
}
