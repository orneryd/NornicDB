// Package mcp provides tool definitions and server for the NornicDB MCP (Model Context Protocol).

package mcp

import "context"

type databaseContextKey string

const (
	keyDatabase           databaseContextKey = "mcp:database"
	keyAuthorizedDatabase databaseContextKey = "mcp:authorized_database"
)

// ContextWithDatabase returns a context that carries the database name for MCP tool execution.
// When the agentic loop calls MCP tools in process, the handler should set this so store/recall/link
// run against the request's database (e.g. lifecycle.database.DefaultDatabaseName()).
func ContextWithDatabase(ctx context.Context, dbName string) context.Context {
	if dbName == "" {
		return ctx
	}
	return context.WithValue(ctx, keyDatabase, dbName)
}

func contextWithAuthorizedDatabase(ctx context.Context, dbName string) context.Context {
	ctx = ContextWithDatabase(ctx, dbName)
	return context.WithValue(ctx, keyAuthorizedDatabase, true)
}

func hasAuthorizedDatabase(ctx context.Context) bool {
	authorized, _ := ctx.Value(keyAuthorizedDatabase).(bool)
	return authorized
}

// DatabaseFromContext returns the database name from the context, or empty if not set.
func DatabaseFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v := ctx.Value(keyDatabase)
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
