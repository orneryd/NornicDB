// Package mcp provides tool definitions and server for the NornicDB MCP (Model Context Protocol).

package mcp

import "context"

type databaseContextKey string

const (
	keyDatabase           databaseContextKey = "mcp:database"
	keyAuthorizedDatabase databaseContextKey = "mcp:authorized_database"
	keyURLDatabase        databaseContextKey = "mcp:url_database"
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

// contextWithURLDatabase carries the database pinned by the request URL path
// (/mcp/{database}/...) from routing to the HTTP handlers. The handlers inject
// it as the tool's database argument, so the payload database cannot override
// the URL pin.
func contextWithURLDatabase(ctx context.Context, dbName string) context.Context {
	if dbName == "" {
		return ctx
	}
	return context.WithValue(ctx, keyURLDatabase, dbName)
}

func urlDatabaseFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if s, ok := ctx.Value(keyURLDatabase).(string); ok {
		return s
	}
	return ""
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
