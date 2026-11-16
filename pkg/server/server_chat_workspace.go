package server

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"regexp"
	"strings"
)

var assistantDatabaseName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func (r *heimdallDBRouter) ChatWorkspace(ctx context.Context, createName *string) (heimdall.ChatWorkspace, error) {
	identity := auth.RequestIdentityFromContext(ctx)
	if identity.ID == "" || identity.ID == "anonymous" || identity.Username == "" {
		return heimdall.ChatWorkspace{}, heimdall.ErrChatAuthenticationRequired
	}
	if r.dbManager == nil {
		return heimdall.ChatWorkspace{}, fmt.Errorf("database manager unavailable")
	}
	suggested := identity.Username + "-chats"
	workspace := heimdall.ChatWorkspace{SuggestedDatabase: suggested, Username: identity.Username}
	if existing := r.dbManager.PrivateWorkspace(identity.ID); existing != nil {
		workspace.Configured = true
		workspace.Database = existing.Name
		return workspace, nil
	}
	if createName == nil {
		return workspace, nil
	}
	name := strings.TrimSpace(*createName)
	if !assistantDatabaseName.MatchString(name) {
		return workspace, fmt.Errorf("use 1–128 letters, digits, periods, underscores or hyphens for the chat database name")
	}
	if err := r.dbManager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: name, PrivateOwner: identity.ID}); err != nil {
		return workspace, err
	}
	workspace.Configured = true
	workspace.Database = name
	return workspace, nil
}

// chatContext grants only the owner-authorized private database to plugin CRUD.
// It does not modify the caller's context or grant graph access to other tools.
func (r *heimdallDBRouter) chatContext(ctx context.Context) (context.Context, string, error) {
	workspace, err := r.ChatWorkspace(ctx, nil)
	if err != nil {
		return nil, "", err
	}
	if !workspace.Configured {
		return nil, "", heimdall.ErrChatWorkspaceMissing
	}
	name := workspace.Database
	scope := auth.NewRequestDatabaseScopeWithSelectionCheck(name, map[string]string{name: name}, func(database string) bool { return database == name })
	grant := func(database string, permission auth.Permission) bool {
		return database == name && (permission == auth.PermRead || permission == auth.PermWrite || permission == auth.Permission("access"))
	}
	ctx = auth.WithRequestPrincipal(ctx, grant, scope)
	ctx = auth.WithRequestDatabaseAccessMode(ctx, chatWorkspaceMode{name: name})
	ctx = auth.WithRequestResolvedAccessResolver(ctx, func(database string) auth.ResolvedAccess {
		return auth.ResolvedAccess{Read: database == name, Write: database == name}
	})
	return ctx, name, nil
}

type chatWorkspaceMode struct{ name string }

func (m chatWorkspaceMode) CanSeeDatabase(database string) bool    { return database == m.name }
func (m chatWorkspaceMode) CanAccessDatabase(database string) bool { return database == m.name }
func (r *heimdallDBRouter) ChatQuery(ctx context.Context, query string, params map[string]interface{}) ([]map[string]interface{}, error) {
	scoped, database, err := r.chatContext(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.Query(scoped, database, query, params)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = make([]map[string]interface{}, 0)
	}
	return rows, nil
}
func (r *heimdallDBRouter) ChatDiscover(ctx context.Context, query string, limit int) (*heimdall.DiscoverResult, error) {
	scoped, database, err := r.chatContext(ctx)
	if err != nil {
		return nil, err
	}
	return r.Discover(scoped, database, query, []string{"AssistantMessage"}, limit, 1)
}
