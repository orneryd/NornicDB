package server

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/mcp"
)

// registerHeimdallMCPTools publishes opted-in actions after plugin initialization.
// Handlers retain their plugin's initialized HeimdallInvoker, including the local
// llama.cpp inference provider; MCP execution needs no second inference runtime.
func registerHeimdallMCPTools(server *mcp.Server, manager *heimdall.SubsystemManager, database heimdall.DatabaseRouter, metrics heimdall.MetricsReader, bifrost heimdall.BifrostBridge) {
	if server == nil {
		return
	}
	registered := make(map[string]bool)
	for _, action := range manager.ExternalActions() {
		schema := action.InputSchema
		if len(schema) == 0 {
			schema = heimdall.DefaultActionInputSchema
		}
		// RegisterTool logs collisions and retains the first registration.
		err := server.RegisterTool(mcp.Tool{Name: action.Name, Description: action.Description, InputSchema: schema}, func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			if action.Handler == nil {
				return nil, fmt.Errorf("action %s has no handler", action.Name)
			}
			return action.Handler(heimdall.ActionContext{
				Context: ctx, Params: args, Database: database, Metrics: metrics, Bifrost: bifrost,
				PrincipalRoles:     heimdall.PrincipalRolesFromContext(ctx),
				DatabaseAccessMode: heimdall.DatabaseAccessModeFromContext(ctx),
				ResolvedAccess:     heimdall.ResolvedAccessResolverFromContext(ctx),
			})
		})
		if err == nil {
			registered[action.Name] = true
		}
	}
	if len(registered) > 0 {
		manager.OnActionsRemoved(func(names []string) {
			for _, name := range names {
				if registered[name] {
					server.UnregisterTool(name)
				}
			}
		})
	}
}
