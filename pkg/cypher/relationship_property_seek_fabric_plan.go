package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

// Reuse execution's graph selection: ordinary USE selects a local storage view;
// composite USE consumes the Fabric planner's fragments. Neither route executes
// a fragment, procedure or remote request to describe its relationship access.
func (e *StorageExecutor) relationshipSelectedGraphAccessSites(statement admittedRelationshipStatement, ctx context.Context, query string) []*relationshipAccessSite {
	if !e.shouldUseFabricPlanner(query) {
		use, body, hasUse, err := parseUseClause(query, true)
		if err != nil || !hasUse || e.dynamicUseError(use) != nil {
			return nil
		}
		scoped, selected, err := e.useDatabase(ctx, use.Name, body)
		if err != nil {
			return nil
		}
		sites := scoped.relationshipLocalAccessSites(statement, selected, body)
		for _, site := range sites {
			site.graph = GetUseDatabaseFromContext(selected)
		}
		return sites
	}
	prepared, err := e.prepareFabricExecution(ctx, query)
	if err != nil {
		return nil
	}
	var sites []*relationshipAccessSite
	var collect func(fabric.Fragment)
	collect = func(fragment fabric.Fragment) {
		switch current := fragment.(type) {
		case *fabric.FragmentExec:
			collect(current.Input)
			if !containsRelationshipPattern(current.Query) {
				return
			}
			if current.Graph != nil && current.Graph.IsDynamic() {
				sites = append(sites, &relationshipAccessSite{graph: current.Graph.Text(), deferred: "DynamicGraphAccess"})
				return
			}
			location, err := prepared.catalog.Resolve(current.GraphName)
			if err != nil {
				return
			}
			local, ok := location.(*fabric.LocationLocal)
			if !ok {
				sites = append(sites, &relationshipAccessSite{graph: current.GraphName, deferred: "RemoteQuery"})
				return
			}
			if e.authorizeSelectedDatabase(ctx, current.GraphName, current.Query) != nil {
				return
			}
			scoped, _, err := e.scopedExecutorForUse(local.DBName, GetAuthTokenFromContext(ctx))
			if err != nil {
				return
			}
			for _, site := range scoped.relationshipLocalAccessSites(statement, ctx, current.Query) {
				site.graph = local.DBName
				sites = append(sites, site)
			}
		case *fabric.FragmentApply:
			collect(current.Input)
			collect(current.Inner)
		case *fabric.FragmentUnion:
			collect(current.LHS)
			collect(current.RHS)
		case *fabric.FragmentLeaf:
			collect(current.Input)
		}
	}
	collect(prepared.fragment)
	return sites
}
