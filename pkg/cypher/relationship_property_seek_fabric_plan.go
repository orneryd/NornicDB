package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

// Reuse the graph planner's fragment/database selections. This discovers no
// bindings and runs no fragment, procedure or remote request. A target whose
// schema is selected at execution remains an explicit deferred access owner.
func (e *StorageExecutor) relationshipFabricAccessSites(ctx context.Context, query string) []*relationshipAccessSite {
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
			for _, site := range scoped.relationshipLocalAccessSites(ctx, current.Query) {
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
