package cypher

import (
	"github.com/orneryd/nornicdb/pkg/fabric"
	"sync"
)

// preparedFabricGraph retains the planned graph's native selection and catalog
// identity. Admission, cache eligibility and dispatch consume this one view.
type preparedFabricGraph struct {
	execution *fabric.FragmentExec
	location  fabric.Location
	dynamic   bool
}

// describeFabricGraphs is the one graph inventory of a prepared invocation.
// It reads planned typed selections, never raw Cypher operands or USE text.
func describeFabricGraphs(catalog *fabric.Catalog, fragment fabric.Fragment) (graphs []preparedFabricGraph, remote bool, err error) {
	if catalog == nil {
		return nil, false, nil
	}
	var visit func(fabric.Fragment)
	visit = func(fragment fabric.Fragment) {
		if err != nil {
			return
		}
		switch current := fragment.(type) {
		case *fabric.FragmentExec:
			visit(current.Input)
			if err != nil {
				return
			}
			graph := preparedFabricGraph{execution: current, dynamic: current.Graph != nil && current.Graph.IsDynamic()}
			if !graph.dynamic {
				graph.location, err = catalog.Resolve(current.GraphName)
				if err != nil {
					return
				}
				_, isRemote := graph.location.(*fabric.LocationRemote)
				remote = remote || isRemote
			}
			graphs = append(graphs, graph)
		case *fabric.FragmentApply:
			visit(current.Input)
			visit(current.Inner)
		case *fabric.FragmentUnion:
			visit(current.LHS)
			visit(current.RHS)
		case *fabric.FragmentLeaf:
			visit(current.Input)
		}
	}
	visit(fragment)
	return
}

// fabricGraphAdmission is invocation-owned, not part of a cached plan. It
// applies the existing transaction policy to each selected graph once. Static
// selections finish before cache/shortcut execution; dynamic selections finish
// at the existing graph-selection callback before any participant opens.
type fabricGraphAdmission struct {
	database, statement string
	databases           TransactionDatabases
	catalog             *fabric.Catalog
	mu                  sync.Mutex
	accepted            map[string]struct{}
}

func (admission *fabricGraphAdmission) admit(graph string, location fabric.Location) error {
	if admission == nil {
		return nil
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if _, ok := admission.accepted[graph]; ok {
		return nil
	}
	if location == nil {
		var err error
		location, err = admission.catalog.Resolve(graph)
		if err != nil {
			return fabricGraphReferenceError(err)
		}
	}
	target := graph
	// A composite transaction owns its logical constituent scope. An ordinary
	// transaction compares local physical database identities, including aliases.
	// A remote database with the same name is never that local transaction.
	composite := admission.databases != nil && admission.databases.IsCompositeDatabase(admission.database)
	if local, ok := location.(*fabric.LocationLocal); ok && !composite {
		target = local.DBName
	}
	if err := TransactionDatabaseError(admission.databases, admission.database, target, admission.statement); err != nil {
		return err
	}
	admission.accepted[graph] = struct{}{}
	return nil
}
