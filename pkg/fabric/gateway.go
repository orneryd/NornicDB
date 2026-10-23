package fabric

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

// QueryGateway is the shared Fabric entrypoint used by protocol adapters.
// It owns planning + fragment execution so Bolt/HTTP/GraphQL can share one path.
type QueryGateway struct {
	planner  *FabricPlanner
	executor *FabricExecutor
}

// NewQueryGateway creates a gateway from planner+executor dependencies.
func NewQueryGateway(planner *FabricPlanner, executor *FabricExecutor) *QueryGateway {
	return &QueryGateway{planner: planner, executor: executor}
}

// Execute plans and executes a Cypher query via Fabric semantics.
func (g *QueryGateway) Execute(
	ctx context.Context,
	tx *FabricTransaction,
	query string,
	sessionDB string,
	params map[string]interface{},
	authToken string,
	facts resultmetadata.Facts,
) (*ResultStream, error) {
	if g == nil || g.planner == nil || g.executor == nil {
		return nil, fmt.Errorf("fabric query gateway is not fully configured")
	}
	fragment, err := g.planner.Plan(query, sessionDB, facts)
	if err != nil {
		return nil, err
	}
	return g.executor.Execute(ctx, tx, fragment, params, authToken)
}
