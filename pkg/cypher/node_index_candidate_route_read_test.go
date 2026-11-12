package cypher

// Test-side reads of the deferred index candidate routes.
//
// The routes the canonical node candidate dispatch prepares
// (node_index_candidate_plan.go) resolve their candidates, and
// readNodeIndexCandidatePlan reads the nodes they name. Every branch test below
// resolves through the production resolver and then composes that read with the
// production reader through readResolvedCandidateNodes, so each route's
// recognition and admission is asserted exactly as the release's
// node-returning entry points reported them: no test re-implements a route, no
// test asserts less than it did, and no test owns a per-route entry that would
// have to be kept in step with the resolver behind it.
//
// The same composition covers a component's own seek: a component test plans
// its seek with the production planner and completes that answer through
// planCandidateSeekNodes (or readRankedCandidateSeekNodes, for a component
// whose seeks are ranked), so a case still asserts the component's eligibility,
// admission and seed shape rather than a router's choice.

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// readResolvedCandidateNodes reads the nodes a resolved route lists for
// nodePattern, or reports the route's own not-applicable answer.
func readResolvedCandidateNodes(exec *StorageExecutor, ctx context.Context, nodePattern nodePatternInfo, route nodeCandidateRoute, ids nodeIDResults, used bool, err error) ([]*storage.Node, bool, error) {
	if err != nil || !used {
		return nil, used, err
	}
	plan := nodeIndexCandidatePlan{route: route, ids: ids, pattern: nodePattern}
	nodes, _, _, err := exec.readNodeIndexCandidatePlan(ctx, plan)
	return nodes, used, err
}

// planCandidateSeekNodes reads the nodes one component's prepared point or
// inline seek lists. The component decides eligibility and plans its seek, and
// this composes the canonical preparation (planNodeIndexSeek) with the
// canonical read (readNodeIndexCandidatePlan) exactly as the dispatcher does
// for the seek it selected, so a test asserts its component's recognition and
// admission rather than a router's choice. A component that is not eligible, or
// that failed while preparing, reports its own answer unchanged.
func planCandidateSeekNodes(exec *StorageExecutor, ctx context.Context, seek nodeIndexSeek, planned bool, err error) ([]*storage.Node, bool, error) {
	if err != nil || !planned {
		return nil, false, err
	}
	plan, used, err := exec.planNodeIndexSeek(ctx, &seek, nodeIndexCandidatePlan{})
	return readCandidatePlanNodes(exec, ctx, plan, used, err)
}

// readRankedCandidateSeekNodes reads the nodes the best of one component's
// planned seeks lists: it ranks the seeks that component added
// (resolveRankedNodeIndexSeek) and reads the selected plan. used is false when
// the component planned no seek the index answers.
func readRankedCandidateSeekNodes(exec *StorageExecutor, ctx context.Context, seeks *nodeIndexSeekSet) ([]*storage.Node, bool, error) {
	plan, used, err := exec.resolveRankedNodeIndexSeek(ctx, seeks, nodeIndexCandidatePlan{})
	return readCandidatePlanNodes(exec, ctx, plan, used, err)
}

// readCandidatePlanNodes reads the nodes a prepared candidate plan lists, or
// reports the plan's own not-applicable answer or preparation error.
func readCandidatePlanNodes(exec *StorageExecutor, ctx context.Context, plan nodeIndexCandidatePlan, used bool, err error) ([]*storage.Node, bool, error) {
	if err != nil || !used {
		return nil, false, err
	}
	nodes, _, used, err := exec.readNodeIndexCandidatePlan(ctx, plan)
	return nodes, used, err
}
