//go:build closeout_l2_e53_22

package cypher_test

// E53.22: a vector index whose population failed must stay FAILED after the
// search service is replaced (until it is dropped and created again).

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const l2E53_22Create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"
const l2E53_22Query = "CALL db.index.vector.queryNodes('docs', 10, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.id, score ORDER BY node.id"
const l2E53_22Show = "SHOW VECTOR INDEXES YIELD name,state,failureMessage RETURN name,state,failureMessage"

type l2E53_22Outcome struct {
	buildErr1, createErr     error
	stateBefore, msgBefore   string
	queryErrBefore           error
	buildErr2                error
	stateAfter, msgAfter     string
	queryErrAfter            error
	rowsAfter                [][]interface{}
	svcStateOld, svcStateNew string
	svcErrOld, svcErrNew     error
}

func l2E53_22Docs(t *testing.T, exec *cypher.StorageExecutor) (string, string) {
	t.Helper()
	res, err := exec.Execute(context.Background(), l2E53_22Show, nil)
	if err != nil {
		t.Fatalf("SHOW VECTOR INDEXES: %v", err)
	}
	for _, row := range res.Rows {
		if len(row) >= 3 && fmt.Sprint(row[0]) == "docs" {
			msg := ""
			if row[2] != nil {
				msg = fmt.Sprint(row[2])
			}
			return fmt.Sprint(row[1]), msg
		}
	}
	t.Fatalf("SHOW VECTOR INDEXES has no row for docs: %v", res.Rows)
	return "", ""
}

// l2E53_22Run: steps 1-7. withBudget selects the ending of step 6 (the new
// service with the 30-byte budget, or without a budget). failFirst=false is
// the control: the first service has no budget, so the population succeeds.
func l2E53_22Run(t *testing.T, failFirst, withBudget bool) l2E53_22Outcome {
	t.Helper()
	ctx := context.Background()
	base, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatalf("NewBadgerEngineInMemory: %v", err)
	}
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := cypher.NewStorageExecutor(store)
	if _, err := exec.Execute(ctx, "CREATE (:Doc {id:'a', emb:[1.0,0.0,0.0]}), (:Doc {id:'b', emb:[0.0,1.0,0.0]})", nil); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	var out l2E53_22Outcome
	var firstOpts *search.ServiceOptions
	if failFirst {
		firstOpts = &search.ServiceOptions{VectorMemoryMaxBytes: 30}
	}
	svc := search.NewService(store, 3, firstOpts)
	out.buildErr1 = svc.BuildIndexes(ctx)
	exec.SetSearchService(svc)

	_, out.createErr = exec.Execute(ctx, l2E53_22Create, nil)
	out.stateBefore, out.msgBefore = l2E53_22Docs(t, exec)
	_, out.queryErrBefore = exec.Execute(ctx, l2E53_22Query, nil)
	out.svcStateOld, out.svcErrOld = svc.VectorIndexState("docs")

	_ = svc.Close()
	var opts *search.ServiceOptions
	if withBudget {
		opts = &search.ServiceOptions{VectorMemoryMaxBytes: 30}
	}
	next := search.NewService(store, 3, opts)
	t.Cleanup(func() { _ = next.Close() })
	out.buildErr2 = next.BuildIndexes(ctx)
	exec.SetSearchService(next)
	out.svcStateNew, out.svcErrNew = next.VectorIndexState("docs")

	out.stateAfter, out.msgAfter = l2E53_22Docs(t, exec)
	res, err := exec.Execute(ctx, l2E53_22Query, nil)
	out.queryErrAfter = err
	if err == nil && res != nil {
		out.rowsAfter = res.Rows
	}
	return out
}

func (o l2E53_22Outcome) String() string {
	return fmt.Sprintf("build1_err=%v create_err=%v state_before=%s msg_before=%q query_err_before=%v build2_err=%v state_after=%s msg_after=%q query_err_after=%v rows_after=%v",
		o.buildErr1, o.createErr, o.stateBefore, o.msgBefore, o.queryErrBefore, o.buildErr2, o.stateAfter, o.msgAfter, o.queryErrAfter, o.rowsAfter)
}

func l2E53_22Parsers(t *testing.T, fn func(t *testing.T, parser string)) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			var restore func()
			if parser == config.ParserTypeANTLR {
				restore = config.WithANTLRParser()
			} else {
				restore = config.WithNornicParser()
			}
			defer restore()
			fn(t, parser)
		})
	}
}

func TestL2_e53_22_Repro(t *testing.T) {
	l2E53_22Parsers(t, func(t *testing.T, parser string) {
		for _, withBudget := range []bool{true, false} {
			ending := "new-service-30-byte-budget"
			if !withBudget {
				ending = "new-service-no-budget"
			}
			o := l2E53_22Run(t, true, withBudget)
			if o.stateBefore != search.VectorIndexFailed || o.queryErrBefore == nil {
				t.Fatalf("harness [%s %s]: the population did not fail as in step 4/5: %s", parser, ending, o)
			}
			if o.stateAfter != search.VectorIndexFailed {
				t.Logf("L2RESULT E53.22 DEFECT parser=%s ending=%s %s", parser, ending, o)
				t.Errorf("[%s %s] docs is %s after the service was replaced (want FAILED until dropped)", parser, ending, o.stateAfter)
				continue
			}
			t.Logf("L2RESULT E53.22 OK parser=%s ending=%s %s", parser, ending, o)
		}
	})
}

// Control: no budget on either service: the population succeeds, docs is
// ONLINE before and after the replacement and the query returns a and b.
func TestL2_e53_22_Control(t *testing.T) {
	l2E53_22Parsers(t, func(t *testing.T, parser string) {
		o := l2E53_22Run(t, false, false)
		t.Logf("control parser=%s %s", parser, o)
		if o.createErr != nil || o.stateBefore != search.VectorIndexOnline || o.queryErrBefore != nil ||
			o.stateAfter != search.VectorIndexOnline || o.queryErrAfter != nil || len(o.rowsAfter) != 2 {
			t.Errorf("control [%s]: want ONLINE before/after and 2 rows; got %s", parser, o)
		}
	})
}

// Cause: at the pin the population record lives only in the Service
// (vectorIndexPopulations, pkg/search/vector_index_population.go); a name
// without a record takes the new service's own build state
// (Service.VectorIndexState -> vectorIndexBuildState). This logs the
// service-level state of docs on the failed service and on its replacement
// for both endings: FAILED on the old one, the build's state on the new one.
func TestL2_e53_22_Cause(t *testing.T) {
	restore := config.WithNornicParser()
	defer restore()
	for _, withBudget := range []bool{true, false} {
		o := l2E53_22Run(t, true, withBudget)
		t.Logf("CAUSE withBudget=%v old_service_state=%s (%v) new_service_state=%s (%v) new_build_err=%v",
			withBudget, o.svcStateOld, o.svcErrOld, o.svcStateNew, o.svcErrNew, o.buildErr2)
	}
}
