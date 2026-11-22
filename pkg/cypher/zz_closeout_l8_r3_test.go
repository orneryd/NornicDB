// Close-out lane L8, round 3 (2026-11-22): owner run for L-H2-010. The lead's
// statements reach the expression evaluator's list forms
// (functions_eval_math.go:1120 evaluateQuantifierWithContext, :1188
// evaluateListComprehension), whose per-element closures evaluate through
// evaluateExpressionWithContextFull. Each form is called directly with a
// context that is already canceled; the reduce form through the same
// evaluator is the control. Test-only, not for merge.
package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestL8Owner_H2_010_LegacyListEvaluator(t *testing.T) {
	exec := l8Exec(t)
	params := map[string]interface{}{"items": l8Items(500000)}
	base := withParams(context.Background(), params)
	canceledCtx := func() context.Context {
		ctx, cancel := context.WithCancel(base)
		cancel()
		return ctx
	}
	measure := func(step string, call func(ctx context.Context) interface{}) {
		ctx := canceledCtx()
		start := time.Now()
		value := call(ctx)
		elapsed := time.Since(start)
		fields := map[string]any{"elapsed_ms": elapsed.Milliseconds(), "failure": fmt.Sprint(getExpressionFailure(ctx))}
		if list, ok := value.([]interface{}); ok {
			fields["result_list_len"] = len(list)
		} else {
			fields["value"] = fmt.Sprint(value)
		}
		l8Owner(t, "L-H2-010", step, fields)
	}
	measure("evaluateListComprehension projection (canceled ctx)", func(ctx context.Context) interface{} {
		ctx = withExpressionFailureSlot(ctx)
		return exec.evaluateListComprehension(ctx, "x", "$items", "", "size(randomUUID())", nil, nil, nil, nil, nil, 0)
	})
	measure("evaluateListComprehension filter (canceled ctx)", func(ctx context.Context) interface{} {
		ctx = withExpressionFailureSlot(ctx)
		return exec.evaluateListComprehension(ctx, "x", "$items", "size(randomUUID()) > 0", "", nil, nil, nil, nil, nil, 0)
	})
	measure("evaluateQuantifierWithContext any (canceled ctx)", func(ctx context.Context) interface{} {
		ctx = withExpressionFailureSlot(ctx)
		return exec.evaluateQuantifierWithContext(ctx, "any", "x IN $items WHERE size(randomUUID()) < 0", nil, nil, nil, nil, nil, 0)
	})
	measure("evaluateExpressionWithContextFull reduce (canceled ctx, control)", func(ctx context.Context) interface{} {
		ctx = withExpressionFailureSlot(ctx)
		return exec.evaluateExpressionWithContextFull(ctx, "reduce(a = 0, x IN $items | a + size(randomUUID()))", nil, nil, nil, nil, nil, 0)
	})
	measure("evaluateExpressionWithContextFull comprehension (canceled ctx)", func(ctx context.Context) interface{} {
		ctx = withExpressionFailureSlot(ctx)
		return exec.evaluateExpressionWithContextFull(ctx, "[x IN $items | size(randomUUID())]", nil, nil, nil, nil, nil, 0)
	})
}
