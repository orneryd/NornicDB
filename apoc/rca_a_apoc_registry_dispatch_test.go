package apoc

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-apoc-registry-dispatch).
// Test-side only. apoc.Call (apoc.go:198-218) always invokes with
// registry.APOCArguments; Callable.argumentPlan admits that policy only for a
// signature listed in the fixed table apocArgumentPlans
// (registry/invocation_policy.go:38-62, lookup at :76-78). The diagnostic logs
// the decisive signature, shows the same Callable runs under GoArguments, and
// counts the registered callables whose signature the APOC table lacks.

import (
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
)

func TestRCAAPOCArgumentPolicyTable(t *testing.T) {
	mu.RLock()
	reg := active
	mu.RUnlock()
	if reg == nil {
		t.Fatal("RCA_APOC registry not published")
	}
	fns := reg.functions

	for _, name := range []string{"apoc.import.cypherData", "apoc.cypher.runMany"} {
		fn, ok := fns[name]
		if !ok {
			t.Logf("RCA_APOC %s not registered", name)
			continue
		}
		_, apocErr := fn.Handler.CallWithPolicy([]interface{}{"RETURN 1", map[string]interface{}{}}, nil, registry.APOCArguments)
		t.Logf("RCA_APOC %s signature=%s APOCArguments: invoked=%v err=%v", name, fn.Handler.Signature(), registry.InvocationStarted(apocErr), apocErr)
	}
	if fn, ok := fns["apoc.import.cypherData"]; ok {
		value, goErr := fn.Handler.CallWithPolicy([]interface{}{"CREATE (:RCA)", map[string]interface{}{}}, nil, registry.GoArguments)
		t.Logf("RCA_APOC corrected policy (declared-type binding, GoArguments) apoc.import.cypherData: invoked=%v value=%T %+v err=%v", registry.InvocationStarted(goErr) || goErr == nil, value, value, goErr)
	}

	// Census: a call with no arguments is refused before invocation either by
	// the policy table ("cannot express") or by the arity check.
	refused := map[string][]string{}
	total := 0
	for name, fn := range fns {
		if fn.Handler == nil || fn.Handler.Signature() == nil || fn.Handler.Signature().NumIn() == 0 {
			continue
		}
		total++
		_, err := fn.Handler.CallWithPolicy(nil, nil, registry.APOCArguments)
		if err != nil && strings.Contains(err.Error(), "argument policy cannot express") {
			sig := fn.Handler.Signature().String()
			refused[sig] = append(refused[sig], name)
		}
	}
	sigs := make([]string, 0, len(refused))
	count := 0
	for sig, names := range refused {
		sigs = append(sigs, sig)
		count += len(names)
	}
	sort.Strings(sigs)
	t.Logf("RCA_APOC_CENSUS registered_with_args=%d refused_by_APOC_policy=%d distinct_signatures=%d", total, count, len(sigs))
	for _, sig := range sigs {
		names := refused[sig]
		sort.Strings(names)
		if len(names) > 6 {
			names = append(names[:6], "...")
		}
		t.Logf("RCA_APOC_CENSUS %s n=%d e.g. %v", sig, len(refused[sig]), names)
	}
}
