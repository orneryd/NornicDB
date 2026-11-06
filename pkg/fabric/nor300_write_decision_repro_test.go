package fabric

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// NOR-300 reproduction (PR #1699): Fabric fragment write mode for a registered
// WRITE procedure (kp.suppress) must be a write.
func TestNOR300_FabricFragmentRegisteredWriteProcedure(t *testing.T) {
	q := "MATCH (n:Doc) CALL kp.suppress(n) RETURN n"
	catalog := NewCatalog()
	frag, err := NewFabricPlanner(catalog).Plan(t.Context(), q, "neo4j", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	exec, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("fragment is %T", frag)
	}
	if !exec.IsWrite {
		t.Fatalf("fragment for registered WRITE procedure has IsWrite=false")
	}
}
