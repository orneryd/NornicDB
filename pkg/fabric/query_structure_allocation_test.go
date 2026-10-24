package fabric

import "testing"

func TestProjectionSyntaxWarmLookupDoesNotAllocate(t *testing.T) {
	const projection = "n.id AS id, n.name AS name"
	if ProjectionSyntax(projection) == nil {
		t.Fatal("valid projection did not parse")
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_ = ProjectionSyntax(projection)
	})
	if allocs != 0 {
		t.Fatalf("warm projection lookup allocated %.2f times per call; want 0", allocs)
	}
}
