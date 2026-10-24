package resultmetadata

import "testing"

func TestNamesKnownUnknownAndCopies(t *testing.T) {
	source := []string{"value"}
	value := Known(source)
	source[0] = "changed"
	for i := 0; i < 2; i++ {
		value.Visit(func(names []string) {
			if len(names) != 1 || names[0] != "value" {
				t.Fatalf("unexpected names: %v", names)
			}
			names[0] = "mutated"
		}, func() { t.Fatal("known became unknown") })
	}
	Known(nil).Visit(func(names []string) {
		if names == nil || len(names) != 0 {
			t.Fatalf("known empty: %#v", names)
		}
	}, func() { t.Fatal("empty became unknown") })
	unknown := false
	Unknown().Visit(func([]string) { t.Fatal("unknown exposed names") }, func() { unknown = true })
	if !unknown {
		t.Fatal("unknown branch was not visited")
	}
}
