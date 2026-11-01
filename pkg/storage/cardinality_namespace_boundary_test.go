package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A scoped cardinality count admits the images of one database, and a database
// owns exactly the "namespace:" prefix of an ID. edgeCardinalityCounter matches
// that boundary against the caller's namespace directly instead of preparing a
// "name:" key, so these tests pin the boundary itself: the owning prefix, a
// longer namespace that merely starts with it, the bare namespace with no
// separator, an empty namespace (unscoped), and non-ASCII namespaces. The final
// case requires the in-place check to agree with the "name:" prefix it replaces
// on every namespace/ID pair, which is the contract the two enforcement routes
// depend on; it is written against the counter's public surface only, so the same
// test runs unchanged against the prefix implementation it replaced.
func TestCardinalityCounterNamespaceBoundary(t *testing.T) {
	outgoing := Constraint{Type: ConstraintCardinality, Label: "OWNS", Direction: "OUTGOING", MaxCount: 2}
	probe := &Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:b", Type: "OWNS"}
	scoped := func(namespace string) edgeCardinalityCounter {
		return newEdgeCardinalityCounter(outgoing, probe, namespace, probe.ID)
	}

	cases := []struct {
		namespace string
		admitted  []EdgeID
		rejected  []EdgeID
	}{
		{
			namespace: "test",
			admitted:  []EdgeID{"test:e1", "test:", "test:0", "test:a:b"},
			rejected: []EdgeID{
				"test:new", // the edge under validation
				"test2:e1", // a longer namespace that starts with this one
				"test",     // the namespace without a separator
				"tes",      // a shorter namespace
				"",         // no namespace at all
				"Test:e1",  // namespaces are case-sensitive
				"teste1",   // the prefix without its boundary
				"other:e1",
				":test:e1",
			},
		},
		{
			namespace: "test2",
			admitted:  []EdgeID{"test2:e1", "test2:"},
			rejected:  []EdgeID{"test:e1", "test:new", "test2", "test", "test2x:e1"},
		},
		{
			namespace: "租户甲",
			admitted:  []EdgeID{"租户甲:e1", "租户甲:"},
			rejected:  []EdgeID{"租户甲", "租户甲x:e1", "租户:e1", "租户甲乙:e1", ""},
		},
		{
			namespace: "", // unscoped: every namespace's edges count
			admitted:  []EdgeID{"", "other:e1", "租户甲:e1", "test2:e1", "test:e1"},
			rejected:  []EdgeID{"test:new"},
		},
	}

	for _, testCase := range cases {
		testCase := testCase
		t.Run("namespace="+testCase.namespace, func(t *testing.T) {
			counter := scoped(testCase.namespace)
			for _, id := range testCase.admitted {
				require.True(t, counter.admits(id), "namespace %q admits %q", testCase.namespace, id)
			}
			for _, id := range testCase.rejected {
				require.False(t, counter.admits(id), "namespace %q must not admit %q", testCase.namespace, id)
			}
		})
	}
}

// The in-place boundary check must decide every namespace/ID pair exactly as the
// "namespace:" prefix it replaces did: an empty namespace is the empty prefix
// (unscoped), and a non-empty namespace is its own name plus the separator.
func TestCardinalityCounterNamespaceScopeMatchesPrefixContract(t *testing.T) {
	outgoing := Constraint{Type: ConstraintCardinality, Label: "OWNS", Direction: "OUTGOING", MaxCount: 2}
	probe := &Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:b", Type: "OWNS"}
	// An edge under validation outside the corpus keeps the exclusion rule out of
	// this comparison, so only the namespace scope is being decided.
	const excludeEdgeID EdgeID = "excluded:x"

	namespaces := []string{"", "a", "test", "test2", "test:", "a:b", "租户甲", "租户甲:", "é"}
	ids := []EdgeID{
		"", ":", "a", "a:", "a:b", "a:b:", "ab", "ab:", "a:bc",
		"test", "test:", "test:a", "test2", "test2:", "test2:a", "test2x:a", "teste1",
		"租户甲", "租户甲:", "租户甲:a", "租户甲x:a", "租户:a", "租户甲乙:a",
		"é", "é:", "é:a", "éx:a", "e:a",
	}
	for _, namespace := range namespaces {
		prefix := ""
		if namespace != "" {
			prefix = namespace + ":"
		}
		counter := newEdgeCardinalityCounter(outgoing, probe, namespace, excludeEdgeID)
		for _, id := range ids {
			require.Equal(t, strings.HasPrefix(string(id), prefix), counter.admits(id),
				"namespace %q on %q must match the %q prefix contract", namespace, id, prefix)
		}
	}
}
