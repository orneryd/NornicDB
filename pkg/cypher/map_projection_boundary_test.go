package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMapProjectionBoundaryPolicies(t *testing.T) {
	for _, tc := range []struct {
		text        string
		policy      mapProjectionPolicy
		name, items string
		ok          bool
	}{
		{" n{ .x } ", mapProjectionRuntime, "n", ".x", true},
		{" n{ .x } ", mapProjectionStatic, "", "", false},
		{"n{ .x }", mapProjectionStatic, "n", " .x ", true},
		{"`a b`{.x}", mapProjectionRuntime, "`a b`", ".x", true},
		{"`a b`{.x}", mapProjectionStatic, "a b", ".x", true},
		{"COUNT { MATCH (n) RETURN n }", mapProjectionRuntime, "", "", false},
		{"COUNT { MATCH (n) RETURN n }", mapProjectionStatic, "", "", false},
		{"n{v: {inner: '}'}}", mapProjectionRuntime, "n", "v: {inner: '}'}", true},
		{"n{v: {inner: '}'}}", mapProjectionStatic, "n", "v: {inner: '}'}", true},
		{"n{.x} + 1", mapProjectionRuntime, "", "", false},
		{"n{.x} + 1", mapProjectionStatic, "", "", false},
		{"{x:1}", mapProjectionRuntime, "", "", false},
		{"{x:1}", mapProjectionStatic, "", "", false},
	} {
		t.Run(tc.text+map[mapProjectionPolicy]string{mapProjectionRuntime: "/runtime", mapProjectionStatic: "/static"}[tc.policy], func(t *testing.T) {
			name, items, ok := mapProjectionSplit(tc.text, tc.policy)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.name, name)
			require.Equal(t, tc.items, items)
		})
	}
}
