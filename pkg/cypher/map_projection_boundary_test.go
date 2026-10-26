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
		binding     string
	}{
		{" n{ .x } ", mapProjectionRuntime, "n", ".x", true, ""},
		{" n{ .x } ", mapProjectionStatic, "", "", false, ""},
		{"n{ .x }", mapProjectionStatic, "n", " .x ", true, ""},
		{"`a b`{.x}", mapProjectionRuntime, "`a b`", ".x", true, ""},
		{"`a b`{.x}", mapProjectionStatic, "a b", ".x", true, ""},
		{"COUNT { MATCH (n) RETURN n }", mapProjectionRuntime, "", "", false, ""},
		{"COUNT { MATCH (n) RETURN n }", mapProjectionStatic, "", "", false, ""},
		{"n{v: {inner: '}'}}", mapProjectionRuntime, "n", "v: {inner: '}'}", true, ""},
		{"n{v: {inner: '}'}}", mapProjectionStatic, "n", "v: {inner: '}'}", true, ""},
		{"n{.x} + 1", mapProjectionRuntime, "", "", false, ""},
		{"n{.x} + 1", mapProjectionStatic, "", "", false, ""},
		{"{x:1}", mapProjectionRuntime, "", "", false, ""},
		{"{x:1}", mapProjectionStatic, "", "", false, ""},
		{"skip{.x}", mapProjectionStatic, "skip", ".x", true, "skip"},
		{"skip{.x}", mapProjectionStatic, "", "", false, "other"},
		{"n{ .x }", mapProjectionStatic, "n", " .x ", true, "other"},
		{"`skip`{.x}", mapProjectionStatic, "skip", ".x", true, "skip"},
		{"COUNT { MATCH (n) RETURN n }", mapProjectionStatic, "", "", false, "COUNT"},
		{"skip{.x} + 1", mapProjectionStatic, "", "", false, "skip"},
	} {
		t.Run(tc.text+map[mapProjectionPolicy]string{mapProjectionRuntime: "/runtime", mapProjectionStatic: "/static"}[tc.policy], func(t *testing.T) {
			var scope *staticTypeScope
			if tc.binding != "" {
				scope = &staticTypeScope{values: map[string]string{tc.binding: "Map"}}
			}
			name, items, ok := mapProjectionSplit(originalBraceView(tc.text), tc.policy, scope)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.name, name)
			require.Equal(t, tc.items, items)
		})
	}
}
