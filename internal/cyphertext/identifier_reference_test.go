package cyphertext

import "testing"

func TestIdentifierReferencePolicies(t *testing.T) {
	for _, tc := range []struct {
		label, text, name string
		from, want        int
		policy            IdentifierReferencePolicy
	}{
		{"exact-property", "n.x", "n", 0, 0, IdentifierExact},
		{"unqualified-property", "n.x", "n", 0, -1, IdentifierUnqualifiedFold},
		{"unqualified-later", "x.n + n", "n", 0, 6, IdentifierUnqualifiedFold},
		{"folded-name", "N + 1", "`n`", 0, 0, IdentifierUnqualifiedFold},
		{"exact-case", "N + 1", "n", 0, -1, IdentifierExact},
		{"quoted-name", "`n` + 1", "n", 0, -1, IdentifierFold},
		{"literal-skipped", "'n' + n", "n", 0, 6, IdentifierFold},
		{"comment-skipped", "// n\nN", "n", 0, 5, IdentifierFold},
		{"unicode-boundary", "猫n", "n", 0, -1, IdentifierFold},
		{"anchored-hit", "x + n", "n", 4, 4, IdentifierFoldAt},
		{"anchored-no-search", "x + n", "n", 0, -1, IdentifierFoldAt},
		{"anchored-lexical-owner", "'n'", "n", 1, 1, IdentifierFoldAt},
		{"empty-name", "x", "", 0, -1, IdentifierFold},
		{"negative-offset", "n", "n", -1, -1, IdentifierFoldAt},
	} {
		t.Run(tc.label, func(t *testing.T) {
			if got := FindIdentifier(tc.text, tc.name, tc.from, tc.policy); got != tc.want {
				t.Fatalf("reference offset = %d, want %d", got, tc.want)
			}
		})
	}
}
