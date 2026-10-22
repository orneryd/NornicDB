package cyphertext

import "testing"

// A quoted name reads back as exactly that name, so names that look like
// pattern syntax stay one name.
func TestQuoteNameAndNameText(t *testing.T) {
	for _, tc := range []struct{ name, quoted, text string }{
		{"Person", "`Person`", "Person"},
		{"A|B", "`A|B`", "`A|B`"},
		{"A:B", "`A:B`", "`A:B`"},
		{"a`b", "`a``b`", "`a``b`"},
		{"odd key", "`odd key`", "`odd key`"},
		{"", "``", "``"},
	} {
		if got := QuoteName(tc.name); got != tc.quoted {
			t.Errorf("QuoteName(%q) = %q, want %q", tc.name, got, tc.quoted)
		}
		if got := NameText(tc.name); got != tc.text {
			t.Errorf("NameText(%q) = %q, want %q", tc.name, got, tc.text)
		}
		if name, end, ok := ScanQuotedName(QuoteName(tc.name), 0); !ok || name != tc.name || end != len(QuoteName(tc.name)) {
			t.Errorf("ScanQuotedName(QuoteName(%q)) = %q, %d, %v", tc.name, name, end, ok)
		}
	}
}
