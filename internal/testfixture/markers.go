package testfixture

import "strings"

// RequireMarkersAbsent fails when text contains any supplied marker. It reports
// neither text nor markers, so credential-output tests cannot disclose the very
// values they detect. For example, pass a fixture's synthetic password markers.
func RequireMarkersAbsent(t interface {
	Helper()
	Fatal(...any)
}, text string, markers ...string) {
	t.Helper()
	for _, marker := range markers {
		if strings.Contains(text, marker) {
			t.Fatal("configured credential material reached output; output withheld")
		}
	}
}
