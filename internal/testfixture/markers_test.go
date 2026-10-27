package testfixture

import (
	"fmt"
	"strings"
	"testing"
)

type markerAssertionFailure struct{ message string }

type markerAssertionRecorder struct{}

func (markerAssertionRecorder) Helper() {}
func (markerAssertionRecorder) Fatal(values ...any) {
	panic(markerAssertionFailure{message: fmt.Sprint(values...)})
}

func TestMarkerAssertionDoesNotDiscloseFailureInput(t *testing.T) {
	const secret = "synthetic-assertion-secret"
	defer func() {
		failure, ok := recover().(markerAssertionFailure)
		if !ok {
			t.Fatal("marker assertion did not reject leaked material")
		}
		if strings.Contains(failure.message, secret) || strings.Contains(failure.message, "private-output") {
			t.Fatal("assertion failure repeated private input")
		}
	}()
	RequireMarkersAbsent(markerAssertionRecorder{}, "private-output "+secret, secret)
}
