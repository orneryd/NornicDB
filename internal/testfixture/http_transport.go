package testfixture

import "net/http"

// RoundTripperFunc adapts a fixture callback to http.RoundTripper without a
// separate transport type. The callback owns request and response semantics.
type RoundTripperFunc func(*http.Request) (*http.Response, error)

// RoundTrip delegates the request unchanged to the fixture callback.
func (f RoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
