package testfixture

// AlwaysEnabled is the shared capability of a fixture provider that is always
// enabled. Its zero value is ready for use, and it belongs first in an
// embedding struct so the zero-size capability stays out of any trailing
// padding.
type AlwaysEnabled struct{}

// Enabled reports that the embedding fixture provider is always enabled.
func (AlwaysEnabled) Enabled() bool { return true }
