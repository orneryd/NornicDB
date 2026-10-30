package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBackendVar_NonEmpty asserts that whichever build-tag file is active
// has set the package-level `backend` var to a non-empty string. CI matrix
// runs once per tag (-tags metal, -tags cuda, -tags vulkan, -tags noembed,
// and undecorated) so this test executes against all five variants.
//
// CONTEXT D-13a / D-13b: backend feeds the `nornicdb_build_info` const-label
// payload.
func TestBackendVar_NonEmpty(t *testing.T) {
	assert.NotEmpty(t, backend,
		"build-tag matrix incomplete: every variant must declare a non-empty `backend`")

	// Whichever variant is active, the value must be in the closed set.
	allowed := map[string]bool{
		"cpu":     true, // build_default.go (undecorated)
		"metal":   true, // build_metal.go
		"cuda":    true, // build_cuda.go
		"vulkan":  true, // build_vulkan.go
		"noembed": true, // build_noembed.go
	}
	assert.Truef(t, allowed[backend],
		"backend=%q is outside the closed Docker-variant set %v", backend, []string{
			"cpu", "metal", "cuda", "vulkan", "noembed",
		})
}
