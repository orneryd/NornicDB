// Author: Lane #1644 (Codex). Composite label configuration currently has no
// public routing consumer. This checks its handoff, without inventing routing.
package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFoundationCompositeConfigurationRetention(t *testing.T) {
	view := NewCompositeEngine(nil, nil, nil)
	routes := []string{"a"}
	view.SetLabelRouting("Doc", routes)
	routes[0] = "b"
	require.Equal(t, []string{"a"}, view.labelRouting["doc"])
}
