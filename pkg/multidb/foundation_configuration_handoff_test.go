// Author: Lane #1644 (Codex). Label routing is the same configuration handoff
// class as the storage query analyzer, exercised through both public decisions.
package multidb_test

import (
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFoundationLabelConfigurationHandoff(t *testing.T) {
	router := multidb.NewLabelRouting()
	routes := []string{"a"}
	router.SetLabelRouting("Doc", routes)
	routes[0] = "b"
	info := &multidb.QueryInfo{Labels: []string{"Doc"}}
	require.Equal(t, []string{"a"}, router.RouteQuery(info))
	require.Equal(t, "a", router.RouteWrite("create", info.Labels, nil))
	router.SetLabelRouting("Doc", []string{"b"})
	require.Equal(t, "b", router.RouteWrite("create", info.Labels, nil))
}
