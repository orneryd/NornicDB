package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameGraphFilters(t *testing.T) {
	for _, name := range []string{" ", "  ", "\t"} {
		filter := newGraphFilterSet([]string{"", name}, []string{"", name})
		matching := &storage.Node{Labels: []string{name}, Properties: map[string]interface{}{name: "allow"}}
		require.True(t, filter.allowNode(matching))
		require.False(t, filter.allowNode(&storage.Node{Labels: []string{"Other"}}))
		require.True(t, filter.allowEdge(&storage.Edge{Type: name}))
		require.False(t, filter.allowEdge(&storage.Edge{Type: "Other"}))
		value := "allow"
		property := newGraphPropertyFilter(graphPropertyFilterRequest{Property: name, Scope: name, Value: &value})
		require.NotNil(t, property)
		require.Equal(t, name, property.property)
		require.Equal(t, name, property.scope)
		included := newGraphFilterSet(nil, nil).withFilters(nil, nil, []graphPropertyFilterRequest{{Property: name, Scope: name, Value: &value}}, nil, nil, nil)
		require.True(t, included.allowNode(matching))
		require.False(t, included.allowNode(&storage.Node{Labels: []string{name}, Properties: map[string]interface{}{name: "deny"}}))
		excluded := newGraphFilterSet(nil, nil).withFilters(nil, nil, nil, nil, []string{name}, []string{name})
		require.True(t, excluded.hasExclusions())
		require.Contains(t, excluded.excludeLabels, name)
		require.Contains(t, excluded.excludeRelTypes, name)
	}
}
