package nornicdb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameRetentionLabels(t *testing.T) {
	for _, name := range []string{" ", "  ", "\t"} {
		cfg := DefaultConfig()
		cfg.Retention.ExcludedLabels = []string{"", name}
		db := &DB{config: cfg}
		excluded := db.retentionExcludedLabels()
		require.Equal(t, map[string]struct{}{name: {}}, excluded)
		require.True(t, hasExcludedLabel(&storage.Node{Labels: []string{name}}, excluded))
		require.False(t, hasExcludedLabel(&storage.Node{Labels: []string{"Other"}}, excluded))
	}
}
