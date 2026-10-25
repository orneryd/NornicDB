package label_test

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/label"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/stretchr/testify/require"
)

func TestLabelPatternEmitterProperty(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			names := []string{name, "Other"}
			text := label.Pattern(names)
			_, got, ok := cyphertext.StrictNodeHeadLabels("n"+text, false)
			require.True(t, ok, text)
			require.Equal(t, names, got)
			require.Equal(t, names, label.FromPattern(text))
		})
	}
}

func TestLabelPatternPresentation(t *testing.T) {
	require.Equal(t, ":Person:Employee", label.Pattern([]string{"Person", "Employee"}))
	require.Empty(t, label.Pattern(nil))
	require.Empty(t, label.FromPattern(""))
	require.Equal(t, "A|B", label.Format([]string{"A|B"}, "%s"), "generic formatting remains a text protocol")
}
