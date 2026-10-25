package storagecensus

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataLiteralKeySpaces(t *testing.T) {
	k := &keys{metaByte: 0xf0}
	for _, text := range []string{"wal-property-identity", "wal-property-foreign", "wal-property-clean", "wal-property-clean-unreviewed", "accessmeta:user"} {
		t.Run(text, func(t *testing.T) {
			name, parameter, why := k.space(keyShape{bytes: []byteSrc{{known: true, val: k.metaByte}, {str: text}}})
			require.Equal(t, -1, parameter)
			if text == "wal-property-clean-unreviewed" {
				require.Empty(t, name)
				require.NotEmpty(t, why, "new metadata keys must still require classification")
				return
			}
			require.Empty(t, why)
			if text == "accessmeta:user" {
				require.Equal(t, `prefixMVCCMeta+"accessmeta:"`, name)
				require.Equal(t, "access-metadata", spaceClasses[name])
			} else {
				require.Equal(t, `prefixMVCCMeta+"`+text+`"`, name)
				require.Equal(t, "store-metadata", spaceClasses[name])
			}
		})
	}
	require.True(t, orderedClasses[spaceClasses["prefixMVCCMeta"]], "whole metadata-family writes remain ordered")
}
