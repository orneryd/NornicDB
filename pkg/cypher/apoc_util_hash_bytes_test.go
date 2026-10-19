package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApocUtilHashByteArrays(t *testing.T) {
	for _, bundled := range []bool{false, true} {
		t.Run(fmt.Sprintf("bundled=%v", bundled), func(t *testing.T) {
			handlers := map[string]interface{}{}
			if bundled {
				handlers = bundledAPOCPluginFunctions(t)
			}
			withPluginFunctions(t, handlers)
			exec := newDigestExecutor(t)
			for _, tc := range []struct {
				name, expression, text string
				data                   []byte
			}{
				{"binary", "[$data]", "[0, 1, 127, -128, -1]", []byte{0, 1, 127, 128, 255}},
				{"empty", "[$data]", "[]", []byte{}},
				{"nested-list", "[[$data]]", "[[0, 1, 127, -128, -1]]", []byte{0, 1, 127, 128, 255}},
				{"nested-map", "[{data:$data}]", "{data=[0, 1, 127, -128, -1]}", []byte{0, 1, 127, 128, 255}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					params := map[string]interface{}{"data": tc.data, "text": tc.text}
					require.IsType(t, []byte{}, digestValue(t, exec, "RETURN $data", params))
					for _, algorithm := range []string{"md5", "sha1", "sha256"} {
						want := digestValue(t, exec, "RETURN apoc.util."+algorithm+"([$text])", params)
						if tc.name == "binary" && algorithm == "md5" {
							require.Equal(t, "dc234d8d2c22e88c3a7e1f69ab43379c", want)
						}
						suffixes := []string{""}
						if bundled {
							suffixes = append(suffixes, "Hex", "Base64")
						}
						for _, suffix := range suffixes {
							name := algorithm + suffix
							require.Equal(t, want, digestValue(t, exec, "RETURN apoc.util."+name+"("+tc.expression+")", params), name)
						}
					}
				})
			}
		})
	}
}
