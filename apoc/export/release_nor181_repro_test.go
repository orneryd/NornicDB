package apocexport

import (
	"encoding/xml"
	"io"
	"strings"
	"testing"
)

func TestReleaseNOR181GraphMLSpecialCharacters(t *testing.T) {
	s := GraphML([]*Node{{ID: 1, Properties: map[string]interface{}{"name": "a&b"}}}, nil)
	d := xml.NewDecoder(strings.NewReader(s))
	for {
		_, err := d.Token()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatalf("export is invalid XML: %v", err)
		}
	}
}
