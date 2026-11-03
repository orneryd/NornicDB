package convert

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/stretchr/testify/require"
)

func TestJSONFloat32BindingPaths(t *testing.T) {
	type envelope struct {
		Value   float32
		Dynamic any
	}
	paths := []struct {
		name   string
		decode func([]byte, *envelope) error
	}{
		{"syntax", func(raw []byte, dst *envelope) error { return jsontext.Unmarshal(raw, dst) }},
		{"number_stream", func(raw []byte, dst *envelope) error {
			return jsontext.NewNumberDecoder(bytes.NewReader(raw)).Decode(dst)
		}},
		{"property_document", func(raw []byte, dst *envelope) error { return PropertyJSON.Unmarshal(raw, dst) }},
		{"property_stream", func(raw []byte, dst *envelope) error {
			return PropertyJSON.NewDecoder(bytes.NewReader(raw)).Decode(dst)
		}},
		{"input_document", func(raw []byte, dst *envelope) error { return CypherInputJSON.Unmarshal(raw, dst) }},
		{"typed_document", func(raw []byte, dst *envelope) error {
			var typed struct{ Value float32 }
			err := PropertyJSON.Unmarshal(raw, &typed)
			dst.Value = typed.Value
			return err
		}},
	}
	for _, token := range []string{
		"3.4028235e38", "-3.4028235e38", "3.40282356e38", "3.40282357e38",
		"-3.40282357e38", "1e400", "1.0000000596046448", "-1.0000000596046448",
		"1.40129846e-45", "7.006492321624085e-46", "1e-50", "-0", "1.25", "null",
	} {
		raw := []byte(`{"Value":` + token + `}`)
		var want envelope
		wantErr := json.Unmarshal(raw, &want)
		for _, path := range paths {
			t.Run(path.name+"/"+token, func(t *testing.T) {
				var got envelope
				err := path.decode(raw, &got)
				if wantErr != nil {
					var standard, actual *json.UnmarshalTypeError
					require.ErrorAs(t, wantErr, &standard)
					require.ErrorAs(t, err, &actual)
					require.Equal(t, standard.Type, actual.Type)
					return
				}
				require.NoError(t, err)
				require.Equal(t, math.Float32bits(want.Value), math.Float32bits(got.Value))
			})
		}
	}
}
