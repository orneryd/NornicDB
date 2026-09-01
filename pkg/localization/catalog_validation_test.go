package localization

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestValidateCatalogFiles(t *testing.T) {
	tests := []struct {
		name    string
		files   fstest.MapFS
		wantErr string
	}{
		{
			name: "valid complete target catalog",
			files: fstest.MapFS{
				"active.en-US.yaml": {Data: []byte("- id: greeting\n  other: 'Hello {{.Name}}'\n")},
				"active.es-ES.yaml": {Data: []byte("- id: greeting\n  other: 'Hola {{.Name}}'\n")},
			},
		},
		{
			name: "duplicate ID",
			files: fstest.MapFS{
				"active.en-US.yaml": {Data: []byte("- id: greeting\n  other: Hello\n- id: greeting\n  other: Again\n")},
			},
			wantErr: "duplicate message ID greeting",
		},
		{
			name: "placeholder mismatch",
			files: fstest.MapFS{
				"active.en-US.yaml": {Data: []byte("- id: greeting\n  other: 'Hello {{.Name}}'\n")},
				"active.es-ES.yaml": {Data: []byte("- id: greeting\n  other: 'Hola {{.User}}'\n")},
			},
			wantErr: "different template fields in other form",
		},
		{
			name: "placeholder count mismatch",
			files: fstest.MapFS{
				"active.en-US.yaml": {Data: []byte("- id: greeting\n  other: 'Hello {{.Name}} {{.Name}}'\n")},
				"active.es-ES.yaml": {Data: []byte("- id: greeting\n  other: 'Hola {{.Name}}'\n")},
			},
			wantErr: "different template fields in other form",
		},
		{
			name: "plural form mismatch",
			files: fstest.MapFS{
				"active.en-US.yaml": {Data: []byte("- id: count\n  one: '{{.Count}} item'\n  other: '{{.Count}} items'\n")},
				"active.es-ES.yaml": {Data: []byte("- id: count\n  other: '{{.Count}} elementos'\n")},
			},
			wantErr: "different plural forms",
		},
		{
			name: "missing source key",
			files: fstest.MapFS{
				"active.en-US.yaml": {Data: []byte("- id: first\n  other: First\n- id: second\n  other: Second\n")},
				"active.es-ES.yaml": {Data: []byte("- id: first\n  other: Primero\n")},
			},
			wantErr: "missing source message second",
		},
		{
			name: "non-canonical language tag",
			files: fstest.MapFS{
				"active.en-us.yaml": {Data: []byte("- id: greeting\n  other: Hello\n")},
			},
			wantErr: "invalid or non-canonical language tag",
		},
		{
			name: "duplicate across domain files",
			files: fstest.MapFS{
				"active.auth.en-US.yaml":   {Data: []byte("- id: greeting\n  other: Hello\n")},
				"active.server.en-US.yaml": {Data: []byte("- id: greeting\n  other: Again\n")},
			},
			wantErr: "duplicate message ID greeting across domain files",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			paths := make([]string, 0, len(test.files))
			for path := range test.files {
				paths = append(paths, path)
			}
			err := validateCatalogFiles(test.files, paths)
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
		})
	}
}
