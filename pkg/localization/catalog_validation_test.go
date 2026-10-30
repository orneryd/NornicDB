package localization

import (
	"testing"
	"testing/fstest"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func TestGraphDirectionCatalogRendering(t *testing.T) {
	paths := []string{
		"catalog/active.graph.en-US.yaml",
		"catalog/active.graph.es-ES.yaml",
		"catalog/active.graph.en-XA.yaml",
	}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}
	message := GraphDirectionInvalid()
	for _, test := range []struct {
		locale string
		want   string
	}{
		{"en-US", "direction must be one of 'out', 'in' or 'both'"},
		{"es-ES", "direction debe ser uno de 'out', 'in' o 'both'"},
		{"en-XA", "[!! direction must be one of 'out', 'in' or 'both' !!]"},
	} {
		t.Run(test.locale, func(t *testing.T) {
			text, err := i18n.NewLocalizer(bundle, test.locale).Localize(&i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data})
			require.NoError(t, err)
			require.Equal(t, test.want, text)
		})
	}
}

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
