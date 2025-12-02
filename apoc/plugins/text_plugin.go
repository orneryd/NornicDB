package plugins

import (
	"github.com/orneryd/nornicdb/apoc/plugin"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/apoc/text"
)

// TextPlugin provides text processing functions.
type TextPlugin struct {
	store storage.Storage
}

// NewTextPlugin creates a new text plugin.
func NewTextPlugin() plugin.Plugin {
	return &TextPlugin{}
}

func (p *TextPlugin) Name() string {
	return "text"
}

func (p *TextPlugin) Version() string {
	return "1.0.0"
}

func (p *TextPlugin) Description() string {
	return "Text processing and string manipulation functions"
}

func (p *TextPlugin) Initialize(store storage.Storage) error {
	p.store = store
	return nil
}

func (p *TextPlugin) Cleanup() error {
	return nil
}

func (p *TextPlugin) Register(reg *registry.FunctionRegistry) error {
	functions := []struct {
		name        string
		fn          interface{}
		description string
		examples    []string
	}{
		{
			"apoc.text.join",
			text.Join,
			"Join strings with a delimiter",
			[]string{"apoc.text.join(['Hello', 'World'], ' ') => 'Hello World'"},
		},
		{
			"apoc.text.split",
			text.Split,
			"Split a string by delimiter",
			[]string{"apoc.text.split('Hello World', ' ') => ['Hello', 'World']"},
		},
		{
			"apoc.text.replace",
			text.Replace,
			"Replace all occurrences of a substring",
			[]string{"apoc.text.replace('Hello World', 'World', 'Universe') => 'Hello Universe'"},
		},
		{
			"apoc.text.capitalize",
			text.Capitalize,
			"Capitalize first letter of each word",
			[]string{"apoc.text.capitalize('hello world') => 'Hello World'"},
		},
		{
			"apoc.text.camelCase",
			text.CamelCase,
			"Convert to camelCase",
			[]string{"apoc.text.camelCase('hello world') => 'helloWorld'"},
		},
		{
			"apoc.text.snakeCase",
			text.SnakeCase,
			"Convert to snake_case",
			[]string{"apoc.text.snakeCase('HelloWorld') => 'hello_world'"},
		},
		{
			"apoc.text.distance",
			text.Distance,
			"Calculate Levenshtein distance",
			[]string{"apoc.text.distance('kitten', 'sitting') => 3"},
		},
		{
			"apoc.text.jaroWinklerDistance",
			text.JaroWinklerDistance,
			"Calculate Jaro-Winkler similarity",
			[]string{"apoc.text.jaroWinklerDistance('martha', 'marhta') => 0.96"},
		},
		{
			"apoc.text.phonetic",
			text.Phonetic,
			"Get phonetic encoding (Soundex)",
			[]string{"apoc.text.phonetic('Smith') => 'S530'"},
		},
		{
			"apoc.text.slug",
			text.Slug,
			"Convert to URL-friendly slug",
			[]string{"apoc.text.slug('Hello World!') => 'hello-world'"},
		},
	}
	
	for _, f := range functions {
		if err := reg.RegisterFunction(f.name, p.Name(), f.fn, f.description, f.examples); err != nil {
			return err
		}
	}
	
	return nil
}
