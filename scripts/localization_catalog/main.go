// Command localization_catalog validates typed message constructors against the
// source catalog and emits a deterministic catalog manifest.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

var templateFieldPattern = regexp.MustCompile(`\{\{\s*\.([A-Za-z][A-Za-z0-9_]*)`)

type catalogEntry struct {
	ID    string `yaml:"id"`
	Zero  string `yaml:"zero"`
	One   string `yaml:"one"`
	Two   string `yaml:"two"`
	Few   string `yaml:"few"`
	Many  string `yaml:"many"`
	Other string `yaml:"other"`
}

type manifestEntry struct {
	ID          string
	Constructor string
	Fields      []string
	Forms       []string
}

func main() {
	root := flag.String("root", ".", "repository root")
	out := flag.String("out", "pkg/localization/catalog_manifest_gen.go", "generated manifest path")
	check := flag.Bool("check", false, "fail when the generated manifest differs from -out")
	flag.Parse()

	absRoot, err := filepath.Abs(*root)
	fatalIf(err)
	entries, err := loadSourceCatalog(filepath.Join(absRoot, "pkg/localization/catalog"))
	fatalIf(err)
	constants, constructors, err := loadGoAPI(filepath.Join(absRoot, "pkg/localization"))
	fatalIf(err)
	manifest, err := validate(entries, constants, constructors)
	fatalIf(err)
	generated, err := render(manifest)
	fatalIf(err)

	outPath := *out
	if !filepath.IsAbs(outPath) {
		outPath = filepath.Join(absRoot, outPath)
	}
	if *check {
		current, err := os.ReadFile(outPath)
		fatalIf(err)
		if !bytes.Equal(current, generated) {
			fatalf("localization catalog manifest drift: run go generate ./pkg/localization")
		}
		return
	}
	fatalIf(os.WriteFile(outPath, generated, 0o644))
}

func loadSourceCatalog(dir string) (map[string]catalogEntry, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "active*en-US.yaml"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	entries := make(map[string]catalogEntry)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var fileEntries []catalogEntry
		if err := yaml.Unmarshal(data, &fileEntries); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		for _, entry := range fileEntries {
			if _, exists := entries[entry.ID]; exists {
				return nil, fmt.Errorf("duplicate source catalog ID %s", entry.ID)
			}
			entries[entry.ID] = entry
		}
	}
	return entries, nil
}

func loadGoAPI(dir string) (map[string]string, map[string][]string, error) {
	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, dir, func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go") && info.Name() != "catalog_manifest_gen.go"
	}, 0)
	if err != nil {
		return nil, nil, err
	}
	pkg := packages["localization"]
	if pkg == nil {
		return nil, nil, fmt.Errorf("localization package not found")
	}
	constants := make(map[string]string)
	for _, file := range pkg.Files {
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || general.Tok != token.CONST {
				continue
			}
			for _, spec := range general.Specs {
				valueSpec, ok := spec.(*ast.ValueSpec)
				if !ok || len(valueSpec.Names) != len(valueSpec.Values) {
					continue
				}
				for index, name := range valueSpec.Names {
					literal, ok := valueSpec.Values[index].(*ast.BasicLit)
					if !ok || literal.Kind != token.STRING || !strings.HasPrefix(name.Name, "Message") {
						continue
					}
					value, err := strconv.Unquote(literal.Value)
					if err != nil {
						return nil, nil, err
					}
					constants[name.Name] = value
				}
			}
		}
	}
	constructors := make(map[string][]string)
	for _, file := range pkg.Files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Body == nil || !returnsMessage(function) {
				continue
			}
			seen := make(map[string]struct{})
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if ok && len(call.Args) > 0 {
					if value, valueOK := call.Args[0].(*ast.Ident); valueOK {
						if id, exists := constants[value.Name]; exists {
							seen[id] = struct{}{}
						}
					}
				}
				composite, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
				ident, ok := composite.Type.(*ast.Ident)
				if !ok || ident.Name != "Message" {
					return true
				}
				for _, element := range composite.Elts {
					pair, ok := element.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := pair.Key.(*ast.Ident)
					value, valueOK := pair.Value.(*ast.Ident)
					if ok && valueOK && key.Name == "ID" {
						if id, exists := constants[value.Name]; exists {
							seen[id] = struct{}{}
						}
					}
				}
				return true
			})
			for id := range seen {
				constructors[id] = append(constructors[id], function.Name.Name)
			}
		}
	}
	return constants, constructors, nil
}

func returnsMessage(function *ast.FuncDecl) bool {
	if function.Type.Results == nil || len(function.Type.Results.List) != 1 {
		return false
	}
	ident, ok := function.Type.Results.List[0].Type.(*ast.Ident)
	return ok && ident.Name == "Message"
}

func validate(catalog map[string]catalogEntry, constants map[string]string, constructors map[string][]string) ([]manifestEntry, error) {
	constantNames := make(map[string][]string)
	for name, id := range constants {
		constantNames[id] = append(constantNames[id], name)
		if _, exists := catalog[id]; !exists {
			return nil, fmt.Errorf("typed message ID %s (%s) has no source catalog entry", name, id)
		}
	}
	manifest := make([]manifestEntry, 0, len(catalog))
	for id, entry := range catalog {
		names := constantNames[id]
		if len(names) != 1 {
			return nil, fmt.Errorf("source catalog ID %s has %d typed constants", id, len(names))
		}
		functions := constructors[id]
		if len(functions) != 1 {
			return nil, fmt.Errorf("source catalog ID %s has %d typed constructors", id, len(functions))
		}
		fields := entryFields(entry)
		forms := entryForms(entry)
		manifest = append(manifest, manifestEntry{ID: id, Constructor: functions[0], Fields: fields, Forms: forms})
	}
	sort.Slice(manifest, func(i, j int) bool { return manifest[i].ID < manifest[j].ID })
	return manifest, nil
}

func entryFields(entry catalogEntry) []string {
	set := make(map[string]struct{})
	for _, value := range []string{entry.Zero, entry.One, entry.Two, entry.Few, entry.Many, entry.Other} {
		for _, match := range templateFieldPattern.FindAllStringSubmatch(value, -1) {
			set[match[1]] = struct{}{}
		}
	}
	fields := make([]string, 0, len(set))
	for field := range set {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}

func entryForms(entry catalogEntry) []string {
	values := map[string]string{"zero": entry.Zero, "one": entry.One, "two": entry.Two, "few": entry.Few, "many": entry.Many, "other": entry.Other}
	forms := make([]string, 0, len(values))
	for form, value := range values {
		if value != "" {
			forms = append(forms, form)
		}
	}
	sort.Strings(forms)
	return forms
}

func render(entries []manifestEntry) ([]byte, error) {
	var source bytes.Buffer
	source.WriteString("// Code generated by scripts/localization_catalog; DO NOT EDIT.\n\n")
	source.WriteString("package localization\n\n")
	source.WriteString("// CatalogManifestEntry describes one source-catalog message and its typed constructor.\n")
	source.WriteString("type CatalogManifestEntry struct {\n\tID MessageID\n\tConstructor string\n\tFields []string\n\tPluralForms []string\n}\n\n")
	source.WriteString("// CatalogManifest is the generated source-catalog API manifest.\n")
	source.WriteString("var CatalogManifest = [...]CatalogManifestEntry{\n")
	for _, entry := range entries {
		fmt.Fprintf(&source, "\t{ID: %q, Constructor: %q, Fields: %#v, PluralForms: %#v},\n", entry.ID, entry.Constructor, entry.Fields, entry.Forms)
	}
	source.WriteString("}\n")
	return format.Source(source.Bytes())
}

func fatalIf(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "localization_catalog: "+format+"\n", args...)
	os.Exit(1)
}
