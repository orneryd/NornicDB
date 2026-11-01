//go:build cgo

package workcensus

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	sitter "github.com/tree-sitter/go-tree-sitter"
	javascript "github.com/tree-sitter/tree-sitter-javascript/bindings/go"
	typescript "github.com/tree-sitter/tree-sitter-typescript/bindings/go"
)

func extractSyntaxDocument(c Config, root, path, language string, out *Inventory) error {
	parser := sitter.NewParser()
	defer parser.Close()
	var grammar *sitter.Language
	switch language {
	case "javascript":
		grammar = sitter.NewLanguage(javascript.Language())
	case "typescript":
		grammar = sitter.NewLanguage(typescript.LanguageTypescript())
	case "tsx":
		grammar = sitter.NewLanguage(typescript.LanguageTSX())
	default:
		return fmt.Errorf("unsupported document language %q", language)
	}
	if err := parser.SetLanguage(grammar); err != nil {
		return err
	}
	raw, err := readSource(c, path)
	if err != nil {
		return err
	}
	tree := parser.Parse(raw, nil)
	if tree == nil {
		return fmt.Errorf("no syntax tree for %s", path)
	}
	defer tree.Close()
	if tree.RootNode().HasError() {
		return fmt.Errorf("invalid %s syntax in %s", language, path)
	}
	file, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	file = filepath.ToSlash(file)
	state := "source/" + language
	out.Sources = append(out.Sources, SourceBoundary{file, language, state, fingerprint(raw)})
	units := make([]int, len(raw)+1)
	total := 0
	for pos := 0; pos < len(raw); {
		r, size := utf8.DecodeRune(raw[pos:])
		for j := 0; j < size; j++ {
			units[pos+j] = total
		}
		total++
		if r > 0xffff {
			total++
		}
		pos += size
		units[pos] = total
	}
	lineStarts := []int{0}
	for i, b := range raw {
		if b == '\n' {
			lineStarts = append(lineStarts, i+1)
		}
	}
	counts := map[string]map[string]int{}
	names := map[string]int{}
	base := language + "." + file
	var visit func(*sitter.Node, string, string, string)
	visit = func(node *sitter.Node, owner, ownerDigest, declaration string) {
		kind := documentKind(node.Kind())
		name := documentName(node, raw)
		if contains([]string{"function", "method", "closure"}, kind) {
			if name == "" {
				name = "anonymous"
			}
			key := owner + "." + name
			names[key]++
			owner = key
			if names[key] > 1 {
				owner += fmt.Sprintf("$%d", names[key])
			}
			ownerDigest = fingerprint(raw[node.StartByte():node.EndByte()])
			declaration = kind
		} else if kind == "var" || kind == "type" || kind == "alias" || kind == "import" {
			if name == "" {
				name = node.Kind()
			}
			owner = owner + "." + kind + ":" + name
			ownerDigest = fingerprint(raw[node.StartByte():node.EndByte()])
			declaration = kind
		}
		if kind != "" {
			site := sourceSite{pkg: language, file: file, owner: owner, declaration: declaration, state: state, name: name, language: language, nativeKind: kind}
			if kind == "call" {
				site.name = documentCallee(node, raw)
			}
			for _, selected := range selectKinds(site, c.Selectors) {
				if counts[owner] == nil {
					counts[owner] = map[string]int{}
				}
				counts[owner][selected]++
				start, end := int(node.StartByte()), int(node.EndByte())
				row := int(node.StartPosition().Row)
				endRow := int(node.EndPosition().Row)
				m := Member{ID: fmt.Sprintf("%s:%s/%s#%d", file, owner, selected, counts[owner][selected]), Node: kind, File: file, Owner: owner, Kind: selected, Language: language, State: state, Declaration: declaration, OwnerDigest: ownerDigest, Digest: fingerprint(raw[start:end]), Code: string(raw[start:end]), EndLine: endRow + 1, EndColumn: units[end] - units[lineStarts[endRow]] + 1, Line: row + 1, Column: units[start] - units[lineStarts[row]] + 1, Offset: start, EndOffset: end, UTF16Offset: units[start], UTF16End: units[end]}
				if contains([]string{"function", "method", "closure"}, kind) {
					if body := node.ChildByFieldName("body"); body != nil {
						m.FunctionBody = int(body.StartByte()) + 1
						m.Signature = string(raw[start:body.StartByte()])
					}
					m.Callees = documentCallees(node, raw)
				}
				out.Members = append(out.Members, m)
			}
		}
		for i := uint(0); i < node.NamedChildCount(); i++ {
			visit(node.NamedChild(i), owner, ownerDigest, declaration)
		}
	}
	visit(tree.RootNode(), base, fingerprint(raw), "file")
	return nil
}

func documentKind(kind string) string {
	switch kind {
	case "program":
		return "file"
	case "function_declaration", "generator_function_declaration":
		return "function"
	case "function_expression", "generator_function", "arrow_function":
		return "closure"
	case "method_definition", "method_signature", "abstract_method_signature":
		return "method"
	case "variable_declarator":
		return "var"
	case "type_alias_declaration":
		return "alias"
	case "interface_declaration", "class_declaration":
		return "type"
	case "import_statement":
		return "import"
	case "for_statement", "while_statement", "do_statement":
		return "for"
	case "for_in_statement":
		return "range"
	case "call_expression", "new_expression":
		return "call"
	case "subscript_expression":
		return "index"
	case "identifier", "property_identifier":
		return "identifier"
	case "binary_expression":
		return "binary"
	case "switch_statement":
		return "switch"
	case "assignment_expression", "augmented_assignment_expression":
		return "assign"
	case "return_statement":
		return "return"
	case "object", "array":
		return "composite"
	}
	return ""
}

func documentName(node *sitter.Node, raw []byte) string {
	if documentKind(node.Kind()) == "identifier" {
		return node.Utf8Text(raw)
	}
	var parts []string
	if name := node.ChildByFieldName("name"); name != nil {
		parts = append(parts, name.Utf8Text(raw))
	}
	if contains([]string{"function", "method", "closure"}, documentKind(node.Kind())) {
		for parent := node.Parent(); parent != nil; parent = parent.Parent() {
			if contains([]string{"function", "method", "closure"}, documentKind(parent.Kind())) {
				break
			}
			if parent.Kind() == "variable_declarator" || parent.Kind() == "class_declaration" || parent.Kind() == "pair" {
				field := "name"
				if parent.Kind() == "pair" {
					field = "key"
				}
				if name := parent.ChildByFieldName(field); name != nil {
					value := name.Utf8Text(raw)
					if len(parts) == 0 || parts[len(parts)-1] != value {
						parts = append(parts, value)
					}
				}
			}
		}
		for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
			parts[i], parts[j] = parts[j], parts[i]
		}
	}
	return strings.Join(parts, ".")
}

func documentCallee(node *sitter.Node, raw []byte) string {
	f := node.ChildByFieldName("function")
	if f == nil {
		f = node.ChildByFieldName("constructor")
	}
	if f == nil {
		return ""
	}
	if property := f.ChildByFieldName("property"); property != nil {
		return property.Utf8Text(raw)
	}
	return f.Utf8Text(raw)
}

func documentCallees(node *sitter.Node, raw []byte) []string {
	set := map[string]bool{}
	var visit func(*sitter.Node)
	visit = func(node *sitter.Node) {
		if node.Kind() == "call_expression" || node.Kind() == "new_expression" {
			set[documentCallee(node, raw)] = true
		}
		for i := uint(0); i < node.NamedChildCount(); i++ {
			visit(node.NamedChild(i))
		}
	}
	visit(node)
	var out []string
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
