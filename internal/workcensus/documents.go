package workcensus

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Document declares an additional maintained language source. JavaScript,
// TypeScript and TSX use the shared all-source AST census; their Go type/SSA
// reachability is explicitly unavailable. Paths are relative to Config.Dir.
type Document struct {
	Path     string `json:"path"`
	Language string `json:"language"`
	Mode     string `json:"mode,omitempty"` // syntax (default) or explicit lexical approximation
}

// SourceRoot discovers maintained documents independently of package loading,
// reachability or old members. Extension keys include the dot. Exclusions are
// client data: directory names and basename globs. Skipped paths are reported.
type SourceRoot struct {
	Path         string            `json:"path"`
	Languages    map[string]string `json:"languages"`
	Mode         string            `json:"mode,omitempty"`
	ExcludeDirs  []string          `json:"exclude_dirs,omitempty"`
	ExcludeFiles []string          `json:"exclude_files,omitempty"`
}

func discoverDocuments(c *Config, root string, out *Inventory) error {
	seen := map[string]bool{}
	for _, d := range c.Documents {
		path := d.Path
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, path)
		}
		seen[filepath.Clean(path)] = true
	}
	for _, scope := range c.SourceRoots {
		base := scope.Path
		if !filepath.IsAbs(base) {
			base = filepath.Join(root, base)
		}
		base = filepath.Clean(base)
		add := func(path string, directory bool) (bool, error) {
			rel, err := filepath.Rel(base, path)
			if err != nil {
				return false, err
			}
			if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return false, nil
			}
			name := filepath.Base(path)
			excluded := directory && (name == ".git" || contains(scope.ExcludeDirs, name))
			if !directory {
				for _, pattern := range scope.ExcludeFiles {
					match, err := filepath.Match(pattern, name)
					if err != nil {
						return false, err
					}
					excluded = excluded || match
				}
			}
			if excluded {
				file, _ := filepath.Rel(root, path)
				out.Sources = append(out.Sources, SourceBoundary{File: filepath.ToSlash(file), State: "excluded/client"})
				return true, nil
			}
			language, ok := scope.Languages[strings.ToLower(filepath.Ext(path))]
			if !directory && ok && !seen[path] {
				c.Documents = append(c.Documents, Document{Path: path, Language: language, Mode: scope.Mode})
				seen[path] = true
			}
			return false, nil
		}
		if err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			skip, err := add(path, d.IsDir())
			if err != nil {
				return err
			}
			if skip && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}); err != nil {
			return err
		}
		// Overlay files can introduce an unseen package or language document.
		var overlays []string
		for path := range c.Overlay {
			overlays = append(overlays, path)
		}
		sort.Strings(overlays)
		for _, path := range overlays {
			rel, _ := filepath.Rel(base, path)
			parts := strings.Split(filepath.ToSlash(rel), "/")
			skip := false
			for _, part := range parts[:len(parts)-1] {
				skip = skip || part == ".git" || contains(scope.ExcludeDirs, part)
			}
			if !skip {
				if _, err := add(filepath.Clean(path), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func extractDocuments(c Config, root string, out *Inventory) error {
	seen := map[string]bool{}
	for _, document := range c.Documents {
		path := document.Path
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, path)
		}
		path = filepath.Clean(path)
		if seen[path] {
			return fmt.Errorf("duplicate maintained document %s", document.Path)
		}
		seen[path] = true
		if document.Mode == "lexical" {
			if err := extractLexicalDocument(c, root, path, document.Language, out); err != nil {
				return err
			}
			continue
		}
		if document.Mode != "" && document.Mode != "syntax" {
			return fmt.Errorf("unsupported document mode %q", document.Mode)
		}
		if document.Language == "go" {
			file, _ := filepath.Rel(root, path)
			already := false
			for _, s := range out.Sources {
				already = already || s.File == filepath.ToSlash(file)
			}
			if already {
				continue
			}
			raw, err := readSource(c, path)
			if err != nil {
				return err
			}
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, raw, parser.ParseComments)
			if err != nil {
				return err
			}
			e := sourceExtractor{config: c, out: out, fset: fset, root: root, seenFiles: map[string]bool{}}
			if err := e.file(f.Name.Name, f, nil, "syntax-only"); err != nil {
				return err
			}
			out.Limitations = append(out.Limitations, "Go documents outside the loaded composition are syntax-only: no fabricated type or reachability facts.")
			continue
		}
		if err := extractSyntaxDocument(c, root, path, document.Language, out); err != nil {
			return err
		}
	}
	return nil
}

func extractLexicalDocument(c Config, root, path, language string, out *Inventory) error {
	if language == "" {
		return fmt.Errorf("lexical document requires a named language")
	}
	raw, err := readSource(c, path)
	if err != nil {
		return err
	}
	file, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	file = filepath.ToSlash(file)
	state := "lexical/" + language
	out.Sources = append(out.Sources, SourceBoundary{file, language, state, fingerprint(raw)})
	owner := language + ".file:" + file
	counts := map[string]int{}
	selectedSpans := map[string]bool{}
	for _, selector := range c.Selectors {
		if selector.Node != "text" {
			continue
		}
		if selector.Pattern == "" {
			return fmt.Errorf("text selector requires a pattern")
		}
		site := sourceSite{language: language, nativeKind: "text", pkg: language, file: file, owner: owner, state: state}
		if !matches(selector, site) {
			continue
		}
		pattern := selector.Pattern
		if selector.FoldCase {
			pattern = "(?i)" + pattern
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		for _, span := range re.FindAllIndex(raw, -1) {
			start, end := span[0], span[1]
			group := selector.Group
			if group == "" {
				group = selector.Kind
			}
			key := fmt.Sprintf("%s:%d:%d", group, start, end)
			if selectedSpans[key] {
				continue
			}
			selectedSpans[key] = true
			counts[selector.Kind]++
			line, col := coordinates(raw, start)
			endLine, endCol := coordinates(raw, end)
			out.Members = append(out.Members, Member{ID: fmt.Sprintf("%s:%s/%s#%d", file, owner, selector.Kind, counts[selector.Kind]), File: file, Owner: owner, Kind: selector.Kind, Node: "text", Language: language, State: state, Code: string(raw[start:end]), Digest: fingerprint(raw[start:end]), OwnerDigest: fingerprint(raw), Offset: start, EndOffset: end, Line: line, Column: col, EndLine: endLine, EndColumn: endCol})
		}
	}
	out.Limitations = append(out.Limitations, "Lexical "+language+" selection deliberately includes comments, declarations and values; it supplies no syntax, type, def/use or reachability proof.")
	return nil
}

func coordinates(raw []byte, offset int) (int, int) {
	text := string(raw[:offset])
	return strings.Count(text, "\n") + 1, offset - strings.LastIndex(text, "\n")
}
