package adminimport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"gopkg.in/yaml.v3"
)

// ExitOKF identifies an OKF parse, validation, or import failure.
const ExitOKF = 7

const (
	OKFProfile      = "okf"
	PGMProfile      = "pgm-0.4-draft"
	OKFFailIfExists = "fail-if-exists"
)

// OKFImportOptions configures an offline Open Knowledge Format import.
type OKFImportOptions struct {
	DatabaseName string
	FromPath     string
	Profile      string
	Mode         string
	ChunkSize    int
	// PropertyMap maps imported source property names to storage property names.
	// For example, _okf_frontmatter=source_metadata stores the source frontmatter
	// under source_metadata rather than _okf_frontmatter.
	PropertyMap map[string]string
	Now         time.Time
}

// OKFExportOptions configures an offline export of an imported OKF bundle.
type OKFExportOptions struct {
	DatabaseName string
	ToPath       string
	PropertyMap  map[string]string
}

// OKFDiagnostic is a stable, machine-readable import finding.
type OKFDiagnostic struct {
	Code    string `json:"code"`
	Path    string `json:"path"`
	Line    int    `json:"line,omitempty"`
	Message string `json:"message"`
}

// OKFReport summarizes parsing and writes performed by ImportOKF.
type OKFReport struct {
	DatabaseName            string          `json:"databaseName"`
	Profile                 string          `json:"profile"`
	ConceptsImported        int             `json:"conceptsImported"`
	RelationshipsImported   int             `json:"relationshipsImported"`
	UnresolvedRelationships int             `json:"unresolvedRelationships"`
	Warnings                []OKFDiagnostic `json:"warnings,omitempty"`
}

// OKFExportReport summarizes files written by ExportOKF.
type OKFExportReport struct {
	DatabaseName          string          `json:"databaseName"`
	ConceptsExported      int             `json:"conceptsExported"`
	ReservedFilesExported int             `json:"reservedFilesExported"`
	Warnings              []OKFDiagnostic `json:"warnings,omitempty"`
}

type okfConcept struct {
	ID          string
	Path        string
	Frontmatter map[string]any
	Body        string
	Links       []okfLink
	Unresolved  []okfUnresolvedLink
	NodeID      storage.NodeID
}

type okfBundle struct {
	Concepts      []okfConcept
	ReservedFiles map[string]string
}

type okfLink struct {
	Target string
	Text   string
	Title  string
	Line   int
}

type okfUnresolvedLink struct {
	Target string `json:"target"`
	Text   string `json:"text"`
	Title  string `json:"title,omitempty"`
	Line   int    `json:"line"`
}

// ValidateOKF parses an OKF directory without writing database state.
func ValidateOKF(opts OKFImportOptions) (OKFReport, error) {
	opts = okfDefaults(opts)
	bundle, report, err := loadOKFBundle(opts)
	if err != nil {
		return report, err
	}
	_ = bundle
	return report, nil
}

// ImportOKF imports a directory of OKF concepts into a namespaced storage engine.
// It never starts a server or generates embeddings.
func ImportOKF(ctx context.Context, engine storage.Engine, opts OKFImportOptions) (OKFReport, error) {
	opts = okfDefaults(opts)
	bundle, report, err := loadOKFBundle(opts)
	if err != nil {
		return report, err
	}
	if engine == nil {
		return report, okfError("storage engine is required")
	}
	if opts.Mode != OKFFailIfExists {
		return report, &Error{ExitCode: ExitUnsupported, Message: "unsupported OKF import mode: " + opts.Mode}
	}
	target := storage.NewNamespacedEngine(engine, opts.DatabaseName)
	if err := ensureOKFNamespaceEmpty(target, opts); err != nil {
		return report, err
	}

	concepts := bundle.Concepts
	byID := make(map[string]*okfConcept, len(concepts))
	for i := range concepts {
		concept := &concepts[i]
		concept.NodeID = storage.NodeID(fmt.Sprintf("okf-node-%06d", i+1))
		byID[concept.ID] = concept
	}

	edges := make([]*storage.Edge, 0)
	for i := range concepts {
		concept := &concepts[i]
		for ordinal, link := range concept.Links {
			targetConcept, resolved := byID[link.Target]
			if !resolved {
				continue
			}
			edge, edgeErr := okfEdge(concept, targetConcept, link, ordinal, opts)
			if edgeErr != nil {
				return report, edgeErr
			}
			edges = append(edges, edge)
		}
	}
	nodes := make([]*storage.Node, 0, len(concepts)+1)
	for i := range concepts {
		node, nodeErr := okfNode(&concepts[i], opts)
		if nodeErr != nil {
			return report, nodeErr
		}
		nodes = append(nodes, node)
	}
	metadataNode, metadataErr := okfBundleMetadataNode(bundle.ReservedFiles, opts)
	if metadataErr != nil {
		return report, metadataErr
	}
	nodes = append(nodes, metadataNode)
	if err := createNodesInChunks(ctx, target, nodes, opts.ChunkSize); err != nil {
		return report, err
	}
	report.ConceptsImported = len(concepts)
	if err := createEdgesInChunks(ctx, target, edges, opts.ChunkSize); err != nil {
		return report, err
	}
	report.RelationshipsImported = len(edges)
	sortDiagnostics(report.Warnings)
	return report, nil
}

func okfDefaults(opts OKFImportOptions) OKFImportOptions {
	if opts.Profile == "" {
		opts.Profile = OKFProfile
	}
	if opts.Mode == "" {
		opts.Mode = OKFFailIfExists
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 1000
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	return opts
}

// LoadPropertyMap reads an environment-style source-to-property map file.
// Each non-empty, non-comment line must be source_property=storage_property.
func LoadPropertyMap(filePath string) (map[string]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	propertyMap := make(map[string]string)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1024*1024)
	for line := 1; scanner.Scan(); line++ {
		entry := strings.TrimSpace(scanner.Text())
		if entry == "" || strings.HasPrefix(entry, "#") {
			continue
		}
		source, destination, found := strings.Cut(entry, "=")
		if !found || strings.TrimSpace(source) == "" || strings.TrimSpace(destination) == "" {
			return nil, fmt.Errorf("property map line %d must use source_property=storage_property", line)
		}
		source = strings.TrimSpace(source)
		destination = strings.TrimSpace(destination)
		if _, exists := propertyMap[source]; exists {
			return nil, fmt.Errorf("property map repeats source property %q", source)
		}
		propertyMap[source] = destination
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return propertyMap, nil
}

func loadOKFBundle(opts OKFImportOptions) (okfBundle, OKFReport, error) {
	report := OKFReport{DatabaseName: opts.DatabaseName, Profile: opts.Profile}
	if strings.TrimSpace(opts.DatabaseName) == "" {
		return okfBundle{}, report, okfError("database name is required")
	}
	if opts.Profile != OKFProfile && opts.Profile != PGMProfile {
		return okfBundle{}, report, &Error{ExitCode: ExitUnsupported, Message: "unsupported OKF profile: " + opts.Profile}
	}
	root, err := filepath.Abs(opts.FromPath)
	if err != nil || strings.TrimSpace(opts.FromPath) == "" {
		return okfBundle{}, report, okfError("OKF source directory is required")
	}
	info, err := os.Stat(root)
	if err != nil {
		return okfBundle{}, report, okfError("open OKF source: " + err.Error())
	}
	if !info.IsDir() {
		return okfBundle{}, report, okfError("OKF source must be a directory")
	}

	var files []string
	err = filepath.WalkDir(root, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return okfError("symbolic links are not allowed: " + filePath)
		}
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".md") {
			return nil
		}
		files = append(files, filePath)
		return nil
	})
	if err != nil {
		return okfBundle{}, report, err
	}
	sort.Strings(files)
	concepts := make([]okfConcept, 0, len(files))
	reservedFiles := make(map[string]string)
	seen := make(map[string]struct{})
	for _, filePath := range files {
		rel, relErr := filepath.Rel(root, filePath)
		if relErr != nil {
			return okfBundle{}, report, relErr
		}
		rel = filepath.ToSlash(rel)
		base := path.Base(rel)
		data, readErr := os.ReadFile(filePath)
		if readErr != nil {
			return okfBundle{}, report, readErr
		}
		if base == "index.md" || base == "log.md" {
			if reservedErr := validateReservedOKFFile(rel, string(data)); reservedErr != nil {
				return okfBundle{}, report, reservedErr
			}
			reservedFiles[rel] = string(data)
			continue
		}
		frontmatter, body, parseErr := splitOKFFrontmatter(rel, string(data))
		if parseErr != nil {
			return okfBundle{}, report, parseErr
		}
		typeValue, ok := frontmatter["type"].(string)
		if !ok || strings.TrimSpace(typeValue) == "" {
			return okfBundle{}, report, okfErrorAt(rel, 1, "missing_type", "concept frontmatter requires a non-empty type")
		}
		id := strings.TrimSuffix(rel, ".md")
		if _, exists := seen[id]; exists {
			return okfBundle{}, report, okfErrorAt(rel, 1, "duplicate_concept_id", "duplicate concept ID: "+id)
		}
		seen[id] = struct{}{}
		links, warnings := parseOKFLinks(rel, id, body, opts.Profile)
		report.Warnings = append(report.Warnings, warnings...)
		concepts = append(concepts, okfConcept{ID: id, Path: rel, Frontmatter: frontmatter, Body: body, Links: links})
	}
	annotateOKFUnresolvedRelationships(concepts, &report)
	sortDiagnostics(report.Warnings)
	return okfBundle{Concepts: concepts, ReservedFiles: reservedFiles}, report, nil
}

func annotateOKFUnresolvedRelationships(concepts []okfConcept, report *OKFReport) {
	byID := make(map[string]struct{}, len(concepts))
	for _, concept := range concepts {
		byID[concept.ID] = struct{}{}
	}
	for i := range concepts {
		concept := &concepts[i]
		for _, link := range concept.Links {
			if _, resolved := byID[link.Target]; resolved {
				continue
			}
			concept.Unresolved = append(concept.Unresolved, okfUnresolvedLink{
				Target: link.Target,
				Text:   link.Text,
				Title:  link.Title,
				Line:   link.Line,
			})
			report.UnresolvedRelationships++
			report.Warnings = append(report.Warnings, OKFDiagnostic{
				Code: "broken_relationship_target", Path: concept.Path, Line: link.Line,
				Message: "relationship target does not exist: " + link.Target,
			})
		}
	}
}

func splitOKFFrontmatter(filePath, content string) (map[string]any, string, error) {
	content = strings.TrimPrefix(content, "\ufeff")
	if !strings.HasPrefix(content, "---\n") && !strings.HasPrefix(content, "---\r\n") {
		return nil, "", okfErrorAt(filePath, 1, "missing_frontmatter", "concept requires YAML frontmatter")
	}
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	end := -1
	for i := 1; i < len(lines); i++ {
		if lines[i] == "---" {
			end = i
			break
		}
	}
	if end < 0 {
		return nil, "", okfErrorAt(filePath, 1, "invalid_frontmatter", "frontmatter terminator is missing")
	}
	frontmatter := make(map[string]any)
	if err := yaml.Unmarshal([]byte(strings.Join(lines[1:end], "\n")), &frontmatter); err != nil {
		return nil, "", okfErrorAt(filePath, 1, "invalid_frontmatter", err.Error())
	}
	return frontmatter, strings.Join(lines[end+1:], "\n"), nil
}

func validateReservedOKFFile(filePath, content string) error {
	if path.Base(filePath) == "log.md" && strings.HasPrefix(strings.TrimPrefix(content, "\ufeff"), "---") {
		return okfErrorAt(filePath, 1, "invalid_log_frontmatter", "log.md must not have frontmatter")
	}
	if path.Base(filePath) != "index.md" || !strings.HasPrefix(strings.TrimPrefix(content, "\ufeff"), "---") {
		return nil
	}
	frontmatter, _, err := splitOKFFrontmatter(filePath, content)
	if err != nil {
		return err
	}
	if path.Dir(filePath) != "." {
		return okfErrorAt(filePath, 1, "invalid_index_frontmatter", "only root index.md may have frontmatter")
	}
	for key := range frontmatter {
		if key != "okf_version" {
			return okfErrorAt(filePath, 1, "invalid_index_frontmatter", "root index.md only permits okf_version")
		}
	}
	return nil
}

func parseOKFLinks(filePath, conceptID, body, profile string) ([]okfLink, []OKFDiagnostic) {
	var links []okfLink
	var warnings []OKFDiagnostic
	source := []byte(body)
	document := goldmark.DefaultParser().Parse(text.NewReader(source))
	_ = ast.Walk(document, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		link, ok := node.(*ast.Link)
		if !ok {
			return ast.WalkContinue, nil
		}
		target, ok := resolveOKFLink(conceptID, string(link.Destination))
		if !ok {
			return ast.WalkContinue, nil
		}
		title := string(link.Title)
		line := okfMarkdownLine(link, source)
		if profile == PGMProfile && strings.HasPrefix(strings.TrimSpace(title), "{") && pgmRelationshipProperties(title) == nil {
			warnings = append(warnings, OKFDiagnostic{Code: "invalid_pgm_relationship_properties", Path: filePath, Line: line, Message: "PGM relationship annotation is not a YAML flow mapping"})
		}
		links = append(links, okfLink{Target: target, Text: string(link.Text(source)), Title: title, Line: line})
		return ast.WalkContinue, nil
	})
	return links, warnings
}

func okfMarkdownLine(node ast.Node, source []byte) int {
	for parent := node.Parent(); parent != nil; parent = parent.Parent() {
		lines := parent.Lines()
		if lines.Len() == 0 {
			continue
		}
		return 1 + bytesCountNewlines(source[:lines.At(0).Start])
	}
	return 1
}

func bytesCountNewlines(value []byte) int {
	return strings.Count(string(value), "\n")
}

func resolveOKFLink(sourceID, raw string) (string, bool) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "" || parsed.Host != "" || parsed.RawQuery != "" || parsed.Path == "" {
		return "", false
	}
	if !strings.HasSuffix(parsed.Path, ".md") || strings.Contains(parsed.Path, "\\") {
		return "", false
	}
	var resolved string
	if strings.HasPrefix(parsed.Path, "/") {
		resolved = path.Clean(strings.TrimPrefix(parsed.Path, "/"))
	} else {
		resolved = path.Clean(path.Join(path.Dir(sourceID), parsed.Path))
	}
	if resolved == "." || strings.HasPrefix(resolved, "../") || path.Base(resolved) == "index.md" || path.Base(resolved) == "log.md" {
		return "", false
	}
	return strings.TrimSuffix(resolved, ".md"), true
}

func okfNode(concept *okfConcept, opts OKFImportOptions) (*storage.Node, error) {
	frontmatter, err := json.Marshal(concept.Frontmatter)
	if err != nil {
		return nil, okfError("encode frontmatter: " + err.Error())
	}
	props := make(map[string]any, len(concept.Frontmatter)+6)
	for source, value := range map[string]any{
		"_okf_bundle":      opts.DatabaseName,
		"_okf_concept_id":  concept.ID,
		"_okf_path":        concept.Path,
		"_okf_frontmatter": string(frontmatter),
		"_okf_body":        concept.Body,
	} {
		if err := setMappedOKFProperty(props, opts, source, value); err != nil {
			return nil, err
		}
	}
	for key, value := range concept.Frontmatter {
		if isStorageScalar(value) {
			if err := setMappedOKFProperty(props, opts, key, value); err != nil {
				return nil, err
			}
		}
	}
	if len(concept.Unresolved) > 0 {
		unresolved, err := json.Marshal(concept.Unresolved)
		if err != nil {
			return nil, okfError("encode unresolved relationships: " + err.Error())
		}
		if err := setMappedOKFProperty(props, opts, "_okf_unresolved_relationships", string(unresolved)); err != nil {
			return nil, err
		}
	}
	return &storage.Node{ID: concept.NodeID, Properties: props, CreatedAt: opts.Now, UpdatedAt: opts.Now}, nil
}

func okfBundleMetadataNode(reservedFiles map[string]string, opts OKFImportOptions) (*storage.Node, error) {
	encoded, err := json.Marshal(reservedFiles)
	if err != nil {
		return nil, okfError("encode reserved OKF files: " + err.Error())
	}
	properties := make(map[string]any, 2)
	if err := setMappedOKFProperty(properties, opts, "_okf_bundle_metadata", string(encoded)); err != nil {
		return nil, err
	}
	if err := setMappedOKFProperty(properties, opts, "_okf_bundle", opts.DatabaseName); err != nil {
		return nil, err
	}
	return &storage.Node{
		ID:         storage.NodeID("okf-bundle-metadata"),
		Properties: properties,
		CreatedAt:  opts.Now,
		UpdatedAt:  opts.Now,
	}, nil
}

// ExportOKF writes the original OKF source representation of an imported bundle.
// The target directory must be empty so export cannot overwrite unrelated files.
func ExportOKF(ctx context.Context, engine storage.Engine, opts OKFExportOptions) (OKFExportReport, error) {
	report := OKFExportReport{DatabaseName: opts.DatabaseName}
	if strings.TrimSpace(opts.DatabaseName) == "" {
		return report, okfError("database name is required")
	}
	if strings.TrimSpace(opts.ToPath) == "" {
		return report, okfError("OKF output directory is required")
	}
	if engine == nil {
		return report, okfError("storage engine is required")
	}
	if err := ensureEmptyOKFOutputDirectory(opts.ToPath); err != nil {
		return report, err
	}

	target := storage.NewNamespacedEngine(engine, opts.DatabaseName)
	nodes, err := target.AllNodes()
	if err != nil {
		return report, err
	}
	metadataName := okfPropertyName(OKFImportOptions{PropertyMap: opts.PropertyMap}, "_okf_bundle_metadata")
	conceptIDName := okfPropertyName(OKFImportOptions{PropertyMap: opts.PropertyMap}, "_okf_concept_id")
	pathName := okfPropertyName(OKFImportOptions{PropertyMap: opts.PropertyMap}, "_okf_path")
	frontmatterName := okfPropertyName(OKFImportOptions{PropertyMap: opts.PropertyMap}, "_okf_frontmatter")
	bodyName := okfPropertyName(OKFImportOptions{PropertyMap: opts.PropertyMap}, "_okf_body")

	var reservedFiles map[string]string
	concepts := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if raw, metadata := node.Properties[metadataName].(string); metadata {
			if err := json.Unmarshal([]byte(raw), &reservedFiles); err != nil {
				return report, okfError("decode reserved OKF files: " + err.Error())
			}
			continue
		}
		if _, concept := node.Properties[conceptIDName].(string); concept {
			concepts = append(concepts, node)
		}
	}
	if len(concepts) == 0 {
		return report, okfError("database does not contain an imported OKF bundle")
	}
	sort.Slice(concepts, func(i, j int) bool {
		return concepts[i].Properties[pathName].(string) < concepts[j].Properties[pathName].(string)
	})
	for _, node := range concepts {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		pathValue, pathOK := node.Properties[pathName].(string)
		frontmatter, frontmatterOK := node.Properties[frontmatterName].(string)
		body, bodyOK := node.Properties[bodyName].(string)
		if !pathOK || !frontmatterOK || !bodyOK {
			return report, okfError("imported OKF concept is missing preserved source properties")
		}
		if err := writeOKFConcept(opts.ToPath, pathValue, frontmatter, body); err != nil {
			return report, err
		}
		report.ConceptsExported++
	}
	if len(reservedFiles) == 0 {
		reservedFiles = map[string]string{"index.md": "---\nokf_version: \"0.2\"\n---\n"}
	}
	for relativePath, content := range reservedFiles {
		if err := writeOKFFile(opts.ToPath, relativePath, content, true); err != nil {
			return report, err
		}
		report.ReservedFilesExported++
	}
	return report, nil
}

func ensureEmptyOKFOutputDirectory(outputDir string) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return okfError("OKF output directory must be empty")
	}
	return nil
}

func writeOKFConcept(outputDir, relativePath, encodedFrontmatter, body string) error {
	var frontmatter map[string]any
	if err := json.Unmarshal([]byte(encodedFrontmatter), &frontmatter); err != nil {
		return okfError("decode imported frontmatter: " + err.Error())
	}
	encoded, err := yaml.Marshal(frontmatter)
	if err != nil {
		return okfError("encode exported frontmatter: " + err.Error())
	}
	content := "---\n" + string(encoded) + "---\n" + body
	return writeOKFFile(outputDir, relativePath, content, false)
}

func writeOKFFile(outputDir, relativePath, content string, reserved bool) error {
	relativePath = filepath.ToSlash(relativePath)
	clean := path.Clean(relativePath)
	if clean == "." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) || !strings.HasSuffix(clean, ".md") {
		return okfError("invalid OKF output path: " + relativePath)
	}
	if reserved != (path.Base(clean) == "index.md" || path.Base(clean) == "log.md") {
		return okfError("invalid OKF reserved output path: " + relativePath)
	}
	outputPath := filepath.Join(outputDir, filepath.FromSlash(clean))
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(outputPath, []byte(content), 0o600)
}

func okfEdge(source, target *okfConcept, link okfLink, ordinal int, opts OKFImportOptions) (*storage.Edge, error) {
	// OKF links are explicitly untyped. NornicDB stores that fact as an empty
	// relationship type instead of inventing an OKF-specific relationship type.
	edgeType := ""
	props := make(map[string]any, 10)
	for name, value := range map[string]any{
		"_okf_bundle":               opts.DatabaseName,
		"_okf_source":               source.ID,
		"_okf_target":               target.ID,
		"_okf_link_text":            link.Text,
		"_okf_link_title":           link.Title,
		"_okf_relationship_ordinal": int64(ordinal),
	} {
		if err := setMappedOKFProperty(props, opts, name, value); err != nil {
			return nil, err
		}
	}
	if opts.Profile == PGMProfile {
		pgmProps := pgmRelationshipProperties(link.Title)
		if pgmProps == nil {
			pgmProps = map[string]any{}
		}
		encoded, err := json.Marshal(pgmProps)
		if err != nil {
			return nil, okfError("encode PGM relationship properties: " + err.Error())
		}
		for name, value := range map[string]any{
			"_pgm_properties":       string(encoded),
			"_pgm_relationship_key": fmt.Sprintf("%s:%s:%s", source.ID, target.ID, string(encoded)),
			"_pgm_relationship_id":  fmt.Sprintf("%s:%d", source.ID, ordinal),
			"_pgm_occurrence":       int64(ordinal),
		} {
			if err := setMappedOKFProperty(props, opts, name, value); err != nil {
				return nil, err
			}
		}
		if typ, ok := pgmProps["type"].(string); ok && strings.TrimSpace(typ) != "" {
			edgeType = typ
			if err := setMappedOKFProperty(props, opts, "_pgm_type", typ); err != nil {
				return nil, err
			}
		}
	}
	return &storage.Edge{
		ID:         storage.EdgeID(fmt.Sprintf("okf-edge-%s-%06d", strings.ReplaceAll(source.ID, "/", "-"), ordinal+1)),
		StartNode:  source.NodeID,
		EndNode:    target.NodeID,
		Type:       edgeType,
		Properties: props,
		CreatedAt:  opts.Now,
		UpdatedAt:  opts.Now,
		Confidence: 1,
	}, nil
}

func setMappedOKFProperty(properties map[string]any, opts OKFImportOptions, source string, value any) error {
	destination := okfPropertyName(opts, source)
	if _, exists := properties[destination]; exists {
		return fmt.Errorf("property map maps multiple imported fields to %q", destination)
	}
	properties[destination] = value
	return nil
}

func okfPropertyName(opts OKFImportOptions, source string) string {
	if destination, ok := opts.PropertyMap[source]; ok {
		return destination
	}
	return source
}

func pgmRelationshipProperties(title string) map[string]any {
	if !strings.HasPrefix(strings.TrimSpace(title), "{") {
		return map[string]any{}
	}
	var properties map[string]any
	if err := yaml.Unmarshal([]byte(title), &properties); err != nil || properties == nil {
		return nil
	}
	return properties
}

func isStorageScalar(value any) bool {
	switch value.(type) {
	case string, bool, int, int64, float64:
		return true
	case []string:
		return true
	case []any:
		return true
	default:
		return false
	}
}

func ensureOKFNamespaceEmpty(engine storage.Engine, opts OKFImportOptions) error {
	nodes, err := engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if _, exists := node.Properties[okfPropertyName(opts, "_okf_concept_id")]; exists {
			return okfError("OKF namespace already contains concepts")
		}
	}
	return nil
}

func createNodesInChunks(ctx context.Context, engine storage.Engine, nodes []*storage.Node, chunkSize int) error {
	for len(nodes) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(chunkSize, len(nodes))
		if err := engine.BulkCreateNodes(nodes[:n]); err != nil {
			return err
		}
		nodes = nodes[n:]
	}
	return nil
}

func createEdgesInChunks(ctx context.Context, engine storage.Engine, edges []*storage.Edge, chunkSize int) error {
	for len(edges) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(chunkSize, len(edges))
		if err := engine.BulkCreateEdges(edges[:n]); err != nil {
			return err
		}
		edges = edges[n:]
	}
	return nil
}

func sortDiagnostics(diagnostics []OKFDiagnostic) {
	sort.Slice(diagnostics, func(i, j int) bool {
		if diagnostics[i].Path != diagnostics[j].Path {
			return diagnostics[i].Path < diagnostics[j].Path
		}
		if diagnostics[i].Line != diagnostics[j].Line {
			return diagnostics[i].Line < diagnostics[j].Line
		}
		return diagnostics[i].Code < diagnostics[j].Code
	})
}

func okfError(message string) error { return &Error{ExitCode: ExitOKF, Message: message} }

func okfErrorAt(filePath string, line int, code, message string) error {
	return &Error{ExitCode: ExitOKF, Message: fmt.Sprintf("%s:%d: %s: %s", filePath, line, code, message)}
}
