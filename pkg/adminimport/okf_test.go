package adminimport

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestValidateOKFDocumentationCorpus(t *testing.T) {
	report, err := ValidateOKF(OKFImportOptions{
		DatabaseName: "knowledge",
		FromPath:     okfDocumentationCorpusPath(t),
		Profile:      PGMProfile,
	})
	require.NoError(t, err)
	require.Equal(t, 1, report.UnresolvedRelationships)
	require.Len(t, report.Warnings, 1)
	require.Equal(t, OKFDiagnostic{
		Code:    "broken_relationship_target",
		Path:    "architecture/system.md",
		Line:    8,
		Message: "relationship target does not exist: drafts/future-migration",
	}, report.Warnings[0])
}

func TestImportOKFDocumentationCorpusPGM(t *testing.T) {
	base := storage.NewMemoryEngine()
	report, err := ImportOKF(context.Background(), base, OKFImportOptions{
		DatabaseName: "knowledge",
		FromPath:     okfDocumentationCorpusPath(t),
		Profile:      PGMProfile,
		ChunkSize:    2,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)
	require.Equal(t, 3, report.ConceptsImported)
	require.Equal(t, 6, report.RelationshipsImported)
	require.Equal(t, 1, report.UnresolvedRelationships)

	engine := storage.NewNamespacedEngine(base, "knowledge")
	nodes, err := engine.AllNodes()
	require.NoError(t, err)
	nodes = okfConceptNodes(nodes, "_okf_concept_id")
	require.Len(t, nodes, 3)
	byConceptID := make(map[string]*storage.Node, len(nodes))
	for _, node := range nodes {
		byConceptID[node.Properties["_okf_concept_id"].(string)] = node
		require.Equal(t, "knowledge", node.Properties["_okf_bundle"])
		require.NotEmpty(t, node.Properties["_okf_frontmatter"])
		require.NotEmpty(t, node.Properties["_okf_body"])
	}

	architecture := byConceptID["architecture/system"]
	require.Equal(t, "architecture", architecture.Properties["type"])
	require.Equal(t, "NornicDB architecture", architecture.Properties["title"])
	var unresolved []okfUnresolvedLink
	require.NoError(t, json.Unmarshal([]byte(architecture.Properties["_okf_unresolved_relationships"].(string)), &unresolved))
	require.Equal(t, []okfUnresolvedLink{{
		Target: "drafts/future-migration", Text: "future migration", Line: 8,
	}}, unresolved)

	edges, err := engine.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 6)
	edgeTypes := make([]string, 0, len(edges))
	for _, edge := range edges {
		edgeTypes = append(edgeTypes, edge.Type)
		require.Equal(t, "knowledge", edge.Properties["_okf_bundle"])
	}
	sort.Strings(edgeTypes)
	require.Equal(t, []string{"", "", "CONFIGURED_BY", "DOCUMENTS", "OPERATES_ON", "REFERENCES"}, edgeTypes)
}

func TestImportOKFDocumentationCorpusBaselineProfile(t *testing.T) {
	base := storage.NewMemoryEngine()
	report, err := ImportOKF(context.Background(), base, OKFImportOptions{
		DatabaseName: "knowledge",
		FromPath:     okfDocumentationCorpusPath(t),
		Now:          fixedImportTime,
	})
	require.NoError(t, err)
	require.Equal(t, 6, report.RelationshipsImported)

	edges, err := storage.NewNamespacedEngine(base, "knowledge").AllEdges()
	require.NoError(t, err)
	for _, edge := range edges {
		require.Empty(t, edge.Type)
		require.NotContains(t, edge.Properties, "_pgm_properties")
	}

	_, err = ImportOKF(context.Background(), base, OKFImportOptions{
		DatabaseName: "knowledge",
		FromPath:     okfDocumentationCorpusPath(t),
		Now:          fixedImportTime,
	})
	var importErr *Error
	require.True(t, errors.As(err, &importErr))
	require.Equal(t, ExitOKF, importErr.ExitCode)
}

func TestImportOKFPropertyMapWritesRequestedStorageProperties(t *testing.T) {
	base := storage.NewMemoryEngine()
	propertyMap := map[string]string{
		"_okf_concept_id":  "concept_key",
		"_okf_frontmatter": "source_metadata",
		"_pgm_properties":  "relationship_metadata",
		"type":             "concept_type",
	}
	report, err := ImportOKF(context.Background(), base, OKFImportOptions{
		DatabaseName: "knowledge",
		FromPath:     okfDocumentationCorpusPath(t),
		Profile:      PGMProfile,
		PropertyMap:  propertyMap,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)
	require.Equal(t, 3, report.ConceptsImported)

	engine := storage.NewNamespacedEngine(base, "knowledge")
	nodes, err := engine.AllNodes()
	require.NoError(t, err)
	nodes = okfConceptNodes(nodes, "concept_key")
	require.Len(t, nodes, 3)
	for _, node := range nodes {
		require.NotContains(t, node.Properties, "_okf_concept_id")
		require.NotContains(t, node.Properties, "_okf_frontmatter")
		require.Contains(t, node.Properties, "concept_key")
		require.Contains(t, node.Properties, "source_metadata")
		require.Contains(t, node.Properties, "concept_type")
	}
	edges, err := engine.AllEdges()
	require.NoError(t, err)
	for _, edge := range edges {
		require.Contains(t, edge.Properties, "relationship_metadata")
		require.NotContains(t, edge.Properties, "_pgm_properties")
	}
	exportPath := filepath.Join(t.TempDir(), "mapped-export")
	exportReport, err := ExportOKF(context.Background(), base, OKFExportOptions{
		DatabaseName: "knowledge",
		ToPath:       exportPath,
		PropertyMap:  propertyMap,
	})
	require.NoError(t, err)
	require.Equal(t, 3, exportReport.ConceptsExported)
	_, err = ValidateOKF(OKFImportOptions{DatabaseName: "knowledge", FromPath: exportPath, Profile: PGMProfile})
	require.NoError(t, err)

	_, err = ImportOKF(context.Background(), base, OKFImportOptions{
		DatabaseName: "knowledge",
		FromPath:     okfDocumentationCorpusPath(t),
		Profile:      PGMProfile,
		PropertyMap:  propertyMap,
		Now:          fixedImportTime,
	})
	require.ErrorContains(t, err, "already contains concepts")
}

func TestExportOKFDocumentationCorpusRoundTripsSourceBundle(t *testing.T) {
	base := storage.NewMemoryEngine()
	sourcePath := okfDocumentationCorpusPath(t)
	_, err := ImportOKF(context.Background(), base, OKFImportOptions{
		DatabaseName: "knowledge",
		FromPath:     sourcePath,
		Profile:      PGMProfile,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)

	outputPath := filepath.Join(t.TempDir(), "exported-bundle")
	report, err := ExportOKF(context.Background(), base, OKFExportOptions{
		DatabaseName: "knowledge",
		ToPath:       outputPath,
	})
	require.NoError(t, err)
	require.Equal(t, 3, report.ConceptsExported)
	require.Equal(t, 3, report.ReservedFilesExported)

	validation, err := ValidateOKF(OKFImportOptions{
		DatabaseName: "knowledge",
		FromPath:     outputPath,
		Profile:      PGMProfile,
	})
	require.NoError(t, err)
	require.Equal(t, 1, validation.UnresolvedRelationships)

	for _, reservedPath := range []string{"index.md", "log.md", "architecture/index.md"} {
		source, err := os.ReadFile(filepath.Join(sourcePath, reservedPath))
		require.NoError(t, err)
		exported, err := os.ReadFile(filepath.Join(outputPath, reservedPath))
		require.NoError(t, err)
		require.Equal(t, string(source), string(exported))
	}

	exportedConcept, err := os.ReadFile(filepath.Join(outputPath, "architecture", "system.md"))
	require.NoError(t, err)
	require.Contains(t, string(exportedConcept), "[hybrid-search reference][hybrid-reference]")
	require.Contains(t, string(exportedConcept), "[A link in a code example](../operations/admin-import.md)")
}

func TestLoadPropertyMap(t *testing.T) {
	propertyMap, err := LoadPropertyMap(filepath.Join(okfDocumentationCorpusPath(t), "property-map.env"))
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"_okf_concept_id":  "concept_key",
		"_okf_frontmatter": "source_metadata",
		"_pgm_properties":  "relationship_metadata",
		"type":             "concept_type",
	}, propertyMap)
}

func TestValidateOKFRejectsInvalidConceptAndReservedFiles(t *testing.T) {
	t.Run("concept requires type", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, writeOKFFixture(dir, "concept.md", "---\ntitle: Missing type\n---\n# Concept\n"))
		_, err := ValidateOKF(OKFImportOptions{DatabaseName: "knowledge", FromPath: dir})
		require.ErrorContains(t, err, "missing_type")
	})

	t.Run("nested index cannot have frontmatter", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, writeOKFFixture(dir, "concept.md", "---\ntype: concept\n---\n# Concept\n"))
		require.NoError(t, writeOKFFixture(dir, "nested/index.md", "---\nokf_version: '0.2'\n---\n# Nested\n"))
		_, err := ValidateOKF(OKFImportOptions{DatabaseName: "knowledge", FromPath: dir})
		require.ErrorContains(t, err, "invalid_index_frontmatter")
	})
}

func okfDocumentationCorpusPath(t *testing.T) string {
	t.Helper()
	return filepath.Join("testdata", "okf", "nornicdb-docs")
}

func writeOKFFixture(root, relativePath, content string) error {
	filePath := filepath.Join(root, relativePath)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(filePath, []byte(content), 0o600)
}

func okfConceptNodes(nodes []*storage.Node, conceptIDProperty string) []*storage.Node {
	concepts := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if _, ok := node.Properties[conceptIDProperty].(string); ok {
			concepts = append(concepts, node)
		}
	}
	return concepts
}
