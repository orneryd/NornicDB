// Command record runs the bounded subject suite in the lab, captures owner
// callers, and builds a ledger from parsed source sites and recorded frames.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type site struct {
	Runtime, File, Function, Callee, Source string
	Line, End                               int
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "catalog" {
		catalog()
		return
	}
	compiledCatalog, _ := catalogSites()
	dir, err := os.MkdirTemp("", "name-recording-")
	must(err)
	defer os.RemoveAll(dir)
	recording := filepath.Join(dir, "observed.jsonl")
	env := append(os.Environ(), "NORNIC_NAME_RECORDING="+recording)
	// One package at a time: the recorder job remains within its worker's
	// resource allocation. The exact selectors are also its written test plan.
	suites := [][2]string{
		{"./internal/cyphertext", "^Test(NameSemanticFixtures|NameEmissionInventory|NameEmissionInventoryOmissionControls)$"},
		{"./apoc/label", "^TestLabelPatternEmitterProperty$"},
		{"./pkg/adminimport", "^Test(RelationshipEmitterProperty|NameEmissionNodeRenderers)$"},
		{"./pkg/fabric", "^Test(NameEmission.*|ExecuteApplyAsPipeline_.*|MergeImportColumns_Deduplication|FabricImportRewriteHelpers|RewriteLeadingWithImports_.*|RowBindings|BuildColumnIndex_PreservesDecodedWhitespace|IsSimpleWithImportClause.*|SubstituteVarsWithParams_EmptyVar|FabricExecutor_ApplyBatchedLookupRows(_ModifiersPerOuterKey)?|TryExecuteApplyBatchedLookup_AdmissionGate|TryExecuteApplyBatchedCountLookup_TraversalPattern|ExecuteCollectedMapJoinFlatProjection|ExecuteApplyInMemoryProjection_RowsGroupedJoin)$"},
		{"./pkg/graphql/resolvers", "^TestRelationshipEmitterProperty$"},
		{"./pkg/storage", "^Test(RemoteLabelEmitterRoundTrip|RemoteRelationshipEmitterRoundTrip|NameEmissionRemoteParameterKeys)$"},
		{"./pkg/mcp", "^TestRelationshipEmitterProperty$"},
		{"./plugins/heimdall", "^TestAutocompleteRelationshipNamesRoundTrip$"},
		{"./scripts", "^TestLegacyMigrationEmitterProperty$"},
		{"./scripts/migration/neo4j", "^TestMigrationEmitterProperty$"},
		{"./pkg/cypher", "^Test(NameDefinition.*|NameEmission.*|DecodedName.*|DirectName.*|Cypher25BatchTwoSweepCases|AllReduceAndPropertyExists|DbRetrieveDecodedWhitespaceFilterKeys|DDLPropertyReferenceSpansAndModes|RelationshipEmitterProperty|RelationshipEmitterHintProperty|RelationshipDegreeDecodedTypes|CorrelatedSubqueryEscapedImportShortcutParity|CorrelatedPipelineImportedIdentifierShortcutParity|ImportedIdentifierReferenceTokens|TryCollectNodesFromIDInParam_AndIndexCandidateLabels|FindCosineVectorIndexName_DisambiguatesByEntityType|MatchVectorCosineFastPath_(RequiresMatchingIndex|UsesVectorIndex)|SeedNodesFromOuterMatch_FastPathsAndFallbacks|SubqueryHelperBranches|ResolveCorrelatedImportValue_OptionalFallbackBranch)$"},
	}
	failed := false
	var passedSubjects []string
	selection := os.Getenv("NORNIC_NAME_SUBJECT_PACKAGE")
	selected := false
	for _, suite := range suites {
		if selection != "" && selection != suite[0] {
			continue
		}
		selected = true
		args := []string{"test", "-json", "-tags=noui,nolocalllm,cypheremission,cyphernames", "-race", suite[0], "-run", suite[1], "-count=1", "-timeout=8m"}
		fmt.Printf("RECORDER_SUBJECT %s\n", strings.Join(args, " "))
		cmd := exec.Command("go", args...)
		cmd.Env = env
		output, runErr := cmd.CombinedOutput()
		for _, line := range bytes.Split(output, []byte{'\n'}) {
			var event struct{ Action, Package, Test, Output string }
			if json.Unmarshal(line, &event) != nil {
				fmt.Println(string(line))
				continue
			}
			if event.Action == "pass" && event.Test != "" && !strings.Contains(event.Test, "/") {
				passedSubjects = append(passedSubjects, event.Package+"#"+event.Test)
			}
			if event.Test == "" || runErr != nil {
				fmt.Print(event.Output)
			}
		}
		if runErr != nil {
			failed = true
		}
	}
	if !selected {
		panic("no name-emission subject package matched " + selection)
	}
	controls := append([]string{removalControl(dir)}, peerRemovalControls(dir)...)
	cmd := exec.Command("go", "run", "./internal/testutil/cyphernames/cmd/record", "catalog")
	census, err := cmd.Output()
	must(err)
	must(os.WriteFile(filepath.Join(dir, "sites.json"), census, 0600))
	// Preserve the observed set independently of ledger generation, so a
	// generator failure does not require rerunning successful subject tests.
	records, err := os.ReadFile(recording)
	must(err)
	var controlEvidence []json.RawMessage
	for _, control := range controls {
		data, err := os.ReadFile(control)
		must(err)
		controlEvidence = append(controlEvidence, json.RawMessage(data))
	}
	artifact, err := json.Marshal(map[string]any{"authorship": map[string]string{"record_author": "#1486 lane (Codex)", "observations": "lab runtime frames and test outcomes, not semantic approvals"}, "compiled_catalog": json.RawMessage(compiledCatalog), "sites": json.RawMessage(census), "recording": string(records), "controls": controlEvidence, "subjects_passed": !failed, "passed_subjects": passedSubjects, "semantic_sources": semanticSources(passedSubjects)})
	must(err)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err = writer.Write(artifact)
	must(err)
	must(writer.Close())
	fmt.Println("NAME_EMISSION_RECORDING " + base64.StdEncoding.EncodeToString(compressed.Bytes()))
	artifactPath := filepath.Join(dir, "artifact.json")
	must(os.WriteFile(artifactPath, artifact, 0600))
	coverageArgs := []string{"internal/testutil/cyphernames/coverage.py", "--artifact", artifactPath}
	if run(env, "python3", coverageArgs...) != nil {
		failed = true
	}
	if failed {
		os.Exit(1)
	}
}

func run(env []string, program string, args ...string) error {
	cmd := exec.Command(program, args...)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// The Go population comes from #1667's loaded compiler program through the
// nameflow domain query. This consumer has no parser or text-based enumerator.
// The lab pipeline generates the catalog in the same checked-out source state.
func catalogSites() ([]byte, []site) {
	path := os.Getenv("NORNIC_NAME_COMPILED_CATALOG")
	if path == "" {
		panic("compiled Go name catalog required: run the nameflow lab recording pipeline")
	}
	data, err := os.ReadFile(path)
	must(err)
	var catalog struct {
		Authorship map[string]string `json:"authorship"`
		Head       string            `json:"head"`
		Sites      []site            `json:"sites"`
	}
	must(json.Unmarshal(data, &catalog))
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	must(err)
	if catalog.Head != strings.TrimSpace(string(head)) || len(catalog.Sites) == 0 || catalog.Authorship["record_author"] == "" {
		panic("compiled Go name catalog has a foreign head, empty population or missing author")
	}
	return data, catalog.Sites
}

func catalog() {
	_, sites := catalogSites()
	must(json.NewEncoder(os.Stdout).Encode(sites))
}
