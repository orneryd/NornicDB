package storage_test

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

// Proof families name executable properties, rather than using the census as a
// behavioral oracle. The independent gate checks every production site, and the
// properties exercise the shared owner at its real views and public consumers.
var capabilityProofs = map[string][]string{
	"configuration-identity":  {"TestFoundationConfigurationHandoff", "TestFoundationCompositeConfigurationRetention", "TestFoundationLabelConfigurationHandoff", "TestFoundationConfigurationWriterIdentity"},
	"public-search-terminal":  {"TestFoundationPublicSearchStorageFailure", "TestFoundationPublicSearchRelationshipStorageFailure", "TestFoundationPublicSearchReadBindingWork"},
	"batch-point-equivalence": {"TestFoundationPublicCompositeBatchPoint", "TestEnginePopulationAdapters", "TestFoundationBatchPointTypesOutsidePackage"},
	"stored-fact-operation":   {"TestStoredReadNativeModes", "TestStoredReadRetainedViewModes", "TestStoredReadCapturedAndPending", "TestStoredReadMandatoryRefusal", "TestStoredReadPayloadExclusion", "TestStoredReadMetadataPayloadShape", "TestStoredReadBufferedAdjacencyUsesReadOwner", "TestStoredReadFullThenStripControl", "TestStoredReadBatchTopologyWork", "TestStoredReadUsesCanonicalFaultBoundary", "TestStoredReadRequiredStringsDoNotRetainExcludedPayload", "TestStoredReadSelectedAndFullPhysicalPredecessors", "TestStoredReadCapturedEndpointLookupControl", "TestStoredViewPopulationCoverage", "TestEnginePopulationAdapters"},
	"read-operation":          {"TestReadCapabilityOperationOwnsRefusalAndDelivery", "TestFoundationMixedNestedOperations", "TestFoundationChildRefusalAndNoReplay", "TestFoundationHistoricalMethodDependencies", "TestFoundationOrdinaryNodeKernel", "TestFoundationMixedScalarCount"},
	"native-and-full-read":    {"TestEnginePopulationAdapters", "TestReadCapabilityR5CompositeMaintenanceIdentity"},
	"membership-binding":      {"TestFoundationPublicLogicalObligations", "TestFoundationExternalPublicLogicalObligations", "TestFoundationIssuedSystemViewObligation", "TestFoundationReconstructedPeriodicObligation", "TestFoundationReconstructedCoordinatorObligation", "TestFoundationActionPublicationObligation", "TestFoundationPositiveSchemaPublicationObligation", "TestFoundationMembershipPolicies", "TestFoundationIssuedContainerAndRemoteMembership", "TestFoundationReconstructedNativeMembership", "TestFoundationCarriedEnforcementAndLease", "TestFoundationSchemaPublicationUsesRecoveredAdapter", "TestFoundationModeBindingReadsLiveOwner"},
	"commit-owner":            {"TestFoundationCommitOwnerAndExactMutations", "TestFoundationReconstructedNativeMembership", "TestTransactionQuotaWireStatus", "TestAcceptedStandaloneExplicitTransactionReplicatesOriginalSnapshot"},
	"observed-mutation":       {"TestFoundationCommitOwnerAndExactMutations"},
	"view-and-enforcement":    {"TestEngineDecoratorsBoundQueryLimitProviderKeepsLiveLimits", "TestFoundationCarriedEnforcementAndLease"},
	"structural-policy":       {"TestFoundationPublicRowBindingWork", "TestFoundationPublicLogicalObligations", "TestFoundationExternalPublicLogicalObligations", "TestReadBoundaryPolicyCompleteness", "TestFoundationReadBindingRebindsWithoutRowDiscovery", "TestEnginePopulationCoverage", "TestEnginePopulationAdapters"},
	"exact-owner":             {"TestFoundationCommitOwnerAndExactMutations", "TestReadCapabilityBypassGuard"},
	"capability-boundary":     {"TestReadCapabilityBypassGuard", "TestReadCapabilityBypassMutationControl", "TestCapabilityExecutionPopulationNegativeControls"},
	// Context-only exclusion controls remain independently executable; these
	// unrelated values and services are not direct population members.
	"excluded-value-or-service": {"TestCapabilityExecutionPopulationCoverage"},
}

func capabilityPopulationLedger(t *testing.T, population string) []capabilitycensus.Site {
	t.Helper()
	return capabilityPopulationFile(t, population, filepath.Join("testdata", "capability-populations", population+".tsv"))
}

func capabilityPopulationFile(t *testing.T, population, path string) []capabilitycensus.Site {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.Comment = '#'
	reader.FieldsPerRecord = 8
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	var sites []capabilitycensus.Site
	for _, row := range rows {
		sites = append(sites, capabilitycensus.Site{Population: population, Key: row[0], Digest: row[1], Kind: row[2], Contract: row[3], Proof: row[4], Evidence: capabilitycensus.SiteEvidence{OwnerDigest: row[5]}, Disposition: row[6], Reason: row[7]})
	}
	return sites
}

func TestCapabilityExecutionPopulationCoverage(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	// Direct source and containing-owner pins detect changes; the all-source
	// inventory is context, not a second approval population.
	require.Empty(t, capabilitycensus.Problems(result.Sites), "a ledger entry cannot authorize a policy bypass")
	names, err := capabilitycensus.TestNames("../..")
	require.NoError(t, err)
	populations := map[string][]capabilitycensus.Site{}
	for _, site := range result.Mechanisms {
		populations[site.Population] = append(populations[site.Population], site)
		tests, ok := capabilityProofs[site.Proof]
		require.True(t, ok, "%s has no property family", site.Key)
		for _, name := range tests {
			require.True(t, names[name], "%s proof %s does not name an executable test", site.Key, name)
		}
	}
	files, err := filepath.Glob("testdata/capability-populations/*.tsv")
	require.NoError(t, err)
	require.Len(t, files, len(populations), "missing or stale population list")
	for population, sites := range populations {
		t.Run(population, func(t *testing.T) {
			require.Empty(t, capabilitycensus.CoverageProblems(sites, capabilityPopulationLedger(t, population)))
			t.Logf("%s: %d checked members", population, len(sites))
		})
	}
	members, err := os.ReadFile("testdata/capability-populations/engines.txt")
	require.NoError(t, err)
	require.Equal(t, strings.TrimSpace(string(members)), strings.Join(result.Engines, "\n"), "new Engine member needs own policy, factory and independent oracle in TestEnginePopulationCoverage")
	t.Logf("full module: %d packages, %d concrete Engine members", len(result.Packages), len(result.Engines))
}

func TestCapabilityExecutionPopulationNegativeControls(t *testing.T) {
	// Author: Lane #1644 (Codex). Compiled, typed outside-package members are
	// the control. The previous untyped pseudocode remains a text cross-check
	// and is no longer admitted to the classified population.
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26.0\n",
		"pkg/storage/engine.go": `package storage
 type NodeID string
 type Node struct{}
 type Engine interface{GetNode(NodeID)(*Node,error);UpdateNode(*Node)error;GetInnerEngine()Engine}
 type ObservedNodeUpdater interface{UpdateNodeObserved(*Node,*Node)error}
 type MembershipBinding struct{Owner Engine}
 func BindMembership(e Engine)MembershipBinding{return MembershipBinding{Owner:e}}
 func ReadWithCapability(e Engine)error{_,err:=e.GetNode("n");return err}
 func ViewMetadataForEngine[T any](e Engine)(T,bool){v,ok:=any(e).(T);return v,ok}
 `,
		"plugins/unfamiliar/new_owner.go": `package unfamiliar
 import "github.com/orneryd/nornicdb/pkg/storage"
 var engine storage.Engine
 var provider=storage.BindMembership(engine)
 func opaque(values []int){for range values {}}
 func opaqueChain(e storage.Engine){for e!=nil{e=e.GetInnerEngine()}}
 func operation(e storage.Engine){
  _=storage.ReadWithCapability(e)
  for i:=0;i<2;i++{_,_=e.GetNode("n")}
  _=e.UpdateNode(nil)
  if observed,ok:=e.(storage.ObservedNodeUpdater);ok{_=observed.UpdateNodeObserved(nil,nil)}
 }
 type retained struct{source storage.Engine}
 func writer(v *retained,source storage.Engine){v.source=source}
 func opaqueAlias(e storage.Engine)storage.MembershipBinding{hidden:=storage.BindMembership;return hidden(e)}
 func BindMembership(string)string{return "ordinary service"}
 func sameName(){_=BindMembership("value")}
 func Walk[T any](values []*T){for range values{}}
 func renamedGenericConsumer(values []*storage.Node){Walk(values)}
 type namedView struct{storage.Engine;scope string}
 func(v *namedView)Namespace()string{return v.readScope()}
 func(v *namedView)readScope()string{return v.scope}
 func metadataConsumer(e storage.Engine){v,ok:=storage.ViewMetadataForEngine[interface{Namespace()string}](e);if ok{_=v.Namespace()}}
 func createView(e storage.Engine,scope string)*namedView{return &namedView{Engine:e,scope:scope}}
 func updateScope(v *namedView,scope string){v.scope=scope}
 `,
		"plugins/unfamiliar/inactive_windows.go": "//go:build windows\n\npackage unfamiliar\nfunc TextOnly(){for{}}\n",
	}
	for name, source := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(source), 0644))
	}
	inventory, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	result, err := capabilitycensus.Interpret(inventory)
	require.NoError(t, err)
	var sites []capabilitycensus.Site
	for _, site := range result.Sites {
		if site.Path == "plugins/unfamiliar/new_owner.go" {
			sites = append(sites, site)
		}
	}
	require.NotEmpty(t, sites)
	for _, site := range sites {
		require.Empty(t, site.Disposition, "candidate discovery must never assign a semantic disposition")
		require.Empty(t, site.Reason, "discovery must leave every member OPEN until individually reviewed: %s", site.Key)
		require.Equal(t, "plugins/unfamiliar/new_owner.go", site.Path)
	}

	// Commit discovery requires real types; its renamed/non-commit controls
	// run in TestCapabilityCommitTypesAndRenamedProviders, not this untyped input.
	for _, population := range []string{"lookups", "assertions", "fallbacks_streams", "observed_mutations"} {
		var members []capabilitycensus.Site
		for _, site := range sites {
			if site.Population == population {
				members = append(members, site)
			}
		}
		require.NotEmpty(t, members, population)
		require.NotEmpty(t, capabilitycensus.CoverageProblems(members, nil), "new %s member must fail before registration", population)
		require.NotEmpty(t, capabilitycensus.CoverageProblems(members, members), "a discovered candidate is OPEN, never its own semantic approval")
		// These fixture-only records exercise canonical consistency. They are not
		// production reviews, and no real member receives a default disposition.
		registered := append([]capabilitycensus.Site(nil), members...)
		for i := range registered {
			registered[i].Disposition = "distinct responsibility"
			registered[i].Reason = "Controlled negative fixture: this exact selected expression tests census identity and policy rejection, not a production storage provider. Selected expression: " + members[i].Detail
		}
		require.Empty(t, capabilitycensus.CoverageProblems(members, registered))
		changed := append([]capabilitycensus.Site(nil), registered...)
		changed[0].Proof = "invented-proof"
		require.NotEmpty(t, capabilitycensus.CoverageProblems(members, changed))
		require.NotEmpty(t, capabilitycensus.CoverageProblems(nil, registered), "stale evidence must fail")
		require.NotEmpty(t, capabilitycensus.CoverageProblems(members, append(changed, registered...)), "duplicate evidence must fail")
		changed = append([]capabilitycensus.Site(nil), registered...)
		changed[0].Disposition = ""
		require.NotEmpty(t, capabilitycensus.CoverageProblems(members, changed), "unread members stay OPEN")
		changed = append([]capabilitycensus.Site(nil), registered...)
		changed[0].Evidence.OwnerDigest = "stale-owner"
		require.NotEmpty(t, capabilitycensus.CoverageProblems(members, changed), "containing source owner must match the reviewed body")
		changed = append([]capabilitycensus.Site(nil), registered...)
		changed[0].Reason = members[0].Function + ": " + members[0].Contract
		require.NotEmpty(t, capabilitycensus.CoverageProblems(members, changed), "the old emitted reason cannot be frozen as a semantic review")
	}
	problems := strings.Join(capabilitycensus.Problems(sites), "\n")
	require.Contains(t, problems, "independent physical wrapper progression")
	foundInitializer, foundWriter, foundAlias := false, false, false
	for _, site := range result.Sites {
		require.NotEqual(t, "plugins/unfamiliar/inactive_windows.go", site.Path, "inactive syntax is a cross-check, not a compiled member")
		if site.Population != "lookups" {
			continue
		}
		foundInitializer = foundInitializer || strings.Contains(site.Function, "provider")
		foundWriter = foundWriter || strings.HasSuffix(site.Function, ".writer") && site.Kind == "assign"
		foundAlias = foundAlias || strings.HasSuffix(site.Function, ".opaqueAlias") && site.Kind == "call"
		require.False(t, strings.HasSuffix(site.Function, ".sameName"), "a same-named service is not the storage protocol object")
	}
	require.True(t, foundInitializer, "compiled initializer must be queried")
	require.True(t, foundWriter, "typed field writer must be queried")
	require.True(t, foundAlias, "canonical alias target must be queried")
	foundGeneric := false
	for _, site := range result.Sites {
		foundGeneric = foundGeneric || site.Population == "fallbacks_streams" && site.Kind == "range" && strings.HasSuffix(site.Function, ".Walk")
	}
	require.True(t, foundGeneric, "actual node instantiation must select the shared generic source loop")
	foundConstructor, foundMetadataWriter := false, false
	for _, site := range result.Sites {
		if site.Population != "lookups" {
			continue
		}
		foundConstructor = foundConstructor || site.Kind == "composite" && strings.HasSuffix(site.Function, ".createView")
		foundMetadataWriter = foundMetadataWriter || site.Kind == "assign" && strings.HasSuffix(site.Function, ".updateScope")
	}
	require.True(t, foundConstructor, "provider constructor is a real typed field producer")
	require.True(t, foundMetadataWriter, "metadata field reached through a helper must retain its writers")
	for _, site := range sites {
		require.False(t, strings.HasSuffix(site.Function, ".opaque"), "an unrelated plain value loop is not storage read work")
	}

}

var foundationCensusReviewOutput = flag.Bool("foundation-census-review-output", false, "emit canonical source candidates for explicit semantic review; never updates approvals")

func TestCapabilityPopulationReviewCandidates(t *testing.T) {
	if !*foundationCensusReviewOutput {
		t.Skip("review artifact export not requested")
	}
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	payload := struct {
		Result   capabilitycensus.Result `json:"result"`
		Sources  any                     `json:"sources"`
		Problems []string                `json:"problems"`
	}{result, result.Inventory.Sources, capabilitycensus.Problems(result.Sites)}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	file, err := writer.Create("canonical-review-candidates.json")
	require.NoError(t, err)
	_, err = file.Write(raw)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	counts := map[string]int{}
	for _, site := range result.Sites {
		counts[site.Population]++
	}
	t.Logf("CANONICAL_CENSUS_COUNTS %v Engines=%d StoredViews=%d Packages=%d", counts, len(result.Types), len(result.StoredViews), len(result.Packages))
	direct := map[string]int{}
	for _, site := range result.Mechanisms {
		direct[site.Population]++
	}
	t.Logf("DIRECT_MECHANISM_COUNTS %v", direct)
	t.Logf("CANONICAL_REVIEW_ZIP_BASE64 %s", base64.StdEncoding.EncodeToString(output.Bytes()))
}

func TestCapabilityPopulationDomainOrderIndependence(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	inventory := result.Inventory
	inventory.Members = append([]workcensus.Member(nil), inventory.Members...)
	for i, j := 0, len(inventory.Members)-1; i < j; i, j = i+1, j-1 {
		inventory.Members[i], inventory.Members[j] = inventory.Members[j], inventory.Members[i]
	}
	reversed, err := capabilitycensus.Interpret(inventory)
	require.NoError(t, err)
	require.Equal(t, result.Engines, reversed.Engines)
	require.Equal(t, result.Sites, reversed.Sites, "domain roles must not depend on map/source encounter order")
	require.Equal(t, result.Mechanisms, reversed.Mechanisms, "direct membership must not depend on inventory encounter order")
}
