package capturecensus

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

type storageSourceProof struct {
	Present bool     `json:"present"`
	File    string   `json:"file"`
	Digest  string   `json:"sourceSHA256"`
	Fresh   []string `json:"freshContextCalls"`
}
type storageRetirement struct {
	Proof  storageSourceProof `json:"proof"`
	Reason string             `json:"reason"`
}
type storageReconciliation struct {
	RepinnedSources map[string]storageSourceProof `json:"repinnedExistingSources"`
	Historical      struct {
		Records int    `json:"claimedRecords"`
		Sites   int    `json:"claimedFreshContexts"`
		Status  string `json:"status"`
		Reason  string `json:"reason"`
	} `json:"historicalHeldClaims"`
	RetiredOwners  map[string]storageRetirement  `json:"retiredCrossUnitOwners"`
	RetiredSources map[string]storageRetirement  `json:"retiredContextSources"`
	FormerRoots    map[string]storageSourceProof `json:"currentFormerSourceRoots"`
}

func readStorageReconciliation(t *testing.T) storageReconciliation {
	t.Helper()
	data, err := os.ReadFile("storage_reconciliation.json")
	require.NoError(t, err)
	var record storageReconciliation
	require.NoError(t, json.Unmarshal(data, &record))
	return record
}

// Tombstones explain why exact former entries disappeared. They confer no
// acceptance on replacement paths; the normal compiled census still judges them.
func TestStorageReconciliationRecords(t *testing.T) {
	record := readStorageReconciliation(t)
	require.Equal(t, 20, record.Historical.Records)
	require.Equal(t, 23, record.Historical.Sites)
	require.Equal(t, "unqualified", record.Historical.Status)
	require.NotEmpty(t, record.Historical.Reason)
	require.Equal(t, 51, len(StoragePolicy().CrossUnitOwners)+len(record.RetiredOwners), "every former cross-unit entry must remain active or have a source-pinned tombstone")
	require.Len(t, record.RetiredSources, 4)
	configuration := storageNilContextConfig(t, "./...")
	configuration.IncludeProgram = true
	configuration.Selectors = []workcensus.Selector{{Node: "declaration", Kind: "declaration", Reachability: "all"}}
	inventory, err := workcensus.Extract(configuration)
	require.NoError(t, err)
	program := inventory.Program
	digests := newSourceDigests(program)
	digests.sourceFiles = inventory.Sources
	require.Empty(t, applyNonNilResultContracts(program, StoragePolicy(), digests), "the reviewed provider must revalidate against this complete current program")
	provider := StoragePolicy().NonNilResults["(*github.com/orneryd/nornicdb/pkg/lifecycle.WorkLifetime).Context"]
	require.Equal(t, 0, provider.Index)
	require.Equal(t, []string{"(*github.com/orneryd/nornicdb/pkg/lifecycle.WorkLifetime).initContext"}, provider.Review.FieldWriters["github.com/orneryd/nornicdb/pkg/lifecycle.WorkLifetime.ctx"])
	check := func(symbol string, proof storageSourceProof) {
		t.Helper()
		if !proof.Present {
			assert.Equal(t, "missing", digests.of(symbol), "%s: retired function reappeared; review its current flow", symbol)
			return
		}
		require.NotEmpty(t, proof.File)
		require.NotEmpty(t, proof.Digest)
		assert.Equal(t, proof.Digest, digests.of(symbol), "%s: reconciliation source changed; re-review instead of carrying its proof forward", symbol)
		sites := map[ssa.Value]bool{}
		for _, fn := range program.Functions {
			if symbolOf(workcensus.OutermostFunction(fn)) != symbol {
				continue
			}
			for _, block := range fn.Blocks {
				for _, instruction := range block.Instrs {
					if call, ok := instruction.(*ssa.Call); ok && freshContext(call) {
						sites[call] = true
					}
				}
			}
		}
		assert.Len(t, sites, len(proof.Fresh), "%s: fresh contexts must stay inventoried even after a former source is retired", symbol)
	}
	for symbol, entry := range record.RetiredOwners {
		require.NotEmpty(t, entry.Reason)
		require.NotContains(t, StoragePolicy().CrossUnitOwners, symbol)
		require.Empty(t, entry.Proof.Fresh)
		check(symbol, entry.Proof)
	}
	for symbol, entry := range record.RetiredSources {
		require.NotEmpty(t, entry.Reason)
		require.False(t, entry.Proof.Present)
		check(symbol, entry.Proof)
	}
	for symbol, proof := range record.RepinnedSources {
		check(symbol, proof)
	}
	for symbol, proof := range record.FormerRoots {
		check(symbol, proof)
	}
}

// File-level pins cover declarations that have no named SSA function, including
// anonymous package initializers. Validate this narrow source population without
// running the whole-module context census.
func TestReviewedProviderPackageSourceFiles(t *testing.T) {
	configuration := storageNilContextConfig(t, "./pkg/lifecycle")
	configuration.SourceOnly = true
	configuration.Selectors = []workcensus.Selector{{Node: "declaration", Kind: "declaration", Reachability: "all"}}
	inventory, err := workcensus.Extract(configuration)
	require.NoError(t, err)
	actual := reviewedPackageSourceFiles(inventory.Sources, "github.com/orneryd/nornicdb/pkg/lifecycle")
	require.NotEmpty(t, actual)
	provider := StoragePolicy().NonNilResults["(*github.com/orneryd/nornicdb/pkg/lifecycle.WorkLifetime).Context"]
	require.Equal(t, provider.PackageFiles, actual, "provider declarations and initializer bytes must match their reviewed file pins")
}
