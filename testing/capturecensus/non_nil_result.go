package capturecensus

import (
	"fmt"
	"go/types"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// ReviewedNonNilResult pins a reviewed normal-return contract to its complete
// relevant source closure and field-writer population. It confers no ownership
// or request-substitution exemption and makes no claim about other tuple slots.
type ReviewedNonNilResult struct {
	Index            int
	PackageFunctions []string
	PackageFiles     map[string]string
	Review           ReviewedHeldValue
}

func applyNonNilResultContracts(program *workcensus.Program, policy Policy, digests *sourceDigests) []string {
	var violations []string
	var contracts []workcensus.NonNilResultContract
	keys := make([]string, 0, len(policy.NonNilResults))
	for key := range policy.NonNilResults {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		record := policy.NonNilResults[key]
		var candidates int
		for _, function := range program.Functions {
			if symbolOf(function) != key || function.Syntax() == nil {
				continue
			}
			candidates++
			stale := reviewedHeldStale(program, function, record.Review, nil, digests)
			files := reviewedPackageSourceFiles(digests.sourceFiles, function.Pkg.Pkg.Path())
			if len(files) == 0 || !equalSourceFilePins(files, record.PackageFiles) {
				stale += "; provider package source-file population/content changed"
			}
			population := reviewedPackageFunctions(program, function.Pkg.Pkg.Path())
			if strings.Join(population, "\n") != strings.Join(record.PackageFunctions, "\n") {
				stale += "; provider package function population changed"
			}
			for _, member := range population {
				if _, pinned := record.Review.Sources[member]; !pinned {
					stale += "; unpinned provider package function: " + member
				}
			}

			// Receiver context fields are part of a provider's mutable proof
			// surface. Their writer population cannot silently be omitted.
			if receiver := function.Signature.Recv(); receiver != nil {
				typ := types.Unalias(receiver.Type())
				if pointer, ok := typ.(*types.Pointer); ok {
					typ = types.Unalias(pointer.Elem())
				}
				if named, ok := typ.(*types.Named); ok && named.Obj().Pkg() != nil {
					identity := named.Obj().Pkg().Path() + "." + named.Obj().Name()
					if _, pinned := record.Review.Methods[identity]; !pinned {
						stale += "; missing receiver method population: " + identity
					}
					if fields, ok := named.Underlying().(*types.Struct); ok {
						for index := 0; index < fields.NumFields(); index++ {
							field := fields.Field(index)
							if !isContext(field.Type()) {
								continue
							}
							key := identity + "." + field.Name()
							if _, pinned := record.Review.FieldWriters[key]; !pinned {
								stale += "; missing receiver context-field writer population: " + key
							}
						}
					}
				}
			}
			if record.Review.Reason == "" || record.Review.Approver == "" || record.Review.Closure == "" {
				stale += "; incomplete reviewed result-contract evidence"
			}
			if stale != "" {
				violations = append(violations, key+": reviewed non-nil result contract is stale: "+stale)
				continue
			}
			contracts = append(contracts, workcensus.NonNilResultContract{Function: function, Index: record.Index, Evidence: record.Review.Approver + ": " + record.Review.Reason})
		}
		if candidates == 0 {
			violations = append(violations, key+": reviewed non-nil result contract matches no exact source function")
		}
	}
	if len(violations) > 0 {
		// No partly validated policy is installed; this also removes any previous
		// registration when the same Program is re-analyzed with changed records.
		if err := program.SetNonNilResultContracts(nil); err != nil {
			violations = append(violations, err.Error())
		}
		return violations
	}
	if err := program.SetNonNilResultContracts(contracts); err != nil {
		violations = append(violations, fmt.Sprintf("reviewed non-nil result contract: %v", err))
		if resetErr := program.SetNonNilResultContracts(nil); resetErr != nil {
			violations = append(violations, resetErr.Error())
		}
	}
	return violations
}

// Pin the complete provider package surface as a conservative backstop for
// aggregate overwrites or new helpers that direct field-store discovery can
// miss. This is existing typed function inventory, not another flow model.
func reviewedPackageFunctions(program *workcensus.Program, packagePath string) []string {
	seen := map[string]bool{}
	for _, function := range program.Functions {
		if function.Pkg == nil || function.Pkg.Pkg.Path() != packagePath || function.Syntax() == nil || workcensus.OutermostFunction(function) != function {
			continue
		}
		seen[symbolOf(function)] = true
	}
	out := make([]string, 0, len(seen))
	for symbol := range seen {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

// SourceBoundary is the extraction owner's overlay-aware, physical-file
// fingerprint. Pinning every loaded file/state also covers globals and
// anonymous initialization bodies absent from the declared-function census.
func reviewedPackageSourceFiles(sources []workcensus.SourceBoundary, packagePath string) map[string]string {
	files := map[string]string{}
	for _, source := range sources {
		if source.Package == packagePath {
			files[source.File+" ["+source.State+"]"] = source.Digest
		}
	}
	return files
}

func equalSourceFilePins(actual, reviewed map[string]string) bool {
	if len(actual) != len(reviewed) {
		return false
	}
	for file, digest := range actual {
		if reviewed[file] != digest {
			return false
		}
	}
	return true
}
