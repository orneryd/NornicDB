package workcensus

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var storageClasses = []string{"contextual statement owner", "checked preparation iteration", "bounded", "non-statement boundary", "owned by another layer", "local statement work", "distinct responsibility", imageHandoffClass, nilContextClass, schemaHandoffClass, compatibilityContextClass, namespaceHandoffClass, directionalHandoffClass}
var envelopeClasses = []string{"emitter", "detection/migration", "shared decoder", "unrelated codec"}

func storagePolicy() Policy {
	return Policy{Classes: storageClasses, ClassKinds: map[string]string{"checked preparation iteration": "statement", "local statement work": "statement", "distinct responsibility": "review", imageHandoffClass: "review", nilContextClass: "review", schemaHandoffClass: "review", compatibilityContextClass: "review", namespaceHandoffClass: "review", directionalHandoffClass: "review"}, RequireOwnerDigest: true, RequireReviewOwner: true, Owners: []string{"#1667"}, RequireSubjectsFor: []string{"contextual statement owner", "checked preparation iteration", imageHandoffClass, nilContextClass, schemaHandoffClass, compatibilityContextClass, namespaceHandoffClass, directionalHandoffClass}, FactKeys: []string{"operation", "input type", "call targets", "context", "iteration guard", "context proof"}}
}

func envelopePolicy() Policy {
	p := Policy{Classes: envelopeClasses, RequireOwnerDigest: true, ClassKinds: map[string]string{}}
	for _, class := range envelopeClasses {
		p.ClassKinds[class] = "review"
	}
	return p
}

func source(t *testing.T, file string) (string, []byte) {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("../..", file))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, raw
}

func approved(t *testing.T, file string) Inventory {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var inv Inventory
	if err := json.Unmarshal(raw, &inv); err != nil {
		t.Fatal(err)
	}
	return inv
}

func populations(inv Inventory) (work, envelope Inventory) {
	relations := storageOwnerRelations(inv)
	work.Roots = inv.Roots
	work.Definitions, envelope.Definitions = inv.Definitions, inv.Definitions
	for _, m := range inv.Members {
		if m.Kind == "owner-call" {
			continue
		}
		if strings.HasPrefix(m.Kind, "envelope.") {
			envelope.Members = append(envelope.Members, m)
		} else {
			m.Facts = map[string][]string{"operation": {m.Node, strings.Split(m.Code, "\n")[0]}, "input type": {m.OperandType}, "call targets": m.Callees, "context": {m.Scope}}
			r := relations[m.ID]
			if len(r.ContextProof) > 0 {
				m.Facts["context proof"] = r.ContextProof
			}
			if r.Guard != "" {
				m.Facts["iteration guard"] = []string{r.Guard, r.GuardContext, r.GuardError}
			}
			work.Members = append(work.Members, m)
		}
	}
	return
}

func TestStorageWorkCensusDiscovery(t *testing.T) {
	if flag.Lookup("test.run").Value.String() != "^TestStorageWorkCensusDiscovery$" {
		t.Skip("explicit discovery-only selector required")
	}
	inv, err := Extract(storageConfig("../.."))
	if err != nil {
		t.Fatal(err)
	}
	work, envelope := populations(inv)
	members := append(work.Members, envelope.Members...)
	for _, m := range members {
		b, _ := json.Marshal(m)
		fmt.Println("CENSUS_MEMBER " + string(b))
	}
	b, _ := json.Marshal(inv.Roots)
	fmt.Println("CENSUS_ROOTS " + string(b))
	t.Fatal("discovery is evidence, never approval; review candidates and classify explicitly")
}

func TestStorageWorkCensus(t *testing.T) {
	inv, err := Extract(storageConfig("../.."))
	if err != nil {
		t.Fatal(err)
	}
	work, envelope := populations(inv)
	boundaries := storageContextBoundaries(inv)
	for _, p := range []struct {
		actual Inventory
		file   string
		policy Policy
	}{
		{work, "storage.json", storagePolicy()}, {envelope, "envelope.json", envelopePolicy()}, {boundaries, "storage-context-boundaries.json", storageContextBoundaryPolicy()},
	} {
		a := approved(t, p.file)
		for _, f := range validateStorageContextClasses(inv, a) {
			t.Error(f)
		}
		for _, f := range Validate(p.actual, a, p.policy) {
			t.Error(f)
		}
		// The shared validator checks this class against freshly computed
		// facts, even if someone refreshes ordinary member/body digests.
		if p.file == "storage.json" {
			for _, f := range validateStorageIterationClass(p.actual, a) {
				t.Error(f)
			}
		}
		counts := map[string]int{}
		for _, m := range a.Members {
			counts[m.Class]++
		}
		t.Logf("%s: %d members, classes %v", p.file, len(p.actual.Members), counts)
	}
	// The former human exemptions contradict the same extracted population.
	// Assert these conditions even after correcting the approved manifest.
	for _, class := range []string{"non-statement boundary", "owned by another layer"} {
		found := false
		for _, m := range work.Members {
			if !strings.Contains(m.Owner, "pendingValueMatchesLockedWithContext") || m.Kind != "range" {
				continue
			}
			claim := m
			claim.Class, claim.Reason = class, "former preparation exemption"
			failures := Validate(Inventory{Roots: work.Roots, Definitions: work.Definitions, Members: []Member{m}}, Inventory{Roots: work.Roots, Members: []Member{claim}}, storagePolicy())
			want := "REACHABLE NON-STATEMENT"
			if class == "owned by another layer" {
				want = "MISSING OWNER LAYER"
			}
			found = strings.Contains(strings.Join(failures, "\n"), want)
			break
		}
		if !found {
			t.Error("old pending preparation disposition escaped consistency: " + class)
		}
	}
	// Graphify's release affected query reports no incoming callers for the
	// transaction property matcher; typed SSA and the wrapper source expose it.
	// Keep that difference from becoming another reviewed population omission.
	t.Run("graph-missed indexed property filter", func(t *testing.T) {
		for _, m := range work.Members {
			if !strings.Contains(m.Owner, ").MergePendingPropertyMatchesWithContext") || m.Kind != "range" {
				continue
			}
			if !m.Reachable {
				t.Fatal("typed statement reachability lost", m.ID)
			}
			omitted := approved(t, "storage.json")
			kept := omitted.Members[:0]
			for _, row := range omitted.Members {
				if row.ID != m.ID {
					kept = append(kept, row)
				}
			}
			omitted.Members = kept
			failures := Validate(work, omitted, storagePolicy())
			if len(failures) != 1 || failures[0] != "UNLISTED "+m.ID {
				t.Fatal("graph-derived omission escaped the real validator", failures)
			}
			return
		}
		t.Fatal("indexed property filter absent from extracted population")
	})
	for _, owner := range []string{
		").pendingAdjacencyLocked", ").currentTemporalNodeByScanInNamespace", ").GetConstraintsForLabelsWithContext",
	} {
		t.Run("graph-missed "+owner, func(t *testing.T) {
			for _, member := range work.Members {
				if !strings.Contains(member.Owner, owner) || member.Kind != "range" {
					continue
				}
				if !member.Reachable {
					t.Fatal("typed statement reachability lost", member.ID)
				}
				omitted := approved(t, "storage.json")
				kept := omitted.Members[:0]
				for _, row := range omitted.Members {
					if row.ID != member.ID {
						kept = append(kept, row)
					}
				}
				omitted.Members = kept
				failures := Validate(work, omitted, storagePolicy())
				if len(failures) != 1 || failures[0] != "UNLISTED "+member.ID {
					t.Fatal("graph-derived omission escaped real validator", failures)
				}
				return
			}
			t.Fatal("source/SSA preparation member absent", owner)
		})
	}
}

// Public cold-ledger and WAL tests reach these through maintained packages
// outside the storage client's original initial load. They must be source
// population members before any approval data is considered.
func TestStoragePreparationPopulationControl(t *testing.T) {
	inv, err := Extract(storageConfig("../.."))
	if err != nil {
		t.Fatal(err)
	}
	work, _ := populations(inv)
	for _, owner := range []string{
		"StoredEntitySnapshot).ForEachWithContext$",
		"PreparedEntityCommit).ChangesWithContext",
		"StoredEntityCountDeltaWithContext",
		"WALEngine).tryContextEntityOperations",
	} {
		t.Run(owner, func(t *testing.T) {
			var member *Member
			for i := range work.Members {
				m := &work.Members[i]
				if strings.HasSuffix(m.Owner, owner) && (m.Kind == "range" || m.Kind == "for") {
					member = m
					break
				}
			}
			if member == nil || !member.Reachable {
				t.Fatal("publicly reached preparation missing from rooted extraction:", owner)
			}
			// Removing this exact row must fail ordinary shared Validate. Other
			// source/semantic rows stay open until their independent review.
			actual := Inventory{Roots: work.Roots, Definitions: work.Definitions, Members: []Member{*member}}
			omitted := Inventory{Roots: work.Roots}
			for _, failure := range Validate(actual, omitted, storagePolicy()) {
				if failure == "UNLISTED "+member.ID {
					t.Log(failure)
					return
				}
			}
			t.Fatal("omitted preparation escaped Validate:", member.ID)
		})
	}
}

// One composed production overlay exercises the same SSA extractor and strict
// manifests as the gate. Each mutation must fail for its own new/changed work.
func TestStorageWorkCensusMutationControls(t *testing.T) {
	path, raw := source(t, "pkg/storage/badger_constraint_read.go")
	needle := []byte("captured, readErr := b.readBufferedEntitiesWithContext(ctx, w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})")
	if bytes.Count(raw, needle) != 1 {
		t.Fatal("real buffered constraint route moved")
	}
	raw = bytes.Replace(raw, needle, []byte("captured := b.readBufferedEntities(w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})\nvar readErr error"), 1)
	needle = []byte("}, func(txn *badger.Txn) error {")
	if bytes.Count(raw, needle) != 1 {
		t.Fatal("constraint body reader moved")
	}
	raw = bytes.Replace(raw, needle, append(append([]byte{}, needle...), []byte("\ncensusUnlistedIterator(txn)\n")...), 1)
	raw = append(raw, []byte(`
func censusUnlistedIterator(txn *badger.Txn) {
 it := txn.NewIterator(badger.DefaultIteratorOptions)
 defer it.Close()
 for it.Rewind(); it.Valid(); it.Next() { _ = it.Item() }
}
`)...)
	decoder, body := source(t, "pkg/storage/projected_node_decoder.go")
	body = append(body, []byte(`
func censusSwitchEnvelope(body []byte) []byte {
 if len(body)==0 { return nil }
 switch body[0] {
 case nodeFormatTokenizedV1:
  length,n:=binary.Uvarint(body[1:])
  if n>0 && length<=uint64(len(body)-1-n) { return body[1+n:1+n+int(length)] }
 }
 return nil
}
`)...)
	c := storageConfig("../..")
	c.Overlay = map[string][]byte{path: raw, decoder: body}
	inv, err := Extract(c)
	if err != nil {
		t.Fatal(err)
	}
	w, e := populations(inv)
	wa, ea := approved(t, "storage.json"), approved(t, "envelope.json")
	wfail, efail := Validate(w, wa, storagePolicy()), Validate(e, ea, envelopePolicy())
	for _, probe := range []struct {
		name, code, owner, kind string
		pop                     Inventory
		failures                []string
	}{
		{"new helper iterator", "NewIterator", "censusUnlistedIterator", "iterator", w, wfail},
		{"real buffered Background route", "b.readBufferedEntities(w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})", "keyConstraintNodesWithContext", "entrance", w, wfail},
		{"switch framing reader", "binary.Uvarint", "censusSwitchEnvelope", "envelope.varint", e, efail},
	} {
		t.Run(probe.name, func(t *testing.T) {
			found := false
			for _, m := range probe.pop.Members {
				if m.Kind != probe.kind || !strings.Contains(m.Owner, probe.owner) || !strings.Contains(m.Code, probe.code) {
					continue
				}
				for _, f := range probe.failures {
					if strings.HasSuffix(f, m.ID) && (strings.HasPrefix(f, "UNLISTED ") || strings.HasPrefix(f, "CHANGED ")) {
						found = true
						t.Log(f)
					}
				}
			}
			if !found {
				t.Fatalf("mutation escaped the production population gate: %s", probe.name)
			}
		})
	}
}
