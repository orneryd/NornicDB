package storagecensus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/internal/collection"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// Report is the census.
type Report struct {
	Rules []string `json:"rules"`
	// Spaces are the key spaces, with their class and how many writes the
	// census found to each.
	Spaces []SpaceRow `json:"spaces"`
	// Acquisitions are the owners of ordered writes; Exempt the functions
	// whose own writes need none, with the reason.
	Acquisitions []NamedReason `json:"acquisitions"`
	Exempt       []NamedReason `json:"exempt"`
	// Writes are every Badger write found, per calling context that names
	// its key.
	Writes []write `json:"writes"`
	// FieldWrites are the changes of classified state fields.
	FieldWrites []FieldWriteRow `json:"fieldWrites"`
	// Decisions are the branches on hidden state that decide a write;
	// Disposition the functions where such a decision belongs.
	Decisions   []Decision    `json:"decisions"`
	Disposition []NamedReason `json:"disposition"`
	// Pending are second interpretations of the write transition that a
	// stacked change removes; each must still exist.
	Pending []NamedReason `json:"pending"`
	// Violations are entry points that reach a write of ordered state no
	// acquisition covers, writes whose key the census cannot classify, and
	// table rows that no longer match the code.
	Violations []string `json:"violations"`
	// Unordered details the unordered-write violations.
	Unordered []UnorderedRow `json:"unordered"`
}

// UnorderedRow is a write of ordered state that entry points reach with no
// acquisition: every such entry point, and the shortest path.
type UnorderedRow struct {
	Space   string   `json:"space"`
	Class   string   `json:"class"`
	Origin  string   `json:"origin"`
	Entries []string `json:"entries"`
	Chain   []string `json:"chain"`
	// Chains is each entry point's shortest path, for diagnosis only
	// (NORNICDB_CENSUS_CHAINS); the artifact keeps the shortest overall.
	Chains map[string][]string `json:"-"`
}

type SpaceRow struct {
	Space  string `json:"space"`
	Class  string `json:"class"`
	Writes int    `json:"writes"`
}

type NamedReason struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

var rules = []string{
	"Every Badger write is classified by the key it writes: the storage prefix constant its key starts with (after prefixMVCCMeta, the constant or string that follows), traced through key builders, appends, parameters and captured variables, the cells, fields, slices and maps that hold keys (through every writer's callers), and the prefix a scan is bounded to; a key an unbounded scan returns is a write of the whole store. A write whose key cannot be traced, or whose constant has no class, is a violation.",
	"A write of ordered state (" + strings.Join(sortedSet(orderedClasses), ", ") + ", whole-store) runs under an acquisition: a call of one of the listed acquisitions whose error is checked and whose success branch is on every path to the write (in the function or in a caller, along every call path), with no release in between; or, on the branch where the engine has no write-behind buffer, none is needed. A listed lock held exclusively on every path covers the writes of its own key spaces.",
	"A closure or function value is checked where it is made, when it runs only while its maker runs; one that is stored, sent, returned or started runs later and is an entry point of its own, and the writes whose keys it captures are writes its maker makes that no acquisition covers. Calls through interfaces follow the VTA call graph.",
	"An entry point is an exported function or method, an init, a main, or a closure that runs later; an unexported function is reached through its callers (a function nothing calls cannot run). An entry point that reaches a write of ordered state no acquisition covers is a violation.",
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Census analyses the module rooted at root, with overlay replacing or adding
// files (absolute path to contents).
func Census(root string, overlay map[string][]byte) (*Report, error) {
	p, err := load(root, overlay)
	if err != nil {
		return nil, err
	}
	k, err := newKeys(p)
	if err != nil {
		return nil, err
	}
	a := &analysis{p: p, k: k, sums: map[*ssa.Function]*summary{}, writes: map[string]write{},
		acq: &acquirer{p: p, cache: map[flowKey]*flow{}}}
	a.fixpoint()
	a.markBodyWriters()
	return a.report(), nil
}

func (a *analysis) report() *Report {
	r := &Report{Rules: rules}
	names := map[string]*ssa.Function{}
	for _, fn := range a.p.funcs {
		names[a.p.funcName(fn)] = fn
	}
	for _, name := range collection.SortedKeys(acquisitions) {
		r.Acquisitions = append(r.Acquisitions, NamedReason{name, acquisitions[name].reason})
		if names[name] == nil {
			r.Violations = append(r.Violations, "stale: acquisition "+name+" does not exist")
		}
	}
	for _, name := range lockNames {
		r.Acquisitions = append(r.Acquisitions, NamedReason{name + " (held exclusively)", lockAcquisitions[name].reason})
		if _, field := a.lookupField(name); field == nil {
			r.Violations = append(r.Violations, "stale: lock acquisition "+name+" names no field")
		}
	}
	for _, name := range collection.SortedKeys(constructors) {
		r.Acquisitions = append(r.Acquisitions, NamedReason{name + " (constructor, until it creates the write-behind buffer)", constructors[name]})
		if names[name] == nil {
			r.Violations = append(r.Violations, "stale: constructor "+name+" does not exist")
		}
	}
	for _, name := range collection.SortedKeys(lockHelpers) {
		h := lockHelpers[name]
		r.Acquisitions = append(r.Acquisitions, NamedReason{name + " (lock helper for " + h.lock + ")", h.reason})
		if names[name] == nil {
			r.Violations = append(r.Violations, "stale: lock helper "+name+" does not exist")
		}
		if _, ok := lockAcquisitions[h.lock]; !ok {
			r.Violations = append(r.Violations, "stale: lock helper "+name+" names no lock acquisition")
		}
	}
	for _, name := range collection.SortedKeys(exemptOwners) {
		r.Exempt = append(r.Exempt, NamedReason{name, exemptOwners[name]})
		if names[name] == nil {
			r.Violations = append(r.Violations, "stale: exemption "+name+" does not exist")
		}
	}
	// Every storage prefix constant has a class, and every class row names
	// a constant that exists.
	counts := map[string]int{}
	for _, id := range collection.SortedKeys(a.writes) {
		w := a.writes[id]
		r.Writes = append(r.Writes, w)
		counts[w.Space]++
		if w.Class == "unclassified" {
			r.Violations = append(r.Violations, fmt.Sprintf("unclassified key: %s in %s (resolved at %s): %s", w.Origin, w.Func, w.Where, w.Why))
		}
	}
	known := map[string]bool{}
	for _, n := range a.k.first {
		known[n] = true
	}
	for _, n := range a.k.meta {
		known[n] = true
	}
	for n := range known {
		if spaceClasses[n] == "" {
			r.Violations = append(r.Violations, "unclassified key space: storage constant "+n+" has no class")
		}
	}
	for _, n := range collection.SortedKeys(spaceClasses) {
		if !known[n] && !strings.Contains(n, `"`) && n != "prefixMVCCMeta" && !strings.HasPrefix(n, "retired ") {
			r.Violations = append(r.Violations, "stale: key space "+n+" is no storage constant")
		}
		r.Spaces = append(r.Spaces, SpaceRow{Space: n, Class: spaceClasses[n], Writes: counts[n]})
	}
	// One violation per unordered write: the entry points that reach it,
	// counted, and the shortest of their paths.
	type unordered struct {
		q       req
		entries []string
		chains  map[string][]string
	}
	byWrite := map[string]*unordered{}
	for _, fn := range a.p.funcs {
		s := a.sums[fn]
		if s == nil || len(s.reqs) == 0 || !a.entry(fn) {
			continue
		}
		for _, id := range collection.SortedKeys(s.reqs) {
			q := s.reqs[id]
			if q.class == "unclassified" {
				continue // reported as an unclassified key
			}
			u := byWrite[id]
			if u == nil {
				u = &unordered{q: q, chains: map[string][]string{}}
				byWrite[id] = u
			} else if shorter(q.chain, u.q.chain) {
				u.q = q
			}
			u.entries = append(u.entries, a.p.funcName(fn))
			u.chains[a.p.funcName(fn)] = q.chain
		}
	}
	for _, id := range collection.SortedKeys(byWrite) {
		u := byWrite[id]
		sort.Strings(u.entries)
		r.Unordered = append(r.Unordered, UnorderedRow{Space: u.q.space, Class: u.q.class, Origin: u.q.origin, Entries: u.entries, Chain: u.q.chain, Chains: u.chains})
		shown := u.entries
		if len(shown) > 8 {
			shown = append(append([]string(nil), shown[:8]...), "...")
		}
		r.Violations = append(r.Violations, fmt.Sprintf("unordered write: %s reaches a write of %s (%s) at %s with no acquisition; %d entry points reach it [%s]: %s",
			u.q.chain[0][:strings.Index(u.q.chain[0], " ")], u.q.space, u.q.class, u.q.origin, len(u.entries), strings.Join(shown, " "), strings.Join(u.q.chain, " -> ")))
	}
	a.checkFields(r)
	r.Decisions = a.decisions()
	for _, name := range collection.SortedKeys(visibilityPrimitives) {
		if names[name] == nil {
			r.Violations = append(r.Violations, "stale: visibility primitive "+name+" does not exist")
		}
	}
	if len(r.Decisions) == 0 {
		r.Violations = append(r.Violations, "no hidden-policy decision found: the decision analysis is broken")
	}
	a.checkRules(r, names)
	sort.Strings(r.Violations)
	return r
}

// JSON is the census as indented JSON.
func (r *Report) JSON() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Markdown is the census for reading.
func (r *Report) Markdown() []byte {
	var b strings.Builder
	b.WriteString("# Stored-entity census\n\n")
	b.WriteString("Generated by `testing/storagecensus` from the type-checked module (SSA, VTA call graph). Do not edit: regenerate with `NORNICDB_CENSUS_UPDATE=1 go test ./testing/storagecensus -run TestStoredEntityCensusIsCurrent`.\n\n## Rules\n\n")
	for _, rule := range r.Rules {
		fmt.Fprintf(&b, "- %s\n", rule)
	}
	b.WriteString("\n## Acquisitions\n\n")
	for _, n := range r.Acquisitions {
		fmt.Fprintf(&b, "- `%s`: %s\n", n.Name, n.Reason)
	}
	b.WriteString("\n## Exempt\n\n")
	for _, n := range r.Exempt {
		fmt.Fprintf(&b, "- `%s`: %s\n", n.Name, n.Reason)
	}
	b.WriteString("\n## Key spaces\n\n| space | class | writes |\n|---|---|---|\n")
	for _, s := range r.Spaces {
		fmt.Fprintf(&b, "| `%s` | %s | %d |\n", s.Space, s.Class, s.Writes)
	}
	b.WriteString("\n## Writes\n\n| class | space | Badger call | function | key resolved at |\n|---|---|---|---|---|\n")
	for _, w := range r.Writes {
		fmt.Fprintf(&b, "| %s | `%s` | %s | `%s` | %s |\n", w.Class, w.Space, w.Origin, w.Func, w.Where)
	}
	b.WriteString("\n## Field changes\n\n| class | field | function | at |\n|---|---|---|---|\n")
	for _, w := range r.FieldWrites {
		fmt.Fprintf(&b, "| %s | `%s` | `%s` | %s |\n", w.Class, w.Field, w.Func, w.Pos)
	}
	b.WriteString("\n## Hidden-policy decisions\n\n| function | branch | decides |\n|---|---|---|\n")
	for _, d := range r.Decisions {
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n", d.Func, d.Pos, d.Gates)
	}
	fmt.Fprintf(&b, "\n## Violations\n\n%d\n", len(r.Violations))
	for _, v := range r.Violations {
		fmt.Fprintf(&b, "- %s\n", v)
	}
	return []byte(b.String())
}
