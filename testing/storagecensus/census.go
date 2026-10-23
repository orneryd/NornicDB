package storagecensus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Site is a function that makes effects of one class itself.
type Site struct {
	Class   Class    `json:"class"`
	Func    string   `json:"func"`
	File    string   `json:"file"`
	Effects []string `json:"effects"`
	// Status is "owner" (the function owns the class), "exempt" (listed
	// with Reason) or "violation".
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// EntryClass is what one entry point reaches of one class.
type EntryClass struct {
	Class Class `json:"class"`
	// Owners are the owners the entry point's paths to the class's effects
	// pass, Exempt the listed exemptions they reach.
	Owners []string `json:"owners,omitempty"`
	Exempt []string `json:"exempt,omitempty"`
	// Sites are the site functions of the class the entry point reaches
	// (counter, hidden and total classes).
	Sites []string `json:"sites,omitempty"`
	// Unowned is a path to a write that passes no owner (write class).
	Unowned []string `json:"unowned,omitempty"`
}

// Entry is an exported function or method, or a function a go statement
// starts, that reaches an effect.
type Entry struct {
	Func    string       `json:"func"`
	File    string       `json:"file"`
	Classes []EntryClass `json:"classes"`
}

// Report is the census.
type Report struct {
	Packages []string `json:"packages"`
	Rules    []string `json:"rules"`
	Sites    []Site   `json:"sites"`
	Entries  []Entry  `json:"entries"`
	// Violations are the effects that bypass their owner; Stale the owner
	// and exemption rows that no longer match the code.
	Violations []string `json:"violations"`
	Stale      []string `json:"stale"`
	Families   []Family `json:"families"`
}

// Family is a group of owners that own their class for one reason.
type Family struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

var rules = []string{
	"write: every path from an entry point to a physical Badger write passes a write-order owner, or a listed exemption; a function that calls an owner is governed by it",
	"counter: a count, derived count or quota-usage change is made only by a listed counter owner, or a listed exemption",
	"hidden: a function that tests hidden or suppressed state, or reads a stored image, and stages or performs a write itself is a listed hidden-policy owner, or a listed exemption",
	"total: an integrity or quota total is read only by a listed reader, or a listed exemption",
}

// Census analyses the module rooted at root against spec.
func Census(root string, spec Spec) (*Report, error) {
	prog, err := load(root)
	if err != nil {
		return nil, err
	}
	g := build(prog)
	return g.report(spec), nil
}

// Spec is what the census classifies against.
type Spec struct {
	// Owners maps, per class, an owner function to its family; Families
	// gives each family's reason.
	Owners   map[Class]map[string]string
	Families map[string]string
	// Exempt maps, per class, a function to the reason it may bypass the
	// class's owner.
	Exempt map[Class]map[string]string
	// Counted is the counted state: the fields and functions whose change
	// is a count, derived-count or quota-usage change, with what each
	// counts. NotCounted are the fields and functions the counting
	// patterns find that count something else, with what.
	Counted, NotCounted map[string]string
	// Totals are the functions whose call reads an integrity or quota
	// total, with the total; NotTotals those the patterns find that read
	// something else.
	Totals, NotTotals map[string]string
}

// target is the state or function an effect names ("verb target").
func target(effect string) string {
	if i := strings.IndexByte(effect, ' '); i >= 0 {
		return effect[i+1:]
	}
	return effect
}

// classify keeps the counter and total effects on the counted state and
// the totals, drops those the spec says count or read something else, and
// reports the rest as unclassified.
func (g *graph) classify(spec Spec, r *Report) {
	g.named = map[string]bool{}
	for _, f := range g.fns {
		for _, c := range []Class{Counter, Total} {
			for e := range f.sites[c] {
				g.named[target(e)] = true
			}
		}
	}
	for _, k := range sortedKeys(g.fns) {
		f := g.fns[k]
		for _, c := range []Class{Counter, Total} {
			in, out := spec.Counted, spec.NotCounted
			if c == Total {
				in, out = spec.Totals, spec.NotTotals
			}
			for _, e := range sortedKeys(f.sites[c]) {
				t := target(e)
				switch {
				case in[t] != "":
				case out[t] != "":
					delete(f.sites[c], e)
				default:
					delete(f.sites[c], e)
					r.Violations = append(r.Violations, fmt.Sprintf("unclassified %s state: %s (in %s)", c, t, k))
				}
			}
			if len(f.sites[c]) == 0 {
				delete(f.sites, c)
			}
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (g *graph) report(spec Spec) *Report {
	r := &Report{Packages: append([]string(nil), Packages...), Rules: rules}
	g.classify(spec, r)
	for _, name := range sortedKeys(spec.Families) {
		r.Families = append(r.Families, Family{Name: name, Reason: spec.Families[name]})
	}
	keys := sortedKeys(g.fns)
	for _, k := range keys {
		f := g.fns[k]
		if f.hidden && f.stages {
			f.site(Hidden, "hidden test and write in one function")
		}
	}
	// Sites.
	for _, c := range Classes {
		for _, k := range keys {
			f := g.fns[k]
			if len(f.sites[c]) == 0 {
				continue
			}
			s := Site{Class: c, Func: k, File: f.file, Effects: sortedKeys(f.sites[c])}
			switch {
			case spec.Owners[c][k] != "":
				s.Status, s.Reason = "owner", spec.Owners[c][k]+": "+spec.Families[spec.Owners[c][k]]
			case spec.Exempt[c][k] != "":
				s.Status, s.Reason = "exempt", spec.Exempt[c][k]
			case c == Write:
				// Writes are governed along paths (entries below).
				s.Status = "governed"
			default:
				s.Status = "violation"
				r.Violations = append(r.Violations, fmt.Sprintf("%s: %s %s", c, k, strings.Join(s.Effects, ", ")))
			}
			r.Sites = append(r.Sites, s)
		}
	}
	// Entries.
	w := &walker{g: g, spec: spec, memo: map[string]*reach{}}
	for _, k := range keys {
		f := g.fns[k]
		if !f.entry {
			continue
		}
		var classes []EntryClass
		for _, c := range Classes {
			res := w.walk(c, k, map[string]bool{})
			if res.empty() {
				continue
			}
			ec := EntryClass{Class: c, Owners: sortedKeys(res.owners), Exempt: sortedKeys(res.exempt), Unowned: res.unowned}
			if c != Write {
				ec.Sites = sortedKeys(res.sites)
			}
			if c == Write && res.unowned != nil && spec.Exempt[Write][k] == "" {
				r.Violations = append(r.Violations, fmt.Sprintf("write: %s reaches a write without an owner: %s", k, strings.Join(res.unowned, " > ")))
			}
			classes = append(classes, ec)
		}
		if len(classes) > 0 {
			r.Entries = append(r.Entries, Entry{Func: k, File: f.file, Classes: classes})
		}
	}
	// Stale rows.
	for _, c := range Classes {
		for _, kind := range []struct {
			name string
			rows map[string]string
		}{{"owner", spec.Owners[c]}, {"exemption", spec.Exempt[c]}} {
			for _, k := range sortedKeys(kind.rows) {
				f := g.fns[k]
				switch {
				case f == nil:
					r.Stale = append(r.Stale, fmt.Sprintf("%s %s %s: no such function", c, kind.name, k))
				case c != Write && kind.name == "exemption" && len(f.sites[c]) == 0:
					r.Stale = append(r.Stale, fmt.Sprintf("%s exemption %s: no longer a site", c, k))
				}
			}
		}
	}
	for _, fam := range sortedKeys(spec.Families) {
		used := false
		for _, c := range Classes {
			for _, f := range spec.Owners[c] {
				used = used || f == fam
			}
		}
		if !used {
			r.Stale = append(r.Stale, "family "+fam+": no owner")
		}
	}
	for _, c := range Classes {
		for _, k := range sortedKeys(spec.Owners[c]) {
			if spec.Families[spec.Owners[c][k]] == "" {
				r.Stale = append(r.Stale, fmt.Sprintf("%s owner %s: unknown family %q", c, k, spec.Owners[c][k]))
			}
		}
	}
	for _, table := range []struct {
		name string
		rows map[string]string
	}{{"counted", spec.Counted}, {"not counted", spec.NotCounted}, {"total", spec.Totals}, {"not total", spec.NotTotals}} {
		for _, k := range sortedKeys(table.rows) {
			if !g.named[k] {
				r.Stale = append(r.Stale, fmt.Sprintf("%s row %s: no longer found", table.name, k))
			}
		}
	}
	sort.Strings(r.Violations)
	sort.Strings(r.Stale)
	return r
}

// reach is what a function reaches of one class.
type reach struct {
	owners, exempt, sites map[string]bool
	unowned               []string
}

func (r *reach) empty() bool {
	return len(r.owners) == 0 && len(r.exempt) == 0 && len(r.sites) == 0 && r.unowned == nil
}

func (r *reach) merge(o *reach) {
	for k := range o.owners {
		r.owners[k] = true
	}
	for k := range o.exempt {
		r.exempt[k] = true
	}
	for k := range o.sites {
		r.sites[k] = true
	}
}

type walker struct {
	g    *graph
	spec Spec
	memo map[string]*reach
}

func newReach() *reach {
	return &reach{owners: map[string]bool{}, exempt: map[string]bool{}, sites: map[string]bool{}}
}

// walk returns what key reaches of class c. Functions are visited in sorted
// order and a cycle is cut where it closes, so the result is the same on
// every run.
func (w *walker) walk(c Class, key string, stack map[string]bool) *reach {
	mk := string(c) + "|" + key
	if r := w.memo[mk]; r != nil {
		return r
	}
	r := newReach()
	f := w.g.fns[key]
	if f == nil || stack[key] {
		return r
	}
	if w.spec.Owners[c][key] != "" {
		r.owners[key] = true
		w.memo[mk] = r
		return r
	}
	if w.spec.Exempt[c][key] != "" {
		r.exempt[key] = true
		w.memo[mk] = r
		return r
	}
	if c == Write {
		governed := false
		for _, callee := range sortedKeys(f.calls) {
			if w.spec.Owners[Write][callee] != "" {
				r.owners[callee] = true
				governed = true
			}
		}
		if governed {
			w.memo[mk] = r
			return r
		}
		if len(f.sites[Write]) > 0 {
			r.unowned = []string{key}
			w.memo[mk] = r
			return r
		}
	} else if len(f.sites[c]) > 0 {
		r.sites[key] = true
	}
	stack[key] = true
	for _, callee := range sortedKeys(f.calls) {
		sub := w.walk(c, callee, stack)
		r.merge(sub)
		if r.unowned == nil && sub.unowned != nil {
			r.unowned = append([]string{key}, sub.unowned...)
		}
	}
	delete(stack, key)
	w.memo[mk] = r
	return r
}

// JSON renders the report, indented, with a trailing newline.
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

// Markdown renders the report's summary, sites and exemptions.
func (r *Report) Markdown() []byte {
	var b strings.Builder
	b.WriteString("# Stored-entity census\n\n")
	b.WriteString("Generated by `testing/storagecensus` from the type-checked source of " + strings.Join(r.Packages, ", ") + ". Do not edit: regenerate with `NORNICDB_CENSUS_UPDATE=1 go test ./testing/storagecensus -run TestStoredEntityCensusIsCurrent`.\n\n")
	b.WriteString("## Rules\n\n")
	for _, rule := range r.Rules {
		b.WriteString("- " + rule + "\n")
	}
	b.WriteString("\n## Summary\n\n| class | owner | exempt | governed | violation |\n|---|---|---|---|---|\n")
	for _, c := range Classes {
		counts := map[string]int{}
		for _, s := range r.Sites {
			if s.Class == c {
				counts[s.Status]++
			}
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d |\n", c, counts["owner"], counts["exempt"], counts["governed"], counts["violation"])
	}
	fmt.Fprintf(&b, "\nEntry points reaching an effect: %d.\n", len(r.Entries))
	for _, c := range Classes {
		fmt.Fprintf(&b, "\n## %s sites\n\n| function | status | effects | reason |\n|---|---|---|---|\n", c)
		for _, s := range r.Sites {
			if s.Class == c {
				fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n", s.Func, s.Status, strings.Join(s.Effects, "; "), s.Reason)
			}
		}
	}
	b.WriteString("\n## Owner families\n\n")
	for _, fam := range r.Families {
		fmt.Fprintf(&b, "- **%s**: %s\n", fam.Name, fam.Reason)
	}
	b.WriteString("\n## Violations\n\n")
	if len(r.Violations) == 0 {
		b.WriteString("None.\n")
	}
	for _, v := range r.Violations {
		b.WriteString("- " + v + "\n")
	}
	return []byte(b.String())
}
