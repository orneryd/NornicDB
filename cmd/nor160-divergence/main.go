// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--worker" {
		if err := worker(os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	opt := Options{}
	flag.CommandLine = flag.NewFlagSet("divergence", flag.ContinueOnError)
	flag.StringVar(&opt.Tree, "tree", ".", "Go module tree to analyze (read-only)")
	flag.StringVar(&opt.OwnersFile, "owners", "", "JSON owner packages/functions; required")
	flag.StringVar(&opt.Focus, "focus", "", "comma-separated package names/prefixes; filters output, not loading")
	flag.StringVar(&opt.Patterns, "packages", "./...", "comma-separated go/packages patterns")
	flag.StringVar(&opt.Tags, "tags", "noui,nolocalllm", "Go build tags")
	flag.StringVar(&opt.Revision, "revision", "", "expected commit SHA; checked against Git when available")
	flag.StringVar(&opt.Sections, "sections", "", "comma-separated section numbers 1..6; default all; skips unrequested analyses")
	out := flag.String("out", "/work/sig/divergence/results", "artifact root; writes <commit>/<flags-key>/report.{json,md}")
	cache := flag.String("cache", "/work/sig/divergence/cache", "shared immutable completed-result cache")
	slots := flag.Int("slots", 2, "analysis slots (1..2); shared measured maximum is two")
	// NOR-160 lab vendoring (lane, 31 Oct): a lab job returns only its output,
	// so print the completed report and receipt there.
	printReport := flag.Bool("print-report", false, "after a completed run, print report.json and receipt.json from --out to stdout")
	parseErr := flag.CommandLine.Parse(os.Args[1:])
	failureOutputRoot = *out
	if parseErr != nil {
		if parseErr == flag.ErrHelp {
			return
		}
		fatal(parseErr)
	}
	selected, err := selectSections(opt.Sections)
	if err != nil {
		fatal(err)
	}
	opt.Sections = canonicalSections(selected)
	if *slots < 1 || *slots > 2 {
		fatal(fmt.Errorf("--slots must be 1..2"))
	}
	if opt.OwnersFile == "" {
		fatal(fmt.Errorf("--owners is required; use an explicit empty families array to disable owner exemptions"))
	}
	abs, err := filepath.Abs(opt.Tree)
	if err != nil {
		fatal(err)
	}
	opt.Tree = abs
	ownerPath, err := filepath.Abs(opt.OwnersFile)
	if err != nil {
		fatal(err)
	}
	opt.OwnersFile = ownerPath
	interruptions := make(chan os.Signal, 2)
	signal.Notify(interruptions, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(interruptions)
	requestSignals = interruptions
	if err := cachedRun(opt, *out, *cache, *slots); err != nil {
		fatal(err)
	}
	if *printReport {
		for _, name := range []string{"receipt.json", "report.json"} {
			_ = filepath.WalkDir(*out, func(path string, entry os.DirEntry, walkErr error) error {
				if walkErr != nil || entry.IsDir() || entry.Name() != name {
					return nil
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				fmt.Printf("NOR160_DIVERGENCE_BEGIN %s\n%s\nNOR160_DIVERGENCE_END %s\n", path, data, path)
				return nil
			})
		}
	}
}

func fatal(err error) {
	if !failureRecorded {
		if path, receiptErr := preflightFailure(err); receiptErr == nil {
			fmt.Fprintln(os.Stderr, "divergence: failure receipt", path)
		} else {
			fmt.Fprintln(os.Stderr, "divergence: cannot persist failure receipt:", receiptErr)
		}
	}
	fmt.Fprintln(os.Stderr, "divergence:", err)
	os.Exit(1)
}

func selectSections(value string) ([6]bool, error) {
	var out [6]bool
	if value == "" {
		for i := range out {
			out[i] = true
		}
		return out, nil
	}
	for _, part := range strings.Split(value, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 || n > 6 {
			return out, fmt.Errorf("--sections: use comma-separated numbers 1..6")
		}
		out[n-1] = true
	}
	return out, nil
}
func canonicalSections(selected [6]bool) string {
	parts := []string{}
	for i, on := range selected {
		if on {
			parts = append(parts, strconv.Itoa(i+1))
		}
	}
	if len(parts) == 6 {
		return ""
	}
	return strings.Join(parts, ",")
}

func readOwners(path string) (Owners, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Owners{}, nil, err
	}
	var o Owners
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&o); err != nil {
		return o, nil, fmt.Errorf("owners: %w", err)
	}
	for _, f := range o.Families {
		if f.Name != "text" && f.Name != "contracts" {
			return o, nil, fmt.Errorf("owner family %q: use text or contracts", f.Name)
		}
	}
	return o, data, nil
}

type analysisJob struct {
	Author     string  `json:"author"`
	Options    Options `json:"options"`
	OwnersJSON string  `json:"owners_json"`
}

func worker(config, dir string) error {
	data, err := os.ReadFile(config)
	if err != nil {
		return err
	}
	var job analysisJob
	if err = json.Unmarshal(data, &job); err != nil {
		return err
	}
	var owners Owners
	if err = json.Unmarshal([]byte(job.OwnersJSON), &owners); err != nil {
		return err
	}
	a, err := load(job.Options, owners)
	if err != nil {
		return err
	}
	report := a.run()
	report.OwnersSHA256 = digest(job.OwnersJSON)
	counts := map[string]int{}
	ids := map[string]string{}
	for _, section := range report.Sections {
		counts[section.Name] = section.Count
		values := []string{}
		for _, f := range section.Findings {
			values = append(values, f.ID)
		}
		ids[section.Name] = digest(strings.Join(values, "\n"))
	}
	if err = writeJSON(filepath.Join(dir, "summary.json"), map[string]any{"author": author, "section_counts": counts, "section_ids_sha256": ids}); err != nil {
		return err
	}
	setStage("write_json")
	if err = writeJSON(filepath.Join(dir, "report.json"), report); err != nil {
		return err
	}
	setStage("write_markdown")
	if err = os.WriteFile(filepath.Join(dir, "report.md"), []byte(markdown(report)), 0644); err != nil {
		return err
	}
	setStage("analysis_complete")
	return nil
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

func markdown(r Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "<!-- Author: %s -->\n# Divergence candidates\n\nRevision: `%s`; loaded source SHA256: `%s`.\n\n%d packages, %d functions. %s.\n\n", author, r.Options.Revision, r.SourceSHA256, r.Packages, r.Functions, r.Algorithm)
	b.WriteString("Counts are finding groups. A group contributes once to each participating package, so package counts need not sum to the section total. The JSON contains every group; this view shows the top 20 by site count, then stable ID.\n\n| Section | Groups |\n|---|---:|\n")
	for _, s := range r.Sections {
		fmt.Fprintf(&b, "| %s | %d |\n", s.Name, s.Count)
	}
	for _, s := range r.Sections {
		fmt.Fprintf(&b, "\n## %s\n\n", s.Name)
		packages := sortedKeys(s.PerPackage)
		b.WriteString("| Package | Groups |\n|---|---:|\n")
		for _, p := range packages {
			fmt.Fprintf(&b, "| `%s` | %d |\n", p, s.PerPackage[p])
		}
		if len(s.Inventory) > 0 {
			fmt.Fprintf(&b, "\nComplete interface inventory (%d contracts, including zero/one implementation) is in JSON.\n", len(s.Inventory))
		}
		for i, f := range s.Findings {
			if i == 20 {
				break
			}
			fmt.Fprintf(&b, "\n### %d. %s (%s)\n\n%s\n\nSubject: `%s`; group `%s`.\n\n", i+1, f.Kind, f.ID, f.Why, strings.ReplaceAll(previewText(f.Subject, 320), "|", "\\|"), f.ID)
			shown := 0
			seen := map[string]bool{}
			for _, site := range f.Sites {
				if seen[site.Function] {
					continue
				}
				seen[site.Function] = true
				fmt.Fprintf(&b, "- `%s:%d` — `%s` (%s)\n", site.File, site.Line, previewText(site.Function, 240), site.Role)
				shown++
				if shown == 12 {
					break
				}
			}
			if len(f.Sites) > shown {
				fmt.Fprintf(&b, "- … %d additional sites; complete partners and call evidence are in report.json under this group ID.\n", len(f.Sites)-shown)
			}
			if len(f.Evidence) > 0 {
				summary := map[string]any{}
				for key, value := range f.Evidence {
					v := reflect.ValueOf(value)
					if v.IsValid() && (v.Kind() == reflect.Slice || v.Kind() == reflect.Array || v.Kind() == reflect.Map) {
						summary[key] = fmt.Sprintf("%d entries (see JSON)", v.Len())
					} else if s, ok := value.(string); ok {
						summary[key] = previewText(s, 240)
					} else {
						summary[key] = value
					}
				}
				data, _ := json.Marshal(summary)
				fmt.Fprintf(&b, "\nEvidence: `%s`\n", data)
			}
		}
	}
	b.WriteString("\n## Scope and limits\n\n")
	for _, limit := range r.Limits {
		fmt.Fprintf(&b, "- %s\n", limit)
	}
	return b.String()
}

func previewText(s string, limit int) string {
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit]) + "… (see JSON)"
}
