// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/token"
	"sort"
	"strings"
)

const author = "Codex (implementation sub-agent), 2026-10-26"

type Site struct {
	Package  string `json:"package"`
	File     string `json:"file"`
	Line     int    `json:"line"`
	Function string `json:"function"`
	Role     string `json:"role,omitempty"`
}

type Finding struct {
	ID       string         `json:"id"`
	Kind     string         `json:"kind"`
	Subject  string         `json:"subject"`
	Why      string         `json:"why"`
	Sites    []Site         `json:"sites"`
	Evidence map[string]any `json:"evidence,omitempty"`
}

type Section struct {
	Name       string         `json:"name"`
	Count      int            `json:"count"`
	PerPackage map[string]int `json:"per_package"`
	Findings   []Finding      `json:"findings"`
	Inventory  []Contract     `json:"interface_inventory,omitempty"`
}

type OwnerFamily struct {
	Name      string   `json:"name"`
	Packages  []string `json:"packages"`
	Functions []string `json:"functions"`
	Contracts []string `json:"contracts"`
}

type Owners struct {
	Author   string        `json:"author"`
	Families []OwnerFamily `json:"families"`
}

type Options struct {
	Tree       string `json:"tree"`
	OwnersFile string `json:"owners_file"`
	Focus      string `json:"focus"`
	Patterns   string `json:"patterns"`
	Tags       string `json:"tags"`
	Revision   string `json:"revision,omitempty"`
	Sections   string `json:"sections,omitempty"`
}

type Report struct {
	Author        string    `json:"author"`
	Schema        int       `json:"schema"`
	Options       Options   `json:"options"`
	SourceSHA256  string    `json:"loaded_source_sha256"`
	OwnersSHA256  string    `json:"owners_sha256"`
	Algorithm     string    `json:"callgraph"`
	Packages      int       `json:"packages"`
	Functions     int       `json:"functions"`
	LoadedFiles   []string  `json:"loaded_files"`
	ExcludedFiles []string  `json:"excluded_go_files"`
	Limits        []string  `json:"limits"`
	Sections      []Section `json:"sections"`
}

var sectionNames = []string{"body_clones", "parallel_deciders", "shared_state_writers", "contract_implementations", "text_rederivation", "context_loss"}

func digest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func siteKey(s Site) string  { return fmt.Sprintf("%s:%d:%s:%s", s.File, s.Line, s.Function, s.Role) }

func finishSection(name string, findings []Finding, focus string) Section {
	s := Section{Name: name, PerPackage: map[string]int{}, Findings: []Finding{}}
	seen := map[string]bool{}
	for _, f := range findings {
		keep := focus == ""
		packages := map[string]bool{}
		for _, p := range f.Sites {
			packages[p.Package] = true
			if matches(p.Package, focus) {
				keep = true
			}
		}
		if !keep {
			continue
		}
		sort.Slice(f.Sites, func(i, j int) bool { return siteKey(f.Sites[i]) < siteKey(f.Sites[j]) })
		keys := []string{name, f.Kind, f.Subject}
		for _, p := range f.Sites {
			keys = append(keys, siteKey(p))
		}
		f.ID = digest(strings.Join(keys, "\n"))[:20]
		if seen[f.ID] {
			continue
		}
		seen[f.ID] = true
		s.Findings = append(s.Findings, f)
		for p := range packages {
			s.PerPackage[p]++
		}
	}
	// Largest groups first; ties are stable and independent of map iteration.
	sort.Slice(s.Findings, func(i, j int) bool {
		a, b := s.Findings[i], s.Findings[j]
		if len(a.Sites) != len(b.Sites) {
			return len(a.Sites) > len(b.Sites)
		}
		return a.ID < b.ID
	})
	s.Count = len(s.Findings)
	return s
}

// Patterns are comma-separated exact package/function names or prefix patterns ending /... or *.
func matches(s, patterns string) bool {
	for _, p := range strings.Split(patterns, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if s == p {
			return true
		}
		if strings.HasSuffix(p, "/...") && (s == strings.TrimSuffix(p, "/...") || strings.HasPrefix(s, strings.TrimSuffix(p, "..."))) {
			return true
		}
		if strings.HasSuffix(p, "*") && strings.HasPrefix(s, strings.TrimSuffix(p, "*")) {
			return true
		}
	}
	return false
}

func (o Owners) owns(family, pkg, fn, contract string) bool {
	for _, f := range o.Families {
		if f.Name != family {
			continue
		}
		if contract != "" && len(f.Contracts) > 0 && !matches(contract, strings.Join(f.Contracts, ",")) {
			continue
		}
		if matches(pkg, strings.Join(f.Packages, ",")) || matches(fn, strings.Join(f.Functions, ",")) {
			return true
		}
	}
	return false
}

func posKey(p token.Position) string { return fmt.Sprintf("%s:%d", p.Filename, p.Line) }
