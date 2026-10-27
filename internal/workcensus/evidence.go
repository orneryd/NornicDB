package workcensus

import (
	"slices"
	"sort"
	"strings"
)

func sourceFailures(actual, approved Inventory) []string {
	var failures []string
	listed := map[string]SourceBoundary{}
	for _, s := range approved.Sources {
		if _, ok := listed[s.File]; ok {
			failures = append(failures, "DUPLICATE SOURCE "+s.File)
		}
		listed[s.File] = s
	}
	seen := map[string]bool{}
	for _, s := range actual.Sources {
		if seen[s.File] {
			failures = append(failures, "DUPLICATE ACTUAL SOURCE "+s.File)
			continue
		}
		seen[s.File] = true
		a, ok := listed[s.File]
		if !ok {
			failures = append(failures, "UNLISTED SOURCE "+s.File)
			continue
		}
		delete(listed, s.File)
		if a != s {
			failures = append(failures, "SOURCE CHANGED "+s.File)
		}
	}
	for file := range listed {
		failures = append(failures, "STALE SOURCE "+file)
	}
	return failures
}

func evidenceFailures(m, a Member, p Policy) []string {
	var failures []string
	if p.ClassFact != "" && (len(m.Facts[p.ClassFact]) != 1 || m.Facts[p.ClassFact][0] != a.Class) {
		failures = append(failures, "CLASS FACT CONTRADICTION "+m.ID)
	}
	for _, key := range p.FactKeys {
		actual, approved := append([]string(nil), m.Facts[key]...), append([]string(nil), a.Facts[key]...)
		sort.Strings(actual)
		sort.Strings(approved)
		if !slices.Equal(actual, approved) {
			failures = append(failures, "FACT CHANGED "+m.ID+" "+key)
		}
	}
	if contains(p.ForbiddenClasses, a.Class) || a.Assignment != "" {
		matched := false
		for _, assignment := range p.Assignments {
			if assignment.ID == a.Assignment && assignment.MemberID == m.ID && assignment.Digest == m.Digest && assignment.Class == a.Class && strings.TrimSpace(assignment.Owner) != "" && strings.TrimSpace(assignment.Reference) != "" {
				matched = true
			}
		}
		if !matched {
			failures = append(failures, "UNRESOLVED ASSIGNMENT "+m.ID)
		}
	}
	return failures
}
