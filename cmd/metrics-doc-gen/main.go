package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type metricEntry struct {
	Name      string
	Type      string
	Help      string
	Labels    []string
	Subsystem string
}

var (
	nameRe    = regexp.MustCompile(`Name:\s*"([^"]+)"`)
	helpRe    = regexp.MustCompile(`Help:\s*"([^"]*)"`)
	helpMore  = regexp.MustCompile(`^\s*"([^"]*)"`)
	typeRe    = regexp.MustCompile(`New(CounterVec|HistogramVec|GaugeVec|Counter|Histogram|Gauge|SummaryVec|Summary)`)
	labelRe   = regexp.MustCompile(`Labels:\s*\[\]string\{([^}]+)\}`)
	listRe    = regexp.MustCompile(`\[\]string\{([^}]*)\}`)
	varListRe = regexp.MustCompile(`\b(?:var\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*(?::=|=)\s*\[\]string\{([^}]*)\}`)
	nsRe      = regexp.MustCompile(`Namespace:\s*"([^"]+)"`)
	subsRe    = regexp.MustCompile(`Subsystem:\s*"([^"]+)"`)
)

func main() {
	catalogDir := "pkg/observability"
	if len(os.Args) > 1 {
		catalogDir = os.Args[1]
	}

	entries, err := scanCatalogs(catalogDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	fmt.Println("# NornicDB Metrics Reference")
	fmt.Println()
	fmt.Printf("Auto-generated from `%s/catalog_*.go`. Total: %d metrics.\n\n", catalogDir, len(entries))

	subsystems := groupBySubsystem(entries)
	subsOrder := sortedKeys(subsystems)

	for _, sub := range subsOrder {
		fmt.Printf("## %s\n\n", strings.Title(sub))
		fmt.Println("| Metric | Type | Labels | Description |")
		fmt.Println("|--------|------|--------|-------------|")
		for _, e := range subsystems[sub] {
			labels := "-"
			if len(e.Labels) > 0 {
				labels = "`" + strings.Join(e.Labels, "`, `") + "`"
			}
			help := strings.ReplaceAll(e.Help, "|", "\\|")
			fmt.Printf("| `%s` | %s | %s | %s |\n", e.Name, e.Type, labels, help)
		}
		fmt.Println()
	}
}

func scanCatalogs(dir string) ([]metricEntry, error) {
	pattern := filepath.Join(dir, "catalog_*.go")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var entries []metricEntry
	for _, f := range files {
		found, err := scanFile(f)
		if err != nil {
			return nil, fmt.Errorf("scanning %s: %w", f, err)
		}
		entries = append(entries, found...)
	}
	return entries, nil
}

func scanFile(path string) ([]metricEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	subsystem := strings.TrimPrefix(filepath.Base(path), "catalog_")
	subsystem = strings.TrimSuffix(subsystem, ".go")

	var entries []metricEntry
	scanner := bufio.NewScanner(f)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	labelVars := collectLabelVars(lines)

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		typeMatch := typeRe.FindStringSubmatch(line)
		if typeMatch == nil {
			continue
		}
		block, end := metricConstructorBlock(lines, i)
		if end > i {
			i = end
		}

		metricType := normalizeType(typeMatch[1])
		namespace := "nornicdb"
		currentSub := ""
		hasNamespace := false
		hasSubsystem := false
		name := ""
		help := ""
		var labels []string

		for j := 0; j < len(block); j++ {
			if m := nsRe.FindStringSubmatch(block[j]); m != nil {
				namespace = m[1]
				hasNamespace = true
			}
			if m := subsRe.FindStringSubmatch(block[j]); m != nil {
				currentSub = m[1]
				hasSubsystem = true
			}
			if m := nameRe.FindStringSubmatch(block[j]); m != nil && name == "" {
				name = m[1]
			}
			if m := helpRe.FindStringSubmatch(block[j]); m != nil && help == "" {
				help = m[1]
				for k := j + 1; k < j+5 && k < len(block); k++ {
					if m2 := helpMore.FindStringSubmatch(block[k]); m2 != nil {
						help += m2[1]
					} else {
						break
					}
				}
			}
		}
		labels = labelsFromConstructor(block, labelVars)

		if name == "" {
			continue
		}

		fullName := name
		if !strings.HasPrefix(name, namespace) && (hasNamespace || hasSubsystem) {
			if currentSub != "" {
				fullName = namespace + "_" + currentSub + "_" + name
			} else {
				fullName = namespace + "_" + name
			}
		}

		entries = append(entries, metricEntry{
			Name:      fullName,
			Type:      metricType,
			Help:      help,
			Labels:    labels,
			Subsystem: subsystem,
		})
	}

	return entries, nil
}

func collectLabelVars(lines []string) map[string][]string {
	vars := make(map[string][]string)
	for _, line := range lines {
		if m := varListRe.FindStringSubmatch(line); m != nil {
			vars[m[1]] = parseStringList(m[2])
		}
	}
	return vars
}

func metricConstructorBlock(lines []string, start int) ([]string, int) {
	depth := 0
	started := false
	block := make([]string, 0, 8)
	for i := start; i < len(lines); i++ {
		line := lines[i]
		block = append(block, line)
		for _, r := range line {
			switch r {
			case '(':
				depth++
				started = true
			case ')':
				if started {
					depth--
				}
			}
		}
		if started && depth <= 0 {
			return block, i
		}
	}
	return block, len(lines) - 1
}

func labelsFromConstructor(block []string, labelVars map[string][]string) []string {
	for _, line := range block {
		if m := labelRe.FindStringSubmatch(line); m != nil {
			return parseStringList(m[1])
		}
	}
	for _, line := range block {
		if m := listRe.FindStringSubmatch(line); m != nil {
			return parseStringList(m[1])
		}
	}
	for i := len(block) - 1; i >= 0; i-- {
		candidate := strings.TrimSpace(block[i])
		candidate = strings.TrimSuffix(candidate, ",")
		candidate = strings.TrimSuffix(candidate, ")")
		candidate = strings.TrimSpace(candidate)
		if labels, ok := labelVars[candidate]; ok {
			return labels
		}
	}
	return nil
}

func parseStringList(raw string) []string {
	var labels []string
	for _, label := range strings.Split(raw, ",") {
		label = strings.Trim(strings.TrimSpace(label), `"`)
		if label != "" {
			labels = append(labels, label)
		}
	}
	return labels
}

func normalizeType(raw string) string {
	switch raw {
	case "CounterVec", "Counter":
		return "counter"
	case "HistogramVec", "Histogram":
		return "histogram"
	case "GaugeVec", "Gauge":
		return "gauge"
	case "SummaryVec", "Summary":
		return "summary"
	default:
		return strings.ToLower(raw)
	}
}

func groupBySubsystem(entries []metricEntry) map[string][]metricEntry {
	m := make(map[string][]metricEntry)
	for _, e := range entries {
		m[e.Subsystem] = append(m[e.Subsystem], e)
	}
	return m
}

func sortedKeys(m map[string][]metricEntry) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
