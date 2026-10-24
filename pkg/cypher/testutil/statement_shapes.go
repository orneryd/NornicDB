package testutil

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// StatementShape is a statement whose text grows with size: the shapes that
// cost a parse the most (long lists, strings, operator chains, nesting).
type StatementShape struct {
	Name, Text string
}

// StatementShapes are the eight shapes at about size bytes each.
func StatementShapes(size int) []StatementShape {
	fill := func(prefix, unit, suffix string) string {
		n := max(1, (size-len(prefix)-len(suffix))/len(unit))
		return prefix + strings.Repeat(unit, n) + suffix
	}
	depth := max(1, (size-len("RETURN  AS v"))/2)
	return []StatementShape{
		{"list_literal", fill("RETURN [", "1,", "1] AS v")},
		{"string_literal", fill("RETURN '", "a", "' AS v")},
		{"or_chain", fill("MATCH (n:ShapeScale) WHERE n.a = 0", " OR n.a = 1", " RETURN n")},
		{"and_chain", fill("MATCH (n:ShapeScale) WHERE n.a = 0", " AND n.a = 1", " RETURN n")},
		{"plus_chain", fill("RETURN 0", " + 1", " AS v")},
		{"map_literal", fill("RETURN {k0: 0", ", k: 1", "} AS v")},
		{"union", fill("RETURN 1 AS v", " UNION ALL RETURN 1 AS v", "")},
		{"nested_list", "RETURN " + strings.Repeat("[", depth) + strings.Repeat("]", depth) + " AS v"},
	}
}

// StackSampler counts, while it runs, the functions on the stacks of the
// goroutines whose stacks pass through the package (a sampling profile with
// no dependency): self is the frame that was running, inclusive any frame.
type StackSampler struct {
	stop            chan struct{}
	done            sync.WaitGroup
	mu              sync.Mutex
	self, inclusive map[string]int
	samples         int
}

// StartStackSampler samples every interval stacks that include within.
func StartStackSampler(interval time.Duration, within string) *StackSampler {
	s := &StackSampler{stop: make(chan struct{}), self: map[string]int{}, inclusive: map[string]int{}}
	s.done.Add(1)
	go func() {
		defer s.done.Done()
		buf := make([]byte, 1<<22)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
			}
			n := runtime.Stack(buf, true)
			s.record(string(buf[:n]), within)
		}
	}()
	return s
}

func (s *StackSampler) record(dump, within string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stack := range strings.Split(dump, "\n\n") {
		if !strings.Contains(stack, within) || strings.Contains(stack, "StackSampler") {
			continue
		}
		s.samples++
		lines := strings.Split(stack, "\n")
		seen := map[string]bool{}
		first := true
		for _, line := range lines[1:] {
			if strings.HasPrefix(line, "\t") || line == "" {
				continue
			}
			name := line
			if paren := strings.LastIndex(name, "("); paren > 0 {
				name = name[:paren]
			}
			if first {
				s.self[name]++
				first = false
			}
			if !seen[name] {
				seen[name] = true
				s.inclusive[name]++
			}
		}
	}
}

// Stop ends sampling and returns the top functions: self and inclusive
// sample counts of the n most frequent, as report lines.
func (s *StackSampler) Stop(n int) []string {
	close(s.stop)
	s.done.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	top := func(counts map[string]int, label string) []string {
		names := make([]string, 0, len(counts))
		for name := range counts {
			names = append(names, name)
		}
		sort.Slice(names, func(i, j int) bool { return counts[names[i]] > counts[names[j]] })
		var out []string
		for _, name := range names[:min(n, len(names))] {
			out = append(out, fmt.Sprintf("%s %d/%d %s", label, counts[name], s.samples, name))
		}
		return out
	}
	return append(top(s.self, "self"), top(s.inclusive, "inclusive")...)
}
