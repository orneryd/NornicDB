package server

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/google/pprof/profile"
)

// zz3bProfileSummary gives the top functions by flat and by cumulative samples.
func zz3bProfileSummary(raw []byte) string {
	p, err := profile.Parse(bytes.NewReader(raw))
	if err != nil {
		return "profile parse: " + err.Error()
	}
	flat, cum := map[string]int64{}, map[string]int64{}
	var total int64
	for _, s := range p.Sample {
		v := s.Value[0]
		total += v
		seen := map[string]bool{}
		for i, loc := range s.Location {
			for j, line := range loc.Line {
				name := line.Function.Name
				if i == 0 && j == 0 {
					flat[name] += v
				}
				if !seen[name] {
					cum[name] += v
					seen[name] = true
				}
			}
		}
	}
	top := func(m map[string]int64, n int) string {
		type kv struct {
			k string
			v int64
		}
		var xs []kv
		for k, v := range m {
			xs = append(xs, kv{k, v})
		}
		sort.Slice(xs, func(i, j int) bool { return xs[i].v > xs[j].v })
		var b strings.Builder
		for i, x := range xs {
			if i == n {
				break
			}
			fmt.Fprintf(&b, "      %5.1f%% %s\n", 100*float64(x.v)/float64(total), x.k)
		}
		return b.String()
	}
	return fmt.Sprintf("    samples=%d total=%d %s\n    flat:\n%s    cum:\n%s", len(p.Sample), total, p.SampleType[0].Type, top(flat, 20), top(cum, 30))
}
