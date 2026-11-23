//go:build closeout_l2_e20_r1

package cypher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
)

// TestC30WarmProfile isolates the second completed execution of the report's
// RETURN statement and compares it with the identical text and no bindings.
func TestC30WarmProfile(t *testing.T) {
	exec := l2E20R1Executor(t)
	ctx := WithTransactionControl(context.Background())
	statement := l2E20R1R()
	want := strings.Repeat("secret", 722) + "x"
	bound := map[string]interface{}{"run": "unused"}
	execute := func(params map[string]interface{}) (*ExecuteResult, error) {
		return exec.Execute(ctx, statement, params)
	}
	requireValue := func(result *ExecuteResult, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if result == nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != want {
			t.Fatal("unexpected concatenation result")
		}
	}

	deadline, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	started := time.Now()
	_, err := exec.Execute(deadline, statement, bound)
	cancel()
	t.Logf("C30 deadline wall=%v err=%v", time.Since(started), err)
	started = time.Now()
	result, err := execute(bound)
	t.Logf("C30 first-completed bound wall=%v", time.Since(started))
	requireValue(result, err)

	var cpu bytes.Buffer
	if err := pprof.StartCPUProfile(&cpu); err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	result, err = execute(bound)
	elapsed := time.Since(started)
	pprof.StopCPUProfile()
	requireValue(result, err)

	p, err := profile.ParseData(cpu.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	flat, cumulative, innermost, total := l2E20R1ProfileTop(p)
	t.Logf("C30 second-completed bound wall=%v cpu=%v", elapsed, time.Duration(total))
	for _, group := range []struct {
		label   string
		entries []l2E20R1ProfileEntry
	}{{"flat", flat}, {"cum", cumulative}, {"innermost", innermost}} {
		for _, entry := range group.entries {
			t.Logf("C30 profile %s %v %s", group.label, time.Duration(entry.value), entry.name)
		}
	}
	c30LogProfileLines(t, p)
	encoded := base64.StdEncoding.EncodeToString(cpu.Bytes())
	t.Logf("C30 pprof sha256=%x bytes=%d", sha256.Sum256(cpu.Bytes()), cpu.Len())
	for i := 0; i < len(encoded); i += 2048 {
		t.Logf("C30 pprof base64 %06d %s", i, encoded[i:min(i+2048, len(encoded))])
	}

	started = time.Now()
	result, err = execute(nil)
	t.Logf("C30 same-text no-bindings wall=%v", time.Since(started))
	requireValue(result, err)

	// Time the exact request stage called by Execute after public admission.
	for _, control := range []struct {
		label  string
		params map[string]interface{}
	}{{"no-bindings", nil}, {"unused-binding", bound}} {
		started = time.Now()
		err := exec.validateBoundParameterExpressions(withQueryParams(ctx, control.params), statement, control.params)
		t.Logf("C30 validateBoundParameterExpressions %s wall=%v err=%v", control.label, time.Since(started), err)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func c30LogProfileLines(t *testing.T, p *profile.Profile) {
	t.Helper()
	for index, sampleType := range p.SampleType {
		if sampleType.Type != "cpu" {
			continue
		}
		values := map[string]int64{}
		for _, sample := range p.Sample {
			seen := map[string]bool{}
			for _, location := range sample.Location {
				for _, line := range location.Line {
					if line.Function == nil {
						continue
					}
					file := filepath.Base(line.Function.Filename)
					switch file {
					case "executor_internal.go", "match_semantic_validation.go", "static_operator_types.go",
						"runtime_type_errors.go", "helpers.go", "trivia.go", "row_expression.go":
						key := line.Function.Name + " " + file + ":" + strconv.FormatInt(line.Line, 10)
						if !seen[key] {
							values[key] += sample.Value[index]
							seen[key] = true
						}
					}
				}
			}
		}
		lines := make([]string, 0, len(values))
		for key := range values {
			lines = append(lines, key)
		}
		sort.Slice(lines, func(i, j int) bool {
			if values[lines[i]] == values[lines[j]] {
				return lines[i] < lines[j]
			}
			return values[lines[i]] > values[lines[j]]
		})
		for _, key := range lines[:min(len(lines), 35)] {
			t.Logf("C30 profile line %v %s", time.Duration(values[key]), key)
		}
	}
}
