package server

// H7 verification helper (verify-h7 lane, scratch branch only; test-side, not product code).
// TestH7Driver replays the hunter's shuffled order of the tests that ran before a victim in
// child processes, probes process-wide state after every test, names the tests after which
// that state stays changed, and runs each of them alone before the victim. TestH7Seq is the child.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/storage"
)

var h7Tests = map[string]func(*testing.T){
	"TestAllDatabaseFeedIncludesSystemAdmission":                              TestAllDatabaseFeedIncludesSystemAdmission,
	"TestExplicitTransactionLifecycleExpiration_EndsTransactionAndAuditsOnce": TestExplicitTransactionLifecycleExpiration_EndsTransactionAndAuditsOnce,
	"TestHTTPTerminateEndsAnIdleHolderAtOnce":                                 TestHTTPTerminateEndsAnIdleHolderAtOnce,
	"TestHeimdallAsyncActionKeepsCallerPrincipal":                             TestHeimdallAsyncActionKeepsCallerPrincipal,
	"TestHeimdallStartupUsesResolvedProviderName":                             TestHeimdallStartupUsesResolvedProviderName,
	"TestServerStopR3ConstructedModels":                                       TestServerStopR3ConstructedModels,
	"TestServerStopR4LogOutcomes":                                             TestServerStopR4LogOutcomes,
}

type h7Unit struct {
	label, victim string
	prefix        []string
}

var h7Units = []h7Unit{
	{label: "008", victim: "TestHTTPTerminateEndsAnIdleHolderAtOnce", prefix: []string{
		"TestExplicitTransactionLifecycleExpiration_EndsTransactionAndAuditsOnce",
		"TestServerStopR4LogOutcomes",
	}},
	{label: "009", victim: "TestAllDatabaseFeedIncludesSystemAdmission", prefix: []string{
		"TestHeimdallStartupUsesResolvedProviderName",
	}},
	{label: "010", victim: "TestHeimdallAsyncActionKeepsCallerPrincipal", prefix: []string{
		"TestHeimdallStartupUsesResolvedProviderName",
		"TestServerStopR3ConstructedModels",
	}},
}

// h7Probe reports the process-wide state the victims read: the idle explicit
// transactions SHOW TRANSACTIONS lists (pkg/cypher show_admin.go runningTransactions)
// and, in pkg/server, whether the global Heimdall subsystem manager admits work.
func h7Probe() string {
	store := storage.NewMemoryEngine()
	defer func() { _ = store.Close() }()
	ids := []string{}
	res, err := cypher.NewStorageExecutor(store).Execute(context.Background(), "SHOW TRANSACTIONS YIELD transactionId, currentQuery WHERE currentQuery = '' RETURN transactionId", nil)
	if err != nil {
		ids = append(ids, "err:"+strings.ReplaceAll(err.Error(), " ", "_"))
	} else {
		for _, row := range res.Rows {
			ids = append(ids, fmt.Sprint(row[0]))
		}
	}
	sort.Strings(ids)
	state := "idle=" + strings.Join(ids, ",")
	_, herr := heimdall.ExecuteAction("h7-unregistered-probe", heimdall.ActionContext{Context: context.Background()})
	admits := "admits"
	if herr != nil && strings.Contains(herr.Error(), "stopping") {
		admits = "stopping"
	} else if herr == nil || !strings.Contains(herr.Error(), "unknown action") {
		admits = "other:" + strings.ReplaceAll(fmt.Sprint(herr), " ", "_")
	}
	state += " heimdall=" + admits
	return state
}

func TestH7Seq(t *testing.T) {
	seq := os.Getenv("H7_SEQ")
	if seq == "" {
		t.Skip("child of TestH7Driver only")
	}
	fmt.Printf("H7_PROBE -1 start %s\n", h7Probe())
	for i, name := range strings.Split(seq, ",") {
		fn, ok := h7Tests[name]
		if !ok {
			fmt.Printf("H7_MISSING %s\n", name)
			continue
		}
		t.Run(name, fn)
		fmt.Printf("H7_PROBE %d %s %s\n", i, name, h7Probe())
	}
}

type h7ChildResult struct {
	victim  string // pass, fail or absent
	probes  [][3]string
	exitErr string
}

func h7Child(label string, seq []string, victim string) h7ChildResult {
	cmd := exec.Command(os.Args[0], "-test.run=^TestH7Seq$", "-test.v", "-test.count=1", "-test.timeout=50m")
	cmd.Env = append(os.Environ(), "H7_SEQ="+strings.Join(seq, ","))
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	res := h7ChildResult{victim: "absent"}
	if err != nil {
		res.exitErr = err.Error()
	}
	for _, line := range strings.Split(out.String(), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "H7_PROBE "):
			parts := strings.SplitN(trimmed, " ", 4)
			if len(parts) == 4 {
				res.probes = append(res.probes, [3]string{parts[1], parts[2], parts[3]})
			}
			fmt.Printf("H7U %s| %s\n", label, trimmed)
		case strings.HasPrefix(trimmed, "H7_MISSING"), strings.HasPrefix(trimmed, "panic:"):
			fmt.Printf("H7U %s| %s\n", label, trimmed)
		case strings.HasPrefix(trimmed, "--- FAIL: TestH7Seq/"):
			fmt.Printf("H7U %s| CHILD_FAIL %s\n", label, strings.TrimPrefix(trimmed, "--- FAIL: "))
			if victim != "" && strings.HasPrefix(trimmed, "--- FAIL: TestH7Seq/"+victim+" ") {
				res.victim = "fail"
			}
		case victim != "" && strings.HasPrefix(trimmed, "--- PASS: TestH7Seq/"+victim+" "):
			res.victim = "pass"
		case victim != "" && len(trimmed) < 400 && (strings.HasPrefix(trimmed, "Error:") || strings.Contains(trimmed, "_test.go:")):
			fmt.Printf("H7U %s| LOG %s\n", label, trimmed)
		}
	}
	fmt.Printf("H7_CHILD %s tests=%d victim=%s exit=%q\n", label, len(seq), res.victim, res.exitErr)
	return res
}

func h7Field(s, key string) string {
	for _, f := range strings.Fields(s) {
		if strings.HasPrefix(f, key+"=") {
			return strings.TrimPrefix(f, key+"=")
		}
	}
	return ""
}

func h7IDs(s string) map[string]bool {
	ids := map[string]bool{}
	for _, id := range strings.Split(h7Field(s, "idle"), ",") {
		if id != "" {
			ids[id] = true
		}
	}
	return ids
}

// h7Changed returns the tests after which the probe changed (a new idle
// transaction, or another Heimdall admission state) and kept that change up to
// the child's last probe.
func h7Changed(probes [][3]string) []string {
	var changed []string
	if len(probes) < 2 {
		return changed
	}
	last := probes[len(probes)-1][2]
	lastIDs := h7IDs(last)
	for i := 1; i < len(probes); i++ {
		prev, cur := probes[i-1][2], probes[i][2]
		prevIDs := h7IDs(prev)
		leaked := false
		for id := range h7IDs(cur) {
			if !prevIDs[id] && lastIDs[id] {
				leaked = true
			}
		}
		hp, hc, hl := h7Field(prev, "heimdall"), h7Field(cur, "heimdall"), h7Field(last, "heimdall")
		if hp != hc && hc == hl {
			leaked = true
		}
		if leaked {
			changed = append(changed, probes[i][1])
		}
	}
	return changed
}

func TestH7Driver(t *testing.T) {
	const chunk = 60
	for _, unit := range h7Units {
		control := h7Child(unit.label+"/control", []string{unit.victim}, unit.victim)
		var changers []string
		for start := 0; start < len(unit.prefix); start += chunk {
			end := start + chunk
			if end > len(unit.prefix) {
				end = len(unit.prefix)
			}
			res := h7Child(fmt.Sprintf("%s/scan%03d", unit.label, start), unit.prefix[start:end], "")
			changers = append(changers, h7Changed(res.probes)...)
		}
		fmt.Printf("H7_CHANGERS %s %s\n", unit.label, strings.Join(changers, ","))
		repro := h7Child(unit.label+"/changers+victim", append(append([]string{}, changers...), unit.victim), unit.victim)
		if repro.victim != "fail" {
			repro = h7Child(unit.label+"/prefix+victim", append(append([]string{}, unit.prefix...), unit.victim), unit.victim)
		}
		pairs := []string{}
		for _, c := range changers {
			pair := h7Child(unit.label+"/pair/"+c, []string{c, unit.victim}, unit.victim)
			pairs = append(pairs, c+"="+pair.victim)
		}
		fmt.Printf("H7_RESULT %s victim=%s control=%s repro=%s pairs=%s\n", unit.label, unit.victim, control.victim, repro.victim, strings.Join(pairs, ";"))
	}
}
