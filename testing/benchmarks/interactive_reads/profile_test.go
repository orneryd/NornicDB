// Author: Codex NOR-285 implementation agent, 2026-10-26.
package interactive_reads_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

//go:embed workload.json
var workloadJSON []byte

//go:embed workload-current.json
var currentWorkloadJSON []byte

type workloadCase struct {
	Name            string                 `json:"name"`
	Family          string                 `json:"family"`
	Statement       string                 `json:"statement"`
	StatementSHA256 string                 `json:"statement_sha256"`
	Parameters      map[string]interface{} `json:"parameters"`
}

type workload struct {
	Author                string         `json:"author"`
	SourceRevision        string         `json:"source_revision"`
	Documents             int            `json:"documents"`
	CorpusSHA256          string         `json:"corpus_sha256"`
	CorpusDigestAlgorithm string         `json:"corpus_digest_algorithm"`
	Cases                 []workloadCase `json:"cases"`
}

const author = "Codex NOR-285 implementation agent, 2026-10-26"

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func digest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

func emit(t *testing.T, kind string, value interface{}) {
	t.Helper()
	data, err := json.Marshal(map[string]interface{}{"author": author, "kind": kind, "value": value})
	must(t, err)
	fmt.Printf("NOR285 %s\n", data)
}

func snapshot(t *testing.T, name, destination string) {
	t.Helper()
	file, err := os.Create(destination)
	must(t, err)
	must(t, pprof.Lookup(name).WriteTo(file, 0))
	must(t, file.Close())
}

func artifact(t *testing.T, name, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	must(t, err)
	emit(t, "artifact", map[string]interface{}{"name": name, "bytes": len(data), "sha256": digest(data), "base64": base64.StdEncoding.EncodeToString(data)})
}

// This opt-in fixture reads a private COPY of a complete maintained saved corpus.
// There is no generator of substitute nodes and no cap on input work. Product
// execution is exclusively StorageExecutor.Execute; profile setup and encoding
// remain outside its timed window. Run only through sig/tnrun.sh on the lab.
func TestNOR285InteractiveReadProfile(t *testing.T) {
	if os.Getenv("NOR285_LAB") != "1" {
		t.Skip("explicit lab-only saved-corpus profile")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("profile must run on the Linux lab")
	}
	corpus := os.Getenv("NOR285_CORPUS")
	selected := os.Getenv("NOR285_CASE")
	if corpus == "" || selected == "" {
		t.Fatal("NOR285_CORPUS and NOR285_CASE are required")
	}
	packetJSON := currentWorkloadJSON
	statementSet := os.Getenv("NOR285_STATEMENTS")
	if statementSet == "historical" {
		packetJSON = workloadJSON
	} else if statementSet != "" && statementSet != "current" {
		t.Fatal("NOR285_STATEMENTS must be current or historical")
	}
	var packet workload
	decoder := json.NewDecoder(bytes.NewReader(packetJSON))
	decoder.UseNumber()
	must(t, decoder.Decode(&packet))
	if packet.Documents != 5280 {
		t.Fatal("fixture must describe exactly 5,280 documents")
	}
	if packet.CorpusDigestAlgorithm != corpusDigestAlgorithm || packet.CorpusSHA256 == "" {
		t.Fatal("frozen packet must identify the maintained export bytes")
	}
	var item workloadCase
	for _, candidate := range packet.Cases {
		if candidate.Name == selected {
			item = candidate
			break
		}
	}
	if item.Name == "" {
		t.Fatalf("unknown case %q", selected)
	}
	if digest([]byte(item.Statement)) != item.StatementSHA256 {
		t.Fatal("statement hash differs")
	}
	// Parameters from JSON retain integer types as Bolt would; JSON numbers
	// must not accidentally become strings or floating-point pagination values.
	item.Parameters = nativeNumbers(item.Parameters).(map[string]interface{})
	temporary := t.TempDir()
	executor, corpusSHA256 := openSavedCorpus(t, packet.CorpusSHA256)
	ctx := context.Background()
	counts, err := executor.Execute(ctx, "MATCH (d:PDDocument) RETURN count(d) AS documents", nil)
	must(t, err)
	if len(counts.Rows) != 1 || len(counts.Rows[0]) != 1 || fmt.Sprint(counts.Rows[0][0]) != "5280" {
		t.Fatalf("saved corpus does not contain 5,280 documents: %+v", counts)
	}
	state, err := executor.Execute(ctx, "MATCH (f:PDFreshness {id:'holdings'}) RETURN f.built AS built, f.updating_since AS updating", nil)
	must(t, err)
	if len(state.Rows) != 1 || len(state.Rows[0]) != 2 || state.Rows[0][0] != true || state.Rows[0][1] != nil {
		t.Fatalf("complete maintained holdings required, got %+v", state)
	}
	if item.Parameters["start_id"] == "__FIXTURE_PARTY_ID__" {
		identity, err := executor.Execute(ctx, "MATCH (i:PDIdentity) WHERE i.name STARTS WITH 'Party' RETURN i.id AS id ORDER BY id LIMIT 1", nil)
		must(t, err)
		if len(identity.Rows) != 1 {
			t.Fatal("missing fixture identity")
		}
		item.Parameters["start_id"] = identity.Rows[0][0]
	}
	var graphBefore map[string]interface{}
	if item.Family == "TOPIC_GROUPS" {
		graphBefore = graphCounts(t, executor)
		requireNoTopicScratch(t, graphBefore)
	}
	emit(t, "input", map[string]interface{}{"case": item.Name, "family": item.Family, "source_revision": packet.SourceRevision, "packet_sha256": digest(packetJSON), "statement_sha256": item.StatementSHA256, "statement_bytes": len(item.Statement), "parameters": item.Parameters, "corpus": corpus, "corpus_sha256": corpusSHA256, "corpus_digest_algorithm": corpusDigestAlgorithm, "parser": os.Getenv("NORNICDB_PARSER"), "gomaxprocs": runtime.GOMAXPROCS(0), "documents": 5280, "holdings": state.Rows, "result_cache": false, "resources": resourceState()})
	profileDir := filepath.Join(temporary, "profiles")
	must(t, os.MkdirAll(profileDir, 0755))
	beforePath := filepath.Join(profileDir, "alloc-before.pb.gz")
	afterPath := filepath.Join(profileDir, "alloc-after.pb.gz")
	cpuPath := filepath.Join(profileDir, "cpu.pb.gz")
	runtime.GC()
	snapshot(t, "allocs", beforePath)
	cpu, err := os.Create(cpuPath)
	must(t, err)
	must(t, pprof.StartCPUProfile(cpu))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	var result *cypher.ExecuteResult
	var queryErr error
	pprof.Do(ctx, pprof.Labels("nor285_case", selected), func(labelled context.Context) {
		result, queryErr = executor.Execute(labelled, item.Statement, item.Parameters)
	})
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	pprof.StopCPUProfile()
	must(t, cpu.Close())
	runtime.GC()
	snapshot(t, "allocs", afterPath)
	emit(t, "measurement", map[string]interface{}{"case": selected, "seconds": elapsed.Seconds(), "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocated_objects": after.Mallocs - before.Mallocs, "gc_cycles": after.NumGC - before.NumGC, "gc_pause_ns": after.PauseTotalNs - before.PauseTotalNs, "heap_inuse_before": before.HeapInuse, "heap_inuse_after": after.HeapInuse, "error": fmt.Sprint(queryErr)})
	// Capture profiles even when the public query reports an error.
	for _, entry := range []struct{ name, path string }{{"cpu.pb.gz", cpuPath}, {"alloc-before.pb.gz", beforePath}, {"alloc-after.pb.gz", afterPath}} {
		artifact(t, entry.name, entry.path)
	}
	binary, err := os.Executable()
	must(t, err)
	for _, analysis := range []struct {
		name  string
		flags []string
	}{
		{"cpu-flat", []string{"-top", "-nodecount=35", cpuPath}},
		{"cpu-cumulative", []string{"-top", "-cum", "-nodecount=35", cpuPath}},
		{"alloc-flat", []string{"-top", "-alloc_space", "-base=" + beforePath, "-nodecount=35", afterPath}},
		{"alloc-cumulative", []string{"-top", "-cum", "-alloc_space", "-base=" + beforePath, "-nodecount=35", afterPath}},
	} {
		args := append([]string{"tool", "pprof"}, analysis.flags...)
		// Explicit binary keeps source/line attribution on the measured build.
		args = append(args[:len(args)-1], binary, args[len(args)-1])
		output, err := exec.Command("go", args...).CombinedOutput()
		emit(t, "pprof", map[string]interface{}{"case": selected, "analysis": analysis.name, "output": string(output), "error": fmt.Sprint(err)})
		must(t, err)
	}
	must(t, queryErr)
	if result == nil {
		t.Fatal("nil public result")
	}
	if item.Family == "TOPIC_GROUPS" {
		graphAfter := graphCounts(t, executor)
		emit(t, "topic-cleanup", map[string]interface{}{"before": graphBefore, "after": graphAfter, "stats": result.Stats})
		requireNoTopicScratch(t, graphAfter)
		if !reflect.DeepEqual(graphBefore, graphAfter) {
			t.Fatalf("topic projection changed net graph counts: before=%v after=%v", graphBefore, graphAfter)
		}
	} else if result.Stats != nil && !reflect.DeepEqual(result.Stats, &cypher.QueryStats{}) {
		t.Fatalf("read reported graph writes: %+v", result.Stats)
	}
	rows, err := json.Marshal(struct {
		Columns []string        `json:"columns"`
		Rows    [][]interface{} `json:"rows"`
	}{result.Columns, result.Rows})
	must(t, err)
	resultPath := filepath.Join(profileDir, "result.json.gz")
	file, err := os.Create(resultPath)
	must(t, err)
	writer := gzip.NewWriter(file)
	_, err = writer.Write(rows)
	must(t, err)
	must(t, writer.Close())
	must(t, file.Close())
	emit(t, "result", map[string]interface{}{"case": selected, "rows": len(result.Rows), "columns": result.Columns, "json_bytes": len(rows), "sha256": digest(rows), "stats": result.Stats})
	artifact(t, "result.json.gz", resultPath)
}

func resourceState() map[string]string {
	result := map[string]string{"gomaxprocs": strconv.Itoa(runtime.GOMAXPROCS(0)), "mem_profile_rate": strconv.Itoa(runtime.MemProfileRate), "gomemlimit": os.Getenv("GOMEMLIMIT")}
	for _, path := range []string{"/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpuset.cpus.effective", "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.events", "/proc/self/status"} {
		if data, err := os.ReadFile(path); err == nil {
			result[path] = string(data)
		}
	}
	return result
}

func nativeNumbers(value interface{}) interface{} {
	switch value := value.(type) {
	case json.Number:
		if integer, err := value.Int64(); err == nil {
			return integer
		}
		decimal, err := value.Float64()
		if err != nil {
			panic(err)
		}
		return decimal
	case map[string]interface{}:
		for key, item := range value {
			value[key] = nativeNumbers(item)
		}
		return value
	case []interface{}:
		for key, item := range value {
			value[key] = nativeNumbers(item)
		}
		return value
	default:
		return value
	}
}
