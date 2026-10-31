// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Populated at installation; executable hash is also included, so unversioned rebuilds invalidate the cache.
var buildID = "development"

const rssBudgetKiB int64 = 5 * 1024 * 1024

type Receipt struct {
	Author              string            `json:"author"`
	Commit              string            `json:"commit"`
	Key                 string            `json:"key"`
	TreeDigest          string            `json:"input_tree_sha256"`
	ToolDigest          string            `json:"tool_binary_sha256"`
	GoVersion           string            `json:"go_version"`
	Seconds             float64           `json:"analysis_seconds"`
	PeakRSSKiB          int64             `json:"peak_rss_kib"`
	Slots               int               `json:"slots"`
	Completed           string            `json:"completed_utc"`
	Counts              map[string]int    `json:"section_counts"`
	Metrics             []StageMetric     `json:"stage_metrics"`
	IDDigest            map[string]string `json:"section_ids_sha256"`
	ReportJSONBytes     int64             `json:"report_json_bytes"`
	ReportMarkdownBytes int64             `json:"report_markdown_bytes"`
}

func sortedKeys[V any](m map[string]V) []string {
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func treeDigest(tree string) (string, error) {
	files := map[string]string{}
	err := filepath.WalkDir(tree, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != tree {
				if strings.HasPrefix(d.Name(), ".") || d.Name() == "node_modules" || d.Name() == "vendor" {
					return filepath.SkipDir
				}
				if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") || d.Name() == "go.mod" || d.Name() == "go.sum" {
			rel, _ := filepath.Rel(tree, path)
			files[filepath.ToSlash(rel)] = path
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	b := sha256.New()
	for _, k := range sortedKeys(files) {
		fmt.Fprintf(b, "%s\x00", k)
		f, err := os.Open(files[k])
		if err != nil {
			return "", err
		}
		_, err = io.Copy(b, f)
		f.Close()
		if err != nil {
			return "", err
		}
		_, _ = b.Write([]byte{0})
	}
	return hex.EncodeToString(b.Sum(nil)), nil
}

func fileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func revision(tree, expected string) (string, error) {
	args := []string{"-c", "safe.directory=*", "-C", tree}
	// Windows-created linked worktrees have an absolute host gitdir. Translate its
	// known /work mount without changing the input .git file (including read-only /repos).
	if data, err := os.ReadFile(filepath.Join(tree, ".git")); err == nil && strings.HasPrefix(string(data), "gitdir: ") {
		gd := strings.TrimSpace(strings.TrimPrefix(string(data), "gitdir: "))
		gd = strings.ReplaceAll(gd, "\\", "/")
		if strings.Contains(gd, ":/") {
			if i := strings.Index(gd, "/nornic-pr387-test/"); i >= 0 {
				gd = filepath.Join("/work", gd[i+len("/nornic-pr387-test/"):])
			}
		}
		if !filepath.IsAbs(gd) {
			gd = filepath.Join(tree, gd)
		}
		if _, err := os.Stat(gd); err == nil {
			args = []string{"-c", "safe.directory=*", "--git-dir=" + gd, "--work-tree=" + tree}
		}
	}
	cmd := exec.Command("git", append(args, "rev-parse", "HEAD")...)
	data, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("cannot establish tree commit: %w", err)
	}
	sha := strings.TrimSpace(string(data))
	if expected != "" && !strings.HasPrefix(sha, expected) {
		return "", fmt.Errorf("expected revision %s, tree is %s", expected, sha)
	}
	return sha, nil
}

func flock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		if err = cancelled("queue"); err != nil {
			f.Close()
			return nil, err
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK {
			f.Close()
			return nil, err
		}
		time.Sleep(100 * time.Millisecond)
	}
	if stat, err := f.Stat(); err == nil && stat.Size() == 0 {
		_, _ = fmt.Fprintln(f, "Author:", author)
	}
	return f, nil
}
func unlock(f *os.File) {
	if f != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}
}

func slot(n int) (*os.File, error) {
	return slotAt("/tmp/go-divergence", n)
}

func slotAt(dir string, n int) (*os.File, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// One blocking admission lock queues callers; only the head waits for an available slot.
	admission, err := flock(filepath.Join(dir, "admission.lock"))
	if err != nil {
		return nil, err
	}
	defer unlock(admission)
	fmt.Fprintln(os.Stderr, "divergence: waiting for an analysis slot (limit", n, ")")
	for {
		if err := cancelled("queue"); err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("slot-%d.lock", i)), os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				return nil, err
			}
			err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				if stat, err := f.Stat(); err == nil && stat.Size() == 0 {
					_, _ = fmt.Fprintln(f, "Author:", author)
				}
				return f, nil
			}
			f.Close()
			if err != syscall.EWOULDBLOCK {
				return nil, err
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func cachedRun(opt Options, outRoot, cacheRoot string, slots int) (retErr error) {
	sha, err := revision(opt.Tree, opt.Revision)
	if err != nil {
		return err
	}
	opt.Revision = sha
	input, err := treeDigest(opt.Tree)
	if err != nil {
		return err
	}
	_, ownerData, err := readOwners(opt.OwnersFile)
	if err != nil {
		return err
	}
	bin, err := os.Executable()
	if err != nil {
		return err
	}
	// The live executable remains addressable across an atomic reinstall. A queued
	// caller must spawn the exact binary it hashed, even if /usr/local/bin is replaced.
	if _, err := os.Stat("/proc/self/exe"); err == nil {
		bin = "/proc/self/exe"
	}
	tool, err := fileDigest(bin)
	if err != nil {
		return err
	}
	// Tree paths are not part of the key: clean release checkouts in different lanes reuse one result.
	identity := opt
	identity.Tree = ""
	identity.OwnersFile = ""
	options, _ := json.Marshal(identity)
	key := digest(strings.Join([]string{sha, input, tool, buildID, runtime.Version(), runtime.GOOS, runtime.GOARCH, string(options), string(ownerData)}, "\x00"))
	cacheDir := filepath.Join(cacheRoot, sha, key)
	if err = os.MkdirAll(filepath.Dir(cacheDir), 0755); err != nil {
		return err
	}
	lock, err := flock(cacheDir + ".lock")
	if err != nil {
		return err
	}
	defer unlock(lock)
	attemptDir := filepath.Join(outRoot, sha, key+".attempts")
	if err = recoverAttempts(attemptDir); err != nil {
		return err
	}
	_, cachedErr := completeArtifact(cacheDir, key, tool, input)
	if cachedErr != nil {
		if _, statErr := os.Stat(cacheDir); statErr == nil {
			quarantine := cacheDir + fmt.Sprintf(".incomplete-%d", time.Now().UnixNano())
			if err = os.Rename(cacheDir, quarantine); err != nil {
				return err
			}
			cause := Attempt{Author: author, State: "failed", Commit: sha, Key: key, Section: "cache", Phase: "cache_validation", Reason: cachedErr.Error(), LimitHit: "none", Updated: time.Now().UTC().Format(time.RFC3339Nano)}
			if err = atomicJSON(filepath.Join(quarantine, "failure.json"), cause); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, "divergence: quarantined incomplete cache", quarantine)
		} else if !os.IsNotExist(statErr) {
			return statErr
		}
		s, err := slot(slots)
		if err != nil {
			return err
		}
		defer unlock(s)
		tmp, err := os.MkdirTemp(filepath.Dir(cacheDir), ".analysis-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp)
		if err = os.MkdirAll(attemptDir, 0755); err != nil {
			return err
		}
		attemptPath := filepath.Join(attemptDir, fmt.Sprintf("%d-%d.json", time.Now().UnixNano(), os.Getpid()))
		stamp := time.Now().UTC().Format(time.RFC3339Nano)
		attempt := Attempt{Author: author, State: "running", Commit: sha, Key: key, Sections: opt.Sections, Section: "not_started", Phase: "starting", SupervisorPID: os.Getpid(), WorkDir: tmp, Started: stamp, Updated: stamp, ToolSHA256: tool}
		var samples []memorySample
		var workerEnd int64
		if err = atomicJSON(attemptPath, attempt); err != nil {
			return err
		}
		defer func() {
			a, err := readAttempt(attemptPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "divergence: cannot finalize attempt receipt:", err)
				return
			}
			a.Updated = time.Now().UTC().Format(time.RFC3339Nano)
			a.State = "completed"
			a.Reason = "analysis completed and reports published"
			if workerEnd == 0 {
				workerEnd = time.Now().UnixNano()
			}
			a.Metrics = stageMetrics(a.Events, samples, workerEnd)
			if retErr != nil {
				markAttemptFailed(&a, retErr)
			}
			if err = atomicJSON(attemptPath, a); err != nil {
				fmt.Fprintln(os.Stderr, "divergence: cannot write failure receipt:", err)
			} else if retErr != nil {
				failureRecorded = true
			}
			if retErr != nil {
				fmt.Fprintf(os.Stderr, "divergence: failure receipt %s; section %s; phase %s; reason %s\n", attemptPath, a.Section, a.Phase, a.Reason)
			}
		}()
		config := filepath.Join(tmp, "options.json")
		if err = writeJSON(config, analysisJob{Author: author, Options: opt, OwnersJSON: string(ownerData)}); err != nil {
			return err
		}
		cmd := exec.Command(bin, "--worker", config, tmp)
		cmd.Stdout = os.Stdout
		var diagnostics diagnosticTail
		cmd.Stderr = io.MultiWriter(os.Stderr, &diagnostics)
		cmd.Env = append(os.Environ(), "GOMAXPROCS=2", "GOMEMLIMIT=3GiB", "DIVERGENCE_ATTEMPT="+attemptPath)
		cmd.ExtraFiles = []*os.File{s}
		start := time.Now()
		err = runWorker(cmd, &samples, rssBudgetKiB)
		workerEnd = time.Now().UnixNano()
		if err != nil {
			return fmt.Errorf("analysis failed; no cached result published: %w; worker diagnostic: %s", err, string(diagnostics.data))
		}
		if err = cancelled("input_verification"); err != nil {
			return err
		}
		after, err := treeDigest(opt.Tree)
		if err != nil {
			return err
		}
		if after != input {
			return fmt.Errorf("input changed during analysis; result discarded")
		}
		rss := int64(0)
		if u, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
			rss = u.Maxrss
		}
		var summary struct {
			Counts   map[string]int    `json:"section_counts"`
			IDDigest map[string]string `json:"section_ids_sha256"`
		}
		data, err := os.ReadFile(filepath.Join(tmp, "summary.json"))
		if err != nil {
			return err
		}
		if err = json.Unmarshal(data, &summary); err != nil {
			return err
		}
		progress, err := readAttempt(attemptPath)
		if err != nil {
			return err
		}
		receipt := Receipt{Author: author, Commit: sha, Key: key, TreeDigest: input, ToolDigest: tool, GoVersion: runtime.Version(), Seconds: time.Since(start).Seconds(), PeakRSSKiB: rss, Slots: slots, Completed: time.Now().UTC().Format(time.RFC3339), Counts: summary.Counts, Metrics: stageMetrics(progress.Events, samples, workerEnd), IDDigest: summary.IDDigest}
		jsonInfo, err := os.Stat(filepath.Join(tmp, "report.json"))
		if err != nil {
			return err
		}
		mdInfo, err := os.Stat(filepath.Join(tmp, "report.md"))
		if err != nil {
			return err
		}
		receipt.ReportJSONBytes = jsonInfo.Size()
		receipt.ReportMarkdownBytes = mdInfo.Size()
		if err = writeJSON(filepath.Join(tmp, "receipt.json"), receipt); err != nil {
			return err
		}
		_ = os.Remove(config)
		if err = os.Rename(tmp, cacheDir); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "divergence: completed %.1fs; peak RSS %.1f MiB\n", receipt.Seconds, float64(rss)/1024)
	} else {
		fmt.Fprintln(os.Stderr, "divergence: cache hit", sha, key)
	}
	out := filepath.Join(outRoot, sha, key)
	absOut, _ := filepath.Abs(out)
	absCache, _ := filepath.Abs(cacheDir)
	if absOut != absCache {
		if _, readyErr := completeArtifact(out, key, tool, input); readyErr != nil {
			if err = os.MkdirAll(out, 0755); err != nil {
				return err
			}
			for _, name := range []string{"report.json", "report.md", "receipt.json"} {
				if err = copyFile(filepath.Join(cacheDir, name), filepath.Join(out, name)); err != nil {
					return err
				}
			}
		}
	}
	if err = cancelled("output_delivery"); err != nil {
		return err
	}
	fmt.Println(out)
	return nil
}

// Preserve the actual compiler/panic diagnostic without allowing failure receipts
// to grow with an unbounded tool log.
type diagnosticTail struct{ data []byte }

func (b *diagnosticTail) Write(p []byte) (int, error) {
	n := len(p)
	const capBytes = 4096
	if len(p) >= capBytes {
		b.data = append(b.data[:0], p[len(p)-capBytes:]...)
	} else {
		if excess := len(b.data) + len(p) - capBytes; excess > 0 {
			copy(b.data, b.data[excess:])
			b.data = b.data[:len(b.data)-excess]
		}
		b.data = append(b.data, p...)
	}
	return n, nil
}

// GOMEMLIMIT is soft; the observed pair-expansion failure proved that a live heap
// can exceed it. Stop one oversized analysis before it consumes the VM headroom.
func runWorker(cmd *exec.Cmd, samples *[]memorySample, limitKiB int64) error {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			return err
		case sig := <-signals:
			_ = cmd.Process.Signal(sig)
			<-done
			return fmt.Errorf("supervisor received %s and forwarded it to its analysis worker", signalName(sig))
		case <-tick.C:
			rss := processRSS(cmd.Process.Pid)
			*samples = append(*samples, memorySample{time.Now().UnixNano(), rss})
			if rss > limitKiB {
				_ = cmd.Process.Kill()
				<-done
				return &rssLimitError{rss, limitKiB}
			}
		}
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.CreateTemp(filepath.Dir(dst), ".copy-")
	if err != nil {
		return err
	}
	tmp := out.Name()
	defer os.Remove(tmp)
	_, err = io.Copy(out, in)
	closeErr := out.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, dst)
}
