// Private lab operator. Invoked only through the native tnrun Go queue.
// No queue mutation, product patch, Python workload, or service restart.
package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const root = "/work/tnrun/operator/r17-rust-route-20261026"

type Component struct{ Name, URL, Sha256 string }
type Patch struct{ Path, Asset, BeforeSha256, AfterSha256 string }
type Manifest struct {
	SourceCommit, SourceArchiveSha256, CargoLockSha256, RustVersion, NextestVersion, NextestSha256, Case string
	SourceArchiveBytes                                                                                   int64
	Components                                                                                           []Component
	Patches                                                                                              []Patch
	NativePackages, NativeSources                                                                        []Component
	SeedPreparedSha256                                                                                   string
}

var report = map[string]any{}
var out, prepared, source, target, toolchain string
var manifest Manifest

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func require(ok bool, s string) {
	if !ok {
		panic(s)
	}
}
func read(p string) []byte   { b, e := os.ReadFile(p); must(e); return b }
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func fileHash(p string) string {
	f, e := os.Open(p)
	must(e)
	defer f.Close()
	h := sha256.New()
	_, e = io.Copy(h, f)
	must(e)
	return hex.EncodeToString(h.Sum(nil))
}
func save(p string, v any) {
	b, e := json.MarshalIndent(v, "", "  ")
	must(e)
	must(os.WriteFile(p, append(b, '\n'), 0644))
}
func mkdir(p string) { must(os.MkdirAll(p, 0755)) }
func cmd(dir, log, name string, args ...string) error {
	fmt.Printf("R17_COMMAND %s %s\n", name, strings.Join(args, " "))
	c := exec.Command(name, args...)
	c.Dir = dir
	c.Env = os.Environ()
	if log != "" {
		f, e := os.OpenFile(log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		must(e)
		defer f.Close()
		c.Stdout = f
		c.Stderr = f
	} else {
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
	}
	return c.Run()
}
func capture(dir, name string, args ...string) string {
	c := exec.Command(name, args...)
	c.Dir = dir
	b, e := c.CombinedOutput()
	if e != nil {
		panic(fmt.Sprintf("%s %v: %s: %v", name, args, b, e))
	}
	return strings.TrimSpace(string(b))
}
func exitCode(e error) int {
	if e == nil {
		return 0
	}
	if x, ok := e.(*exec.ExitError); ok {
		return x.ExitCode()
	}
	return 125
}
func download(url, p, sha string) {
	r, e := http.Get(url)
	must(e)
	defer r.Body.Close()
	require(r.StatusCode == 200, "download status "+r.Status)
	f, e := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	must(e)
	_, e = io.Copy(f, r.Body)
	must(e)
	must(f.Close())
	require(fileHash(p) == sha, "download hash mismatch")
}
func inventory(dir string) map[string]string {
	m := map[string]string{}
	must(filepath.WalkDir(dir, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			require(d.Type().IsRegular(), "nonregular artifact "+p)
			rel, e := filepath.Rel(dir, p)
			must(e)
			m[filepath.ToSlash(rel)] = fileHash(p)
		}
		return nil
	}))
	return m
}
func sameInventory(want map[string]string) {
	got := inventory(source)
	require(len(got) == len(want), "source file census changed")
	for p, h := range want {
		require(got[p] == h, "source changed: "+p)
	}
}
func extract(p, dir string) {
	f, e := os.Open(p)
	must(e)
	defer f.Close()
	g, e := gzip.NewReader(f)
	must(e)
	defer g.Close()
	t := tar.NewReader(g)
	for {
		h, e := t.Next()
		if e == io.EOF {
			break
		}
		must(e)
		n := filepath.Clean(h.Name)
		require(n != "." && !filepath.IsAbs(n) && n != ".." && !strings.HasPrefix(n, "../"), "unsafe archive path")
		dest := filepath.Join(dir, n)
		switch h.Typeflag {
		case tar.TypeXGlobalHeader:
			continue
		case tar.TypeDir:
			mkdir(dest)
		case tar.TypeReg, tar.TypeRegA:
			mkdir(filepath.Dir(dest))
			o, e := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(h.Mode)&0755)
			must(e)
			_, e = io.Copy(o, t)
			must(e)
			must(o.Close())
		default:
			panic("unsupported archive member " + h.Name)
		}
	}
}
func protectSource() {
	var dirs []string
	must(filepath.WalkDir(source, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			dirs = append(dirs, p)
		} else {
			i, e := d.Info()
			must(e)
			must(os.Chmod(p, i.Mode().Perm()&0555))
		}
		return nil
	}))
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	for _, p := range dirs {
		must(os.Chmod(p, 0555))
	}
}
func environment() {
	must(os.Setenv("PATH", prepared+"/native-bin:"+prepared+"/sysroot/usr/bin:"+toolchain+"/bin:"+os.Getenv("PATH")))
	cache := prepared
	if manifest.SeedPreparedSha256 != "" {
		require(regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(manifest.SeedPreparedSha256), "cache seed hash")
		cache = root + "/prepared/" + manifest.SeedPreparedSha256
	}
	must(os.Setenv("CARGO_HOME", cache+"/cargo-home"))
	must(os.Setenv("CARGO_TARGET_DIR", target))
	must(os.Setenv("CARGO_BUILD_JOBS", "2"))
	must(os.Setenv("RUSTC", toolchain+"/bin/rustc"))
	must(os.Setenv("RUSTDOC", toolchain+"/bin/rustdoc"))
	must(os.Setenv("RUST_BACKTRACE", "1"))
	for k, v := range map[string]string{
		"PKG_CONFIG_PATH": prepared + "/native/libxml2/lib/pkgconfig:" + prepared + "/sysroot/usr/lib/x86_64-linux-gnu/pkgconfig",
		"LIBXML2_STATIC":  "1", "LIBCLANG_PATH": prepared + "/sysroot/usr/lib/llvm-14/lib",
		"BINDGEN_EXTRA_CLANG_ARGS": "-resource-dir=" + prepared + "/sysroot/usr/lib/llvm-14/lib/clang/14.0.6",
		"PDFIUM_LIB_PATH":          prepared + "/native/pdfium/lib", "PDFIUM_INCLUDE_PATH": prepared + "/native/pdfium/include",
		"PD_PDFIUM_RENDER_ROOT": prepared + "/native/pdfium-8076", "PYTHONPATH": prepared + "/sysroot/usr/lib/python3/dist-packages",
		"LD_LIBRARY_PATH": prepared + "/native/pdfium/lib:" + prepared + "/native/pdfium-8076/lib:" + prepared + "/sysroot/usr/lib/x86_64-linux-gnu:" + prepared + "/sysroot/usr/lib/llvm-14/lib",
		"LIBRARY_PATH":    prepared + "/sysroot/usr/lib/x86_64-linux-gnu", "CPATH": prepared + "/sysroot/usr/include",
	} {
		must(os.Setenv(k, v))
	}
}
func provisionNative() {
	mkdir(prepared + "/sysroot")
	mkdir(prepared + "/native-bin")
	mkdir(prepared + "/native")
	mkdir(prepared + "/native-downloads")
	for _, p := range manifest.NativePackages {
		file := prepared + "/native-downloads/" + p.Name + ".deb"
		download(p.URL, file, p.Sha256)
		must(cmd("", out+"/native-setup.log", "dpkg-deb", "-x", file, prepared+"/sysroot"))
	}
	// Meson is a Rust dependency build tool; it is not an application/Python test route.
	must(os.WriteFile(prepared+"/native-bin/meson", []byte("#!/bin/sh\nexec python3 '"+prepared+"/sysroot/usr/bin/meson' \"$@\"\n"), 0755))
	environment()
	for _, p := range manifest.NativeSources {
		file := prepared + "/native-downloads/" + p.Name + ".archive"
		download(p.URL, file, p.Sha256)
		dir := prepared + "/native-src-" + p.Name
		strip := true
		if strings.HasPrefix(p.Name, "pdfium") {
			dir = prepared + "/native/" + p.Name
			strip = false
		}
		mkdir(dir)
		flag := "-xzf"
		if strings.HasSuffix(p.URL, ".xz") {
			flag = "-xJf"
		}
		args := []string{flag, file, "--no-same-owner", "-C", dir}
		if strip {
			args = append(args, "--strip-components=1")
		}
		must(cmd("", out+"/native-setup.log", "tar", args...))
	}
	for _, name := range []string{"libiconv", "libxml2"} {
		dir := prepared + "/native-src-" + name
		args := []string{"--disable-dependency-tracking", "--disable-shared", "--prefix=" + prepared + "/native/libxml2", "CFLAGS=-O2 -fPIC"}
		if name == "libxml2" {
			args = append(args, "--without-python", "--with-iconv="+prepared+"/native/libxml2", "--with-zlib", "--without-lzma")
		}
		must(cmd(dir, out+"/native-setup.log", "./configure", args...))
		must(cmd(dir, out+"/native-setup.log", "make", "-j2"))
		must(cmd(dir, out+"/native-setup.log", "make", "install"))
	}
	require(fileHash(prepared+"/native/pdfium-8076/lib/libpdfium.so") == "fd4de9f91f183476d04b1bc3fd17dacd95aba349c55d2c376512d95e082b441e", "canonical PDFium rendering binary hash")
	for _, tool := range []string{"meson", "ninja", "nasm", "xz"} {
		report[tool] = capture("", tool, "--version")
	}
	report["libxml2"] = capture("", "pkg-config", "--modversion", "libxml-2.0")
	require(report["libxml2"] == "2.14.6", "canonical libxml2 version")
}
func prepare() {
	require(!exists(prepared), "Preparation already exists; inspect its receipt, never overwrite or retry blindly")
	mkdir(prepared)
	mkdir(source)
	mkdir(target)
	mkdir(toolchain)
	mkdir(prepared + "/cargo-home")
	archive := root + "/assets/" + manifest.SourceArchiveSha256 + ".tar.gz"
	require(fileHash(archive) == manifest.SourceArchiveSha256, "source archive hash")
	st, e := os.Stat(archive)
	must(e)
	require(st.Size() == manifest.SourceArchiveBytes, "source archive size")
	extract(archive, source)
	for _, p := range manifest.Patches {
		dest := filepath.Join(source, p.Path)
		require(fileHash(dest) == p.BeforeSha256, "patch preimage "+p.Path)
		b := read(".tn-operator/" + p.Asset)
		require(digest(b) == p.AfterSha256, "patch content")
		must(os.WriteFile(dest, b, 0644))
	}
	require(fileHash(source+"/rust/Cargo.lock") == manifest.CargoLockSha256, "Cargo.lock hash")
	save(prepared+"/source-files.json", inventory(source))
	protectSource()
	for _, c := range manifest.Components {
		p := prepared + "/" + c.Name + ".tar.gz"
		download(c.URL, p, c.Sha256)
		dir := prepared + "/install-" + c.Name
		mkdir(dir)
		must(cmd("", out+"/setup.log", "tar", "-xzf", p, "--no-same-owner", "--strip-components=1", "-C", dir))
		must(cmd(dir, out+"/setup.log", "bash", "install.sh", "--prefix="+toolchain, "--disable-ldconfig"))
	}
	nextest := root + "/assets/" + manifest.NextestSha256 + ".tar.gz"
	require(fileHash(nextest) == manifest.NextestSha256, "nextest hash")
	must(cmd("", out+"/setup.log", "tar", "-xzf", nextest, "--no-same-owner", "-C", toolchain+"/bin"))
	environment()
	provisionNative()
	finishRust()
}
func finishRust() {
	rust := capture(source+"/rust", "rustc", "--version")
	cargo := capture(source+"/rust", "cargo", "--version")
	next := capture(source+"/rust", "cargo", "nextest", "--version")
	require(strings.HasPrefix(rust, "rustc 1.99.1 "), "wrong Rust")
	require(strings.Contains(next, "0.9.150"), "wrong nextest")
	report["toolchain"] = map[string]string{"rustc": rust, "cargo": cargo, "nextest": next}
	save(out+"/progress.json", report)
	if !exists(prepared + "/tests.tar.zst") {
		must(cmd(source+"/rust", out+"/rust-build.log", "cargo", "nextest", "archive", "--locked", "-p", "pd-jobs", "--test", "u20_email", "--archive-file", prepared+"/tests.tar.zst"))
	}
	mkdir(out + "/list-binaries")
	must(cmd(source+"/rust", out+"/test-list.json", "cargo", "nextest", "list", "--archive-file", prepared+"/tests.tar.zst", "--workspace-remap", source+"/rust", "--extract-to", out+"/list-binaries", "--message-format", "json", "--", "--exact", manifest.Case))
	require(strings.Contains(string(read(out+"/test-list.json")), manifest.Case), "focused test not listed")
	var index map[string]string
	must(json.Unmarshal(read(prepared+"/source-files.json"), &index))
	sameInventory(index)
	ready := map[string]any{"preparedAt": time.Now().UTC(), "preparationJob": report["jobId"], "manifestSha256": report["manifestSha256"], "testArchiveSha256": fileHash(prepared + "/tests.tar.zst"), "sourceIndexSha256": fileHash(prepared + "/source-files.json"), "toolchain": report["toolchain"], "testList": out + "/test-list.json", "sourceReadonly": true, "workloadExecuted": false}
	save(prepared+"/ready.json", ready)
	report["ready"] = ready
}
func continuePreparation(previous string) {
	require((previous == "j20261026193840-12532-7fed" || previous == "j20261026194512-88484-d6fd"), "only the diagnosed completed preparation may continue")
	require(!exists(prepared+"/ready.json"), "prepared archive already ready; do not execute it again")
	var prior, terminal, commit map[string]any
	must(json.Unmarshal(read(root+"/runs/"+previous+"/receipt.json"), &prior))
	result := read("/work/tnrun/done/" + previous + "/result.json")
	must(json.Unmarshal(result, &terminal))
	must(json.Unmarshal(read("/work/tnrun/done/"+previous+"/result.commit.json"), &commit))
	require(commit["state"] == "COMMITTED" && commit["resultSha256"] == digest(result), "prior native result not committed")
	require(terminal["state"] == "DONE" && terminal["exit"] == float64(1), "prior preparation must be terminal exit 1")
	require(prior["manifestSha256"] == report["manifestSha256"] && prior["operatorExit"] == float64(125), "prior preparation identity differs")
	var index map[string]string
	must(json.Unmarshal(read(prepared+"/source-files.json"), &index))
	sameInventory(index)
	require(exists(prepared+"/sysroot/usr/lib/llvm-14/lib/clang/14.0.6/include/stddef.h"), "private Clang resource headers absent")
	environment()
	require(capture("", "pkg-config", "--modversion", "libxml-2.0") == "2.14.6", "native libxml2 differs")
	report["continuedFrom"] = previous
	report["continuationReason"] = "Continue completed preparation after diagnosed infrastructure correction; resource headers explicit and nextest extraction directory created; no source, dependency or test changes"
	finishRust()
}
func exists(p string) bool { _, e := os.Stat(p); return e == nil }
func replay(expected, selection string) int {
	require(regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(expected), "full expected engine commit required")
	require(selection == "focused" || selection == "suite", "focused or suite required")
	require(capture("", "git", "rev-parse", "HEAD") == expected, "engine source commit differs")
	require(capture("", "git", "diff", "--name-only", "HEAD") == "", "tracked source edits forbidden")
	var ready map[string]any
	must(json.Unmarshal(read(prepared+"/ready.json"), &ready))
	require(ready["manifestSha256"] == report["manifestSha256"], "prepared manifest differs")
	require(fileHash(prepared+"/tests.tar.zst") == ready["testArchiveSha256"], "test archive differs")
	require(fileHash(prepared+"/source-files.json") == ready["sourceIndexSha256"], "source index differs")
	var index map[string]string
	must(json.Unmarshal(read(prepared+"/source-files.json"), &index))
	sameInventory(index)
	report["engineCommit"] = expected
	report["engineTree"] = capture("", "git", "rev-parse", "HEAD^{tree}")
	report["gitStatus"] = capture("", "git", "status", "--porcelain", "--untracked-files=all")
	report["selection"] = selection
	report["prepared"] = ready
	save(out+"/progress.json", report)
	binary := out + "/nornicdb"
	must(cmd("", out+"/engine-build.log", "go", "build", "-mod=readonly", "-buildvcs=true", "-trimpath", "-o", binary, "./cmd/nornicdb"))
	require(capture("", "git", "diff", "--name-only", "HEAD") == "", "engine build changed tracked source")
	must(os.Chmod(binary, 0555))
	info := capture("", "go", "version", "-m", binary)
	require(strings.Contains(info, "vcs.revision="+expected), "binary VCS revision differs")
	must(os.WriteFile(out+"/go-version-m.txt", []byte(info+"\n"), 0644))
	binarySha := fileHash(binary)
	report["binarySha256"] = binarySha
	environment()
	mkdir(out + "/engine-logs")
	mkdir(out + "/query-traces")
	mkdir(out + "/rust-target")
	for k, v := range map[string]string{"PD_NATIVE_RUNNER": binary, "PD_EXPECTED_ENGINE_COMMIT": expected, "PD_EXPECTED_BINARY_SHA256": binarySha, "PD_NATIVE_PARSER": "default", "PD_NATIVE_LOG_DIR": out + "/engine-logs", "PD_QUERY_TRACE_DIR": out + "/query-traces", "CARGO_TARGET_DIR": out + "/rust-target"} {
		must(os.Setenv(k, v))
	}
	must(os.Unsetenv("PD_NATIVE_STORE_IN"))
	mkdir(out + "/nextest-store")
	originalConfig := read(source + "/rust/.config/nextest.toml")
	require(!strings.Contains(string(originalConfig), "[store]"), "review original nextest store configuration before overriding")
	configPath := out + "/nextest-config.toml"
	configBytes := append([]byte("[store]\ndir = "+strconv.Quote(out+"/nextest-store")+"\n\n"), originalConfig...)
	must(os.WriteFile(configPath, configBytes, 0644))
	report["nextestHarnessConfig"] = map[string]any{"path": configPath, "sha256": digest(configBytes), "originalSha256": digest(originalConfig), "onlyOverride": "store.dir", "storeDirectory": out + "/nextest-store", "fixtureSettingsUnchanged": true}
	report["routeVersion"] = "r17-rust-v2-store-and-allocation"
	save(out+"/progress.json", report)
	args := []string{"nextest", "run", "--archive-file", prepared + "/tests.tar.zst", "--workspace-remap", source + "/rust", "--extract-to", out + "/test-binaries", "--config-file", configPath, "--profile", "lab", "--test-threads", "2", "--no-tests=fail", "--retries", "0", "--success-output", "final"}
	if selection == "focused" {
		args = append(args, "--", "--exact", manifest.Case)
	}
	mkdir(out + "/test-binaries")
	begin := time.Now()
	code := exitCode(cmd(source+"/rust", out+"/nextest.txt", "cargo", args...))
	report["workloadSeconds"] = time.Since(begin).Seconds()
	report["workloadExit"] = code
	for _, p := range []string{out + "/nextest-store/lab/junit.xml", out + "/test-binaries/target/nextest/lab/junit.xml", out + "/test-binaries/nextest/lab/junit.xml", out + "/rust-target/nextest/lab/junit.xml"} {
		if exists(p) {
			must(os.WriteFile(out+"/junit.xml", read(p), 0644))
			break
		}
	}
	sameInventory(index)
	require(fileHash(binary) == binarySha, "binary changed during replay")
	report["sourceAndBinaryUnchangedAfterRun"] = true
	mapCode := exitCode(cmd("", out+"/fingerprint-map.log", "go", "run", "-mod=readonly", ".tn-operator/fingerprint.go", out+"/query-traces", out+"/shape-exact-map.json"))
	report["fingerprintMapExit"] = mapCode
	if code == 0 && mapCode != 0 {
		return 125
	}
	return code
}
func mainRun() int {
	require(len(os.Args) >= 2, "prepare or run required")
	b := read(".tn-operator/manifest.json")
	must(json.Unmarshal(b, &manifest))
	mh := digest(b)
	prepared = root + "/prepared/" + mh
	source = prepared + "/source"
	target = prepared + "/target"
	if manifest.SeedPreparedSha256 != "" {
		require(regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(manifest.SeedPreparedSha256), "cache seed hash")
		target = root + "/prepared/" + manifest.SeedPreparedSha256 + "/target"
	}
	toolchain = prepared + "/toolchain"
	wk := os.Getenv("WK")
	require(regexp.MustCompile(`^[0-9]+$`).MatchString(wk), "native worker required")
	jobs, e := filepath.Glob("/work/tnrun/running/w" + wk + "/*.json")
	must(e)
	require(len(jobs) == 1, "expected one native running marker")
	var job map[string]any
	must(json.Unmarshal(read(jobs[0]), &job))
	id, _ := job["id"].(string)
	require(regexp.MustCompile(`^j[0-9]{14}-[A-Za-z0-9_-]+$`).MatchString(id), "native job id required")
	out = root + "/runs/" + id
	require(!exists(out), "job artifact directory already exists")
	mkdir(out)
	memory := strings.TrimSpace(string(read("/sys/fs/cgroup/memory.max")))
	require(memory != "", "memory allocation missing")
	cpus := regexp.MustCompile(`(?m)^Cpus_allowed_list:\s*(.+)$`).FindSubmatch(read("/proc/self/status"))
	require(len(cpus) == 2, "CPU affinity missing")
	cpuList := strings.TrimSpace(string(cpus[1]))
	count := 0
	for _, x := range strings.Split(cpuList, ",") {
		parts := strings.Split(x, "-")
		a, e := strconv.Atoi(parts[0])
		must(e)
		z := a
		if len(parts) == 2 {
			z, e = strconv.Atoi(parts[1])
			must(e)
		}
		count += z - a + 1
	}
	require(count > 0, "CPU allocation missing")
	report = map[string]any{"startedAt": time.Now().UTC(), "jobId": id, "worker": "w" + wk, "mode": os.Args[1], "manifestSha256": mh, "sourceCommit": manifest.SourceCommit, "allocation": map[string]any{"logicalCpus": cpuList, "memoryBytes": memory, "testThreads": 2}, "noQueueMutation": true, "noProductSourceChanges": true, "noPythonWorkload": true, "nativeSource": job["sha"]}
	save(out+"/progress.json", report)
	if os.Args[1] == "prepare" {
		require(len(os.Args) == 2, "prepare takes no arguments")
		prepare()
		return 0
	}
	if os.Args[1] == "prepare-continue" {
		require(len(os.Args) == 3, "prepare-continue requires exact prior job")
		continuePreparation(os.Args[2])
		return 0
	}
	require(os.Args[1] == "run" && len(os.Args) == 4, "run FULL_ENGINE_SHA focused|suite")
	require(job["sha"] == os.Args[2], "native job SHA differs")
	report["route"] = "ordinary-go-pushed-carrier"
	return replay(os.Args[2], os.Args[3])
}
func main() {
	code := 125
	func() {
		defer func() {
			if e := recover(); e != nil {
				report["error"] = fmt.Sprint(e)
				fmt.Fprintln(os.Stderr, e)
			}
		}()
		code = mainRun()
	}()
	if out != "" {
		report["finishedAt"] = time.Now().UTC()
		report["operatorExit"] = code
		// Bind retained logs and the binary to the native committed stdout receipt.
		artifacts := []map[string]any{}
		must(filepath.WalkDir(out, func(p string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			rel, e := filepath.Rel(out, p)
			must(e)
			if d.IsDir() {
				if rel == "test-binaries" || rel == "list-binaries" {
					return filepath.SkipDir
				}
				return nil
			}
			if rel == "receipt.json" || rel == "artifact-index.json" {
				return nil
			}
			i, e := d.Info()
			must(e)
			if !i.Mode().IsRegular() {
				return nil
			}
			artifacts = append(artifacts, map[string]any{"path": filepath.ToSlash(rel), "bytes": i.Size(), "sha256": fileHash(p)})
			return nil
		}))
		save(out+"/artifact-index.json", artifacts)
		report["artifactIndexSha256"] = fileHash(out + "/artifact-index.json")
		save(out+"/receipt.json", report)
		fmt.Printf("R17_RECEIPT %s/receipt.json sha256=%s\n", out, fileHash(out+"/receipt.json"))
	}
	os.Exit(code)
}
