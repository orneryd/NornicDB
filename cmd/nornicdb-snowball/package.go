package main

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/pluginbuild"
	"github.com/orneryd/nornicdb/pkg/search/stemmer"
)

type packageOptions struct {
	Language string
	ID       string
	Version  string
	Module   string
	Source   string
	Output   string

	// Target is the nornicdb binary that will load the plugin. The plugin is
	// built with the target's recorded build settings (a best effort, see
	// internal/pluginbuild), then kept only if `<Target> stemmer check`
	// accepts it: the target's own loader decides. Empty builds with the
	// toolchain defaults and does not verify the plugin.
	Target string

	// CheckTimeout bounds the target's check; zero means defaultCheckTimeout.
	CheckTimeout time.Duration
}

func parsePackageArgs(args []string) (packageOptions, error) {
	fs := flag.NewFlagSet("package", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var opts packageOptions
	fs.StringVar(&opts.Language, "language", "", "Snowball language label")
	fs.StringVar(&opts.ID, "id", "", "stemmer plugin ID")
	fs.StringVar(&opts.Version, "version", "", "stemmer plugin version")
	fs.StringVar(&opts.Module, "module", "", "local vendored Go module directory")
	fs.StringVar(&opts.Source, "source", "", "generated Snowball Go source file")
	fs.StringVar(&opts.Output, "output", "", "output .so plugin path")
	fs.StringVar(&opts.Target, "target", "", "nornicdb binary that will load the plugin; the plugin is built to match it and verified by it")
	if err := fs.Parse(args); err != nil {
		return packageOptions{}, err
	}
	if fs.NArg() != 0 {
		return packageOptions{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	if err := validatePackageOptions(opts); err != nil {
		return packageOptions{}, err
	}
	return opts, nil
}

func validatePackageOptions(opts packageOptions) error {
	missing := make([]string, 0, 6)
	if strings.TrimSpace(opts.Language) == "" {
		missing = append(missing, "--language")
	}
	if strings.TrimSpace(opts.ID) == "" {
		missing = append(missing, "--id")
	}
	if strings.TrimSpace(opts.Version) == "" {
		missing = append(missing, "--version")
	}
	if strings.TrimSpace(opts.Module) == "" {
		missing = append(missing, "--module")
	}
	if strings.TrimSpace(opts.Source) == "" {
		missing = append(missing, "--source")
	}
	if strings.TrimSpace(opts.Output) == "" {
		missing = append(missing, "--output")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required flags: %s", strings.Join(missing, ", "))
	}
	if !stemmer.ValidPluginID(strings.ToLower(strings.TrimSpace(opts.ID))) {
		return fmt.Errorf("invalid stemmer plugin id %q", opts.ID)
	}
	if filepath.Base(opts.Output) != opts.Output && strings.TrimSpace(filepath.Base(opts.Output)) == "" {
		return fmt.Errorf("invalid output path %q", opts.Output)
	}
	if filepath.Ext(opts.Output) != ".so" {
		return fmt.Errorf("--output must end in .so")
	}
	return nil
}

func packageSnowball(opts packageOptions) error {
	if runtime.GOOS == "windows" {
		return errors.New("Go plugin buildmode is not supported on Windows")
	}
	moduleDir, err := filepath.Abs(opts.Module)
	if err != nil {
		return err
	}
	sourcePath, err := filepath.Abs(opts.Source)
	if err != nil {
		return err
	}
	outputPath, err := filepath.Abs(opts.Output)
	if err != nil {
		return err
	}
	if err := validateModule(moduleDir, sourcePath); err != nil {
		return err
	}
	sourceRel, err := filepath.Rel(moduleDir, sourcePath)
	if err != nil || strings.HasPrefix(sourceRel, ".."+string(os.PathSeparator)) || sourceRel == ".." || filepath.IsAbs(sourceRel) {
		return fmt.Errorf("--source must be inside --module")
	}
	runtimeImport, err := detectSnowballRuntimeImport(sourcePath)
	if err != nil {
		return err
	}
	var target *pluginbuild.Config
	if opts.Target != "" {
		// The check runs from a temporary directory, so the target is
		// resolved once, here, and that one file is both read and run.
		opts.Target, err = resolveTarget(opts.Target)
		if err != nil {
			return fmt.Errorf("--target: %w", err)
		}
		cfg, err := pluginbuild.FromBinary(opts.Target)
		if err != nil {
			return fmt.Errorf("--target: %w", err)
		}
		target = &cfg
	}
	tmpRoot, err := os.MkdirTemp("", "nornicdb-snowball-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpRoot)
	tmpModule := filepath.Join(tmpRoot, "module")
	if err := copyTree(moduleDir, tmpModule); err != nil {
		return err
	}
	runtimeImport, err = isolateSnowballRuntime(tmpModule, sourceRel, runtimeImport, opts.ID)
	if err != nil {
		return err
	}
	buildDir := filepath.Join(tmpModule, filepath.Dir(sourceRel))
	if err := os.WriteFile(filepath.Join(buildDir, "nornicdb_stemmer_bridge.go"), []byte(renderBridgeSource(runtimeImport)), 0o644); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	stageDir, err := os.MkdirTemp(filepath.Dir(outputPath), ".nornicdb-snowball-*")
	if err != nil {
		return err
	}
	stagedPath := filepath.Join(stageDir, filepath.Base(outputPath))
	defer func() {
		_ = os.Remove(stagedPath)
		_ = os.Remove(manifestPath(stagedPath))
		// Never recursively delete backups if restoring an old file failed.
		_ = os.Remove(stageDir)
	}()
	args := []string{"build", "-mod=vendor", "-buildmode=plugin"}
	env := append(os.Environ(), "GOPROXY=off")
	if target != nil {
		env = append(env, target.Env...)
		if err := requireTargetGoVersion(buildDir, env, opts.Target, target.GoVersion); err != nil {
			return err
		}
		args = append(args, target.Flags...)
	}
	args = append(args, "-o", stagedPath, ".")
	cmd := exec.Command("go", args...)
	cmd.Dir = buildDir
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go build plugin failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	digest, err := stemmer.SHA256File(stagedPath)
	if err != nil {
		return err
	}
	manifest := stemmer.Manifest{
		SchemaVersion: stemmer.ManifestSchemaVersion,
		APIVersion:    stemmer.APIVersion,
		Type:          stemmer.ManifestType,
		ID:            strings.ToLower(strings.TrimSpace(opts.ID)),
		Version:       strings.TrimSpace(opts.Version),
		Language:      strings.TrimSpace(opts.Language),
		Library:       filepath.Base(outputPath),
		SHA256:        digest,
		Entrypoint:    stemmer.EntrypointSymbol,
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(manifestPath(stagedPath), raw, 0o644); err != nil {
		return err
	}
	if target != nil {
		if err := verifyWithTarget(opts.Target, stagedPath, manifest, tmpRoot, opts.CheckTimeout); err != nil {
			return err
		}
	}
	return publishSnowballPackage(stagedPath, outputPath)
}

// defaultCheckTimeout bounds `<target> stemmer check` when CheckTimeout is
// zero. checkGrace is extra time for the target process to start and exit.
const (
	defaultCheckTimeout = 2 * time.Minute
	checkGrace          = 30 * time.Second
)

// resolveTarget returns the absolute path of the --target binary. A name
// without a path separator is looked up in PATH, as a shell would run it.
func resolveTarget(target string) (string, error) {
	if !strings.ContainsRune(target, '/') && !strings.ContainsRune(target, filepath.Separator) {
		found, err := exec.LookPath(target)
		if err != nil {
			return "", err
		}
		target = found
	}
	return filepath.Abs(target)
}

// verifyWithTarget runs `<target> stemmer check` on the packaged plugin, from
// workDir, and fails unless the target's loader accepts it within timeout:
// the check must exit 0 and print a result line for this manifest's plugin
// and library digest. On a refusal it adds the recorded build differences
// between the target and the plugin, which explain the refusal but never
// decide it.
func verifyWithTarget(targetPath, outputPath string, manifest stemmer.Manifest, workDir string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultCheckTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout+checkGrace)
	defer cancel()
	cmd := exec.CommandContext(ctx, targetPath, "stemmer", "check", "--timeout", timeout.String(), manifestPath(outputPath))
	cmd.Dir = workDir
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	output := strings.TrimSpace(string(out))
	switch {
	case err == nil:
		want := stemmer.CheckResult{ID: manifest.ID, Version: manifest.Version, APIVersion: manifest.APIVersion, SHA256: manifest.SHA256}
		if got, ok := stemmer.ParseCheckResult(output); !ok || got != want {
			return fmt.Errorf("--target %s exited without confirming that it loaded the plugin, which was removed (a plugin init function may have ended the process): %s", targetPath, output)
		}
		return nil
	case ctx.Err() != nil:
		return fmt.Errorf("--target %s did not finish checking the plugin within %s; the plugin was removed", targetPath, timeout+checkGrace)
	case strings.Contains(output, `unknown command "stemmer"`):
		return fmt.Errorf("--target %s cannot verify plugins (it has no `stemmer check` command); the plugin was removed. Upgrade the server, or package without --target to get an unverified plugin", targetPath)
	}
	message := fmt.Sprintf("--target %s refuses the plugin, which was removed: %v: %s", targetPath, err, output)
	if differences := recordedDifferences(targetPath, outputPath); len(differences) > 0 {
		message += "\nrecorded build differences (target vs plugin; they explain, the target decides):\n  " + strings.Join(differences, "\n  ")
	}
	return errors.New(message)
}

// recordedDifferences explains a refusal from both binaries' build
// information, when both are readable.
func recordedDifferences(targetPath, pluginPath string) []string {
	targetInfo, err := buildinfo.ReadFile(targetPath)
	if err != nil {
		return nil
	}
	pluginInfo, err := buildinfo.ReadFile(pluginPath)
	if err != nil {
		return nil
	}
	return pluginbuild.Differences(targetInfo, pluginInfo)
}

// requireTargetGoVersion fails unless the go command that builds the plugin is
// the release that built the target: plugin.Open refuses any other.
func requireTargetGoVersion(buildDir string, env []string, targetPath, targetVersion string) error {
	cmd := exec.Command("go", "env", "GOVERSION")
	cmd.Dir = buildDir
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("go env GOVERSION: %w", err)
	}
	if have := strings.TrimSpace(string(out)); have != targetVersion {
		return fmt.Errorf("--target %s was built with %s, but go is %s; build the plugin with %s", targetPath, targetVersion, have, targetVersion)
	}
	return nil
}

// isolateSnowballRuntime gives each generated plugin its own import identity.
// Go's plugin loader requires matching package paths to have identical build
// hashes; two independently vendored Snowball runtimes otherwise collide even
// when both plugins are valid.
func isolateSnowballRuntime(moduleDir, sourceRel, runtimeImport, pluginID string) (string, error) {
	isolatedModule := "nornicdb.local/stemmer/" + sanitizeModuleSegment(pluginID)
	goModPath := filepath.Join(moduleDir, "go.mod")
	goMod, err := os.ReadFile(goModPath)
	if err != nil {
		return "", err
	}
	lines := strings.Split(string(goMod), "\n")
	replaced := false
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "module ") {
			lines[i] = "module " + isolatedModule
			replaced = true
			break
		}
	}
	if !replaced {
		return "", fmt.Errorf("module must declare a module path")
	}
	if err := os.WriteFile(goModPath, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		return "", err
	}

	runtimeSource := filepath.Join(moduleDir, "vendor", filepath.FromSlash(runtimeImport))
	runtimeTarget := filepath.Join(moduleDir, "internal", "snowballruntime")
	if err := copyTree(runtimeSource, runtimeTarget); err != nil {
		return "", fmt.Errorf("copy vendored Snowball runtime: %w", err)
	}

	sourcePath := filepath.Join(moduleDir, sourceRel)
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		return "", err
	}
	isolatedImport := isolatedModule + "/internal/snowballruntime"
	oldQuoted := strconv.Quote(runtimeImport)
	newQuoted := strconv.Quote(isolatedImport)
	rewritten := bytes.ReplaceAll(source, []byte(oldQuoted), []byte(newQuoted))
	if bytes.Equal(source, rewritten) {
		return "", fmt.Errorf("generated source does not import detected Snowball runtime %q", runtimeImport)
	}
	if err := os.WriteFile(sourcePath, rewritten, 0o644); err != nil {
		return "", err
	}
	return isolatedImport, nil
}

func sanitizeModuleSegment(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var out strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			out.WriteRune(r)
		} else {
			out.WriteByte('-')
		}
	}
	if out.Len() == 0 {
		return "plugin"
	}
	return out.String()
}

func validateModule(moduleDir, sourcePath string) error {
	info, err := os.Stat(moduleDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("--module is not a directory: %s", moduleDir)
	}
	for _, name := range []string{"go.mod", "go.sum"} {
		if _, err := os.Stat(filepath.Join(moduleDir, name)); err != nil {
			return fmt.Errorf("module must contain %s: %w", name, err)
		}
	}
	vendorInfo, err := os.Stat(filepath.Join(moduleDir, "vendor"))
	if err != nil {
		return fmt.Errorf("module must contain vendored dependencies: %w", err)
	}
	if !vendorInfo.IsDir() {
		return fmt.Errorf("module vendor path is not a directory")
	}
	srcInfo, err := os.Stat(sourcePath)
	if err != nil {
		return err
	}
	if srcInfo.IsDir() {
		return fmt.Errorf("--source must be a Go source file")
	}
	return nil
}

func detectSnowballRuntimeImport(sourcePath string) (string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourcePath, nil, parser.ParseComments)
	if err != nil {
		return "", err
	}
	if file.Name.Name != "main" {
		return "", fmt.Errorf("generated Snowball source must use package main")
	}
	imports := make(map[string]string, len(file.Imports))
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return "", err
		}
		name := filepath.Base(path)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = path
	}
	var runtimeAlias string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Stem" || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 || !hasSingleBoolResult(fn.Type.Results) {
			continue
		}
		star, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		sel, ok := star.X.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Env" {
			continue
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok {
			continue
		}
		runtimeAlias = ident.Name
		break
	}
	if runtimeAlias == "" {
		return "", fmt.Errorf("generated source must define func Stem(env *snowballRuntime.Env) bool")
	}
	runtimeImport, ok := imports[runtimeAlias]
	if !ok {
		return "", fmt.Errorf("Snowball runtime import alias %q not found", runtimeAlias)
	}
	return runtimeImport, nil
}

func hasSingleBoolResult(results *ast.FieldList) bool {
	if results == nil || len(results.List) != 1 || len(results.List[0].Names) > 1 {
		return false
	}
	ident, ok := results.List[0].Type.(*ast.Ident)
	return ok && ident.Name == "bool"
}

func renderBridgeSource(runtimeImport string) string {
	return fmt.Sprintf(`package main

import (
	"sync"

	snowballRuntime %q
)

var envPool = sync.Pool{
	New: func() any { return snowballRuntime.NewEnv("") },
}

type generatedStemmer struct{}

func (generatedStemmer) Stem(token string) string {
	env := envPool.Get().(*snowballRuntime.Env)
	env.SetCurrent(token)
	Stem(env)
	result := env.Current()
	env.SetCurrent("")
	envPool.Put(env)
	return result
}

func (generatedStemmer) StemTokens(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}
	env := envPool.Get().(*snowballRuntime.Env)
	for i, token := range tokens {
		env.SetCurrent(token)
		Stem(env)
		tokens[i] = env.Current()
	}
	env.SetCurrent("")
	envPool.Put(env)
	return tokens
}

var Plugin generatedStemmer
`, runtimeImport)
}

func manifestPath(outputPath string) string {
	ext := filepath.Ext(outputPath)
	return strings.TrimSuffix(outputPath, ext) + ".stemmer.json"
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFile(path, target, info.Mode().Perm())
	})
}

func copyFile(source, target string, mode os.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
