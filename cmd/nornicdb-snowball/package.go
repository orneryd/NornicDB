package main

import (
	"bytes"
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

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
)

type packageOptions struct {
	Language string
	ID       string
	Version  string
	Module   string
	Source   string
	Output   string
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
	tmpRoot, err := os.MkdirTemp("", "nornicdb-snowball-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpRoot)
	tmpModule := filepath.Join(tmpRoot, "module")
	if err := copyTree(moduleDir, tmpModule); err != nil {
		return err
	}
	buildDir := filepath.Join(tmpModule, filepath.Dir(sourceRel))
	if err := os.WriteFile(filepath.Join(buildDir, "nornicdb_stemmer_bridge.go"), []byte(renderBridgeSource(runtimeImport)), 0o644); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	cmd := exec.Command("go", "build", "-mod=vendor", "-buildmode=plugin", "-o", outputPath, ".")
	cmd.Dir = buildDir
	cmd.Env = append(os.Environ(), "GOPROXY=off")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go build plugin failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	digest, err := stemmer.SHA256File(outputPath)
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
	return os.WriteFile(manifestPath(outputPath), raw, 0o644)
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
		if !ok || fn.Name.Name != "Stem" || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
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
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
}
