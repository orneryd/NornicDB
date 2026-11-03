package workcensus

import (
	"bytes"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

func TestProgramLoadedSourceSnapshot(t *testing.T) {
	c := Config{Dir: t.TempDir(), Patterns: []string{"./..."}, BuildFlags: []string{"-tags=loaded"}, Env: append(os.Environ(), "LOADED_SOURCE_TEST=original")}
	for name, raw := range map[string]string{"go.mod": "module loaded.fixture\n\ngo 1.26\n", "source.go": "package fixture\nfunc diskOnly() int { return 1 }\n"} {
		if err := os.WriteFile(filepath.Join(c.Dir, name), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(c.Dir, "overlay_only.go")
	original := []byte("package fixture\nfunc overlayOnly() int { return 1 }\n")
	c.Overlay = map[string][]byte{path: bytes.Clone(original)}
	p, err := LoadProgram(c)
	if err != nil {
		t.Fatal(err)
	}
	module, err := p.ModuleConfig()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.Dir, "go.mod"), []byte("changed after load"), 0600); err != nil {
		t.Fatal(err)
	}
	moduleAgain, err := p.ModuleConfig()
	if err != nil || !bytes.Equal(module, moduleAgain) {
		t.Fatalf("module snapshot changed: %v", err)
	}
	moduleAgain[0] = '!'
	moduleAgain, err = p.ModuleConfig()
	if err != nil || !bytes.Equal(module, moduleAgain) {
		t.Fatal("module snapshot escaped")
	}
	c.BuildFlags[0] = "-tags=mutated"
	c.Env[len(c.Env)-1] = "LOADED_SOURCE_TEST=mutated"
	if p.load.BuildFlags[0] != "-tags=loaded" || p.load.Env[len(p.load.Env)-1] != "LOADED_SOURCE_TEST=original" {
		t.Fatal("caller mutated retained load settings")
	}
	c.Overlay[path][0] = '!'
	delete(c.Overlay, path)
	got, err := p.LoadedSource(path)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("effective overlay = %q, %v", got, err)
	}
	got[0] = '!'
	got, err = p.LoadedSource(path)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("returned bytes mutated snapshot: %q, %v", got, err)
	}
	disk := filepath.Join(c.Dir, "source.go")
	want, err := os.ReadFile(disk)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(disk, []byte("changed after compilation"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err = p.LoadedSource(disk)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("disk source differs: %v", err)
	}
	if _, err = p.LoadedSource(filepath.Join(c.Dir, "missing.go")); !os.IsNotExist(err) {
		t.Fatalf("missing source error = %v", err)
	}
}

func TestLoadedSourceRejectsInconsistentPackageVariants(t *testing.T) {
	s := &loadedSources{files: map[string][]byte{}}
	path := "variant.go"
	for i, raw := range []string{"package fixture\nfunc value() int{return 1}", "package fixture\nfunc value() int{return 1}", "package fixture\nfunc value() int{return 2}"} {
		_, err := s.parse(token.NewFileSet(), path, []byte(raw))
		if (err != nil) != (i == 2) {
			t.Fatalf("variant%d error=%v", i, err)
		}
	}
}
