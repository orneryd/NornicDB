package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"strings"
)

func main() {
	names, err := exec.Command("git", "diff", "--name-only", "459bfbe81e38aecf32c79246b585ec8316aa6f9d", "--", "*.go").Output()
	if err != nil {
		panic(err)
	}
	extra, err := exec.Command("git", "ls-files", "--others", "--exclude-standard", "--", "pkg").Output()
	if err != nil {
		panic(err)
	}
	for _, name := range strings.Fields(string(extra)) {
		if strings.HasSuffix(name, ".go") {
			names = append(names, []byte("\n"+name)...)
		}
	}
	directory, err := os.MkdirTemp("", "pipeline-format-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(directory)
	for _, name := range strings.Fields(string(names)) {
		data, err := os.ReadFile(name)
		if err != nil {
			panic(err)
		}
		formatted, err := format.Source(data)
		if err != nil {
			panic(fmt.Errorf("%s: %w", name, err))
		}
		if bytes.Equal(data, formatted) {
			continue
		}
		if err := os.WriteFile(directory+"/before", data, 0600); err != nil {
			panic(err)
		}
		if err := os.WriteFile(directory+"/after", formatted, 0600); err != nil {
			panic(err)
		}
		patch, _ := exec.Command("diff", "-u", directory+"/before", directory+"/after").Output()
		result, _ := json.Marshal(map[string]interface{}{"path": name, "sha": fmt.Sprintf("%x", sha256.Sum256(data)), "patch": base64.StdEncoding.EncodeToString(patch)})
		fmt.Printf("PIPELINE_FORMAT_PATCH:%s\n", result)
	}
}
