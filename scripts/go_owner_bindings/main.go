// Command go_owner_bindings emits object-binding metadata for Go functions.
//
// It reads one JSON request from standard input:
//
//	{"root": "/abs/path", "files": ["pkg/a.go", "pkg/b_test.go"]}
//
// Relative source files identify package directories to inspect. Directories
// are inspected through go/build.Context.MatchFile; same-package _test.go files
// are included in the package proof, while external `_test` packages are
// inspected as their own package. Each package is parsed with go/parser and
// type-checked with go/types, and the result describes every function
// declaration together with the calls in its body.
//
// The output is deterministic JSON:
//
//	{"root": "...", "ok": true, "packages": [ ... ], "errors": [ ... ]}
//
// The helper is evidence only: it never decides duplicate ownership and never
// guesses an unresolved symbol target. Calls whose target object cannot be
// bound through go/types are reported with kind package_unresolved, unknown or
// builtin and carry no trusted target.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// request is the single JSON request accepted on standard input.
type request struct {
	Root  string   `json:"root"`
	Files []string `json:"files"`
}

// output is the deterministic JSON document written to standard output.
type output struct {
	Root     string         `json:"root"`
	OK       bool           `json:"ok"`
	Packages []*packageInfo `json:"packages"`
	Errors   []diagnostic   `json:"errors"`
}

// diagnostic is a structured message. It never carries a resolved symbol
// identity and must not be used as proof of a binding.
type diagnostic struct {
	Path    string `json:"path,omitempty"`
	Line    int    `json:"line,omitempty"`
	Column  int    `json:"column,omitempty"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

// packageInfo describes one inspected package.
type packageInfo struct {
	Name            string       `json:"name"`
	Dir             string       `json:"dir"`
	ImportPath      string       `json:"import_path"`
	IsTest          bool         `json:"is_test"`
	Files           []fileInfo   `json:"files"`
	Diagnostics     []diagnostic `json:"diagnostics"`
	TypeCheckFailed bool         `json:"typecheck_failed"`
}

// fileInfo describes one parsed file.
type fileInfo struct {
	Path     string     `json:"path"`
	Package  string     `json:"package"`
	TestFile bool       `json:"test_file"`
	Funcs    []funcInfo `json:"funcs"`
}

// funcInfo describes one function or method declaration.
type funcInfo struct {
	Name             string     `json:"name"`
	Receiver         string     `json:"receiver"`
	IsMethod         bool       `json:"is_method"`
	TestKind         string     `json:"test_kind,omitempty"`
	TestSigOK        bool       `json:"test_signature_ok"`
	Decl             posInfo    `json:"decl"`
	Body             *posInfo   `json:"body"`
	TypeText         string     `json:"type"`
	Params           []varInfo  `json:"params"`
	Results          []varInfo  `json:"results"`
	Calls            []callInfo `json:"calls"`
	Writes           []objID    `json:"writes"`
	StateWrites      []objID    `json:"state_writes"`
	UnresolvedWrites int        `json:"unresolved_writes"`
}

// posInfo is a byte-offset and line span within one file.
type posInfo struct {
	Start int `json:"start"`
	End   int `json:"end"`
	Line  int `json:"line"`
	EndLn int `json:"end_line"`
}

// varInfo describes a parameter or result.
type varInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Variadic bool   `json:"variadic"`
}

// objID is a stable object identity: the defining file (relative to root) and
// byte offset of the object, so same-name shadowed objects differ.
type objID struct {
	File string `json:"file"`
	Pos  int    `json:"pos"`
	Type string `json:"type"`
}

// target identifies a bound call target.
type target struct {
	File     string `json:"file"`
	Name     string `json:"name"`
	Line     int    `json:"line"`
	Receiver string `json:"receiver"`
	Package  string `json:"package"`
}

// callInfo describes one call expression inside a function body.
type callInfo struct {
	Line            int     `json:"line"`
	Kind            string  `json:"kind"`
	Target          *target `json:"target"`
	ImportPath      string  `json:"import_path,omitempty"`
	Assigned        []objID `json:"assigned"`
	Args            []objID `json:"args"`
	ArgCount        int     `json:"arg_count"`
	Ellipsis        bool    `json:"ellipsis"`
	InFuncLiteral   bool    `json:"in_func_literal"`
	FuncLiteralArg  bool    `json:"func_literal_argument"`
	FuncLiteralLine int     `json:"func_literal_line,omitempty"`
	OuterCallLine   int     `json:"outer_call_line,omitempty"`
	OuterCallArgIdx *int    `json:"outer_call_arg_index,omitempty"`
}

// call kinds.
const (
	kindFree     = "free"
	kindMethod   = "method"
	kindBuiltin  = "builtin"
	kindPkgUnres = "package_unresolved"
	kindUnknown  = "unknown"
)

// supportedGoVersion lets the type checker import current standard-library
// sources through the shared stdlib importer.
const supportedGoVersion = "go1.26"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s < request.json\n", os.Args[0])
	}
}

func main() {
	if err := run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "go_owner_bindings:", err)
		os.Exit(1)
	}
}

// run reads the request, analyzes it and writes the result.
func run(in io.Reader, out io.Writer) error {
	data, err := io.ReadAll(in)
	if err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return errors.New("empty request on stdin")
	}
	var req request
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	res := analyze(req)
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	return nil
}
