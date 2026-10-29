//go:build cypheremission

package cyphertext

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var emissionRecording = struct {
	sync.Mutex
	seen   map[string]bool
	traces map[emissionTrace]bool
}{seen: make(map[string]bool), traces: make(map[emissionTrace]bool)}

type emissionTrace struct {
	PCs          [96]uintptr
	Roles, Input string
}

var emissionRoot = func() string {
	_, file, _, _ := runtime.Caller(0)
	return strings.TrimSuffix(filepath.ToSlash(file), "internal/cyphertext/emission_record.go")
}()

func RecordPropertyReference() { recordNameEmission([]string{"property-ref"}, "written") }

func RecordNameConstruction(role string) { recordNameEmission([]string{role}, "decoded") }

// Each record identifies an observed call instruction, not a test symbol. Keep
// the production stack so delegation through a wrapper credits its real caller
// as well as the wrapper. The census joins these frames to parsed call sites.
func recordNameEmission(roles []string, input string) {
	path := os.Getenv("NORNIC_NAME_RECORDING")
	if path == "" {
		return
	}
	if len(roles) == 0 {
		roles = []string{"variable"}
	}
	trace := emissionTrace{Roles: strings.Join(roles, ","), Input: input}
	n := runtime.Callers(2, trace.PCs[:])
	emissionRecording.Lock()
	defer emissionRecording.Unlock()
	if emissionRecording.traces[trace] {
		return
	}
	emissionRecording.traces[trace] = true
	frames := runtime.CallersFrames(trace.PCs[:n])
	var stack []runtime.Frame
	subject := ""
	for {
		frame, more := frames.Next()
		stack = append(stack, frame)
		file := strings.TrimPrefix(filepath.ToSlash(frame.File), emissionRoot)
		if at := strings.Index(frame.Function, ".Test"); strings.HasSuffix(file, "_test.go") && at >= 0 {
			name := strings.Split(frame.Function[at+1:], ".")[0]
			subject = file + "#" + name
		}
		if !more {
			break
		}
	}
	for _, frame := range stack {
		file := filepath.ToSlash(frame.File)
		file = strings.TrimPrefix(file, emissionRoot)
		owner := file == "internal/cyphertext/identifier.go" || file == "internal/cyphertext/emission_record.go"
		production := !strings.HasSuffix(file, "_test.go") && !strings.HasPrefix(file, "internal/testutil/") && (strings.HasPrefix(file, "pkg/") || strings.HasPrefix(file, "apoc/") || strings.HasPrefix(file, "plugins/") || strings.HasPrefix(file, "scripts/") || strings.HasPrefix(file, "internal/"))
		if production && !owner {
			fn := frame.Function
			if at := strings.LastIndex(fn, "/"); at >= 0 {
				fn = fn[at+1:]
			}
			if at := strings.Index(fn, "."); at >= 0 {
				fn = fn[at+1:]
			}
			for _, role := range roles {
				record := struct {
					Runtime, File, Function, Role, Input, Subject string
					Line                                          int
				}{"go", file, fn, role, input, subject, frame.Line}
				data, err := json.Marshal(record)
				if err != nil {
					panic(err)
				}
				key := string(data)
				if !emissionRecording.seen[key] {
					emissionRecording.seen[key] = true
					f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
					if err != nil {
						panic(err)
					}
					_, writeErr := fmt.Fprintln(f, key)
					closeErr := f.Close()
					if writeErr != nil {
						panic(writeErr)
					}
					if closeErr != nil {
						panic(closeErr)
					}
				}
			}
		}
	}
}
