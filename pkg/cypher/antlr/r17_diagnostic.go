// Author: Codex R17 lane. Temporary scratch-only telemetry; never part of fix.
package antlr
import("crypto/sha256";"fmt";"sync")
var r17Diagnostic = struct{sync.Mutex; counts map[string]int}{counts:map[string]int{}}
func r17DiagnosticRecord(event,source string,entry SyntaxEntry,stage SourceStage){h:=sha256.Sum256([]byte(source));key:=fmt.Sprintf("%s:entry=%d:stage=%d:bytes=%d:sha=%x",event,entry,stage,len(source),h);r17Diagnostic.Lock();r17Diagnostic.counts[key]++;r17Diagnostic.Unlock()}
func R17DiagnosticTake()map[string]int{r17Diagnostic.Lock();defer r17Diagnostic.Unlock();out:=r17Diagnostic.counts;r17Diagnostic.counts=map[string]int{};return out}
