// Scratch diagnostic for the v1.5.2 verification (damaged node bodies).
// Not product code: it logs the deciding value at each suspected owner.
package bolt

import (
	"reflect"
	"errors"
	"log"
	"runtime"
	"strings"
)

var _ = errors.Unwrap
var _ = strings.Join

func v152Diag(format string, args ...any) { log.Printf(format, args...) }

func v152Caller(skip int) string {
	var parts []string
	for i := skip; i < skip+4; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		name := ""
		if fn := runtime.FuncForPC(pc); fn != nil {
			name = fn.Name()
		}
		if index := strings.LastIndex(file, "/"); index >= 0 {
			file = file[index+1:]
		}
		parts = append(parts, name+"@"+file+":"+v152Itoa(line))
	}
	return strings.Join(parts, " <- ")
}

func v152Itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func v152Chain(err error) string {
	var parts []string
	for e := err; e != nil; e = errors.Unwrap(e) {
		parts = append(parts, typeName(e))
	}
	return strings.Join(parts, " > ")
}

func typeName(v any) string { return reflect.TypeOf(v).String() }
