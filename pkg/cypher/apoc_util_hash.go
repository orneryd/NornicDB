package cypher

import (
	"reflect"
	"runtime"

	"github.com/orneryd/nornicdb/apoc/util"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Bundled digest handlers share the native Cypher owner, including its
// required root-list argument. Third-party overrides keep their contract.
// This also covers develop before the separate #1422 export fix lands.
func nativeAPOCDigestHandler(name string, handler interface{}) bool {
	var owner string
	switch name {
	case "apoc.util.md5":
		owner = "MD5"
	case "apoc.util.sha1":
		owner = "SHA1"
	case "apoc.util.sha256":
		owner = "SHA256"
	default:
		return false
	}
	fn, ok := handler.(func(interface{}) string)
	if !ok || fn == nil {
		return false
	}
	symbol := runtime.FuncForPC(reflect.ValueOf(fn).Pointer())
	return symbol != nil && symbol.Name() == "github.com/orneryd/nornicdb/apoc/util."+owner
}

// APOC's hash functions, callable without the APOC plugin. The plugin's
// registrations call the same apoc/util functions, so a loaded plugin (which
// overrides a built-in of the same name) digests the same text.
func init() {
	for name, digest := range map[string]func(interface{}) string{
		"apoc.util.md5":    util.MD5,
		"apoc.util.sha1":   util.SHA1,
		"apoc.util.sha256": util.SHA256,
	} {
		cypherfn.Register(name, apocUtilHash(name, digest))
	}
}

// apocUtilHash is apoc.util.md5 / sha1 / sha256(values): the lower-case hex
// digest of the concatenated string forms of the values (util.DigestText),
// so apoc.util.sha256([text]) is the SHA-256 of text's UTF-8 bytes. A value
// that isn't a list digests its own string form (a NornicDB extension).
// A null argument list fails as APOC's hexHash iteration does; null elements
// within a supplied list contribute empty text recursively.
func apocUtilHash(name string, digest func(interface{}) string) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, argumentCountError(name, "1", len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil {
			return nil, err
		}
		if values[0] == nil {
			return nil, localizedStatusError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed", localization.CypherProceduresArgumentNull(name, "values"))
		}
		return digest(values[0]), nil
	}
}
