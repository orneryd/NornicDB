package cypher

import (
	"github.com/orneryd/nornicdb/apoc/util"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// APOC's hash functions, callable without the APOC plugin. The plugin's
// registrations call the same apoc/util functions, so a loaded plugin (which
// overrides a built-in of the same name) digests the same text.
func init() {
	for name, digest := range map[string]func(interface{}) string{
		"apoc.util.md5":    util.MD5,
		"apoc.util.sha1":   util.SHA1,
		"apoc.util.sha256": util.SHA256,
	} {
		cypherfn.RegisterInvocation(name, apocUtilHash(name, digest))
	}
}

// apocUtilHash is apoc.util.md5 / sha1 / sha256(values): the lower-case hex
// digest of the concatenated string forms of the values (util.DigestText),
// so apoc.util.sha256([text]) is the SHA-256 of text's UTF-8 bytes. A value
// that isn't a list digests its own string form (a NornicDB extension).
// A null argument list fails as APOC's hexHash iteration does; null elements
// within a supplied list contribute empty text recursively.
func apocUtilHash(name string, digest func(interface{}) string) cypherfn.InvocationFunc {
	return func(ctx cypherfn.InvocationContext, args []string) (interface{}, error) {
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
