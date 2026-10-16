package cypher

import (
	"crypto/sha1"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// uuid.v5, a NornicDB extension (Neo4j and APOC have no name-based UUID):
// a key that MERGE can deduplicate on.
func init() {
	cypherfn.Register("uuid.v5", cypherfn.PrivilegeNone, fnUUIDv5)
}

// fnUUIDv5 is uuid.v5(namespace, name): the version 5 (SHA-1, name-based)
// UUID of name in namespace (RFC 9562), a UUID or its text. The same
// namespace and name always give the same UUID. Null in, null out.
func fnUUIDv5(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, argumentCountError("uuid.v5", "2", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil || values[1] == nil {
		return nil, err
	}
	namespace, ok := values[0].(CypherUUID)
	if text, isText := values[0].(string); isText {
		namespace, ok = parseCypherUUID(text)
	}
	if !ok {
		return nil, &cypherfn.TypeMismatchError{Function: "uuid.v5", Expected: "UUID", Value: values[0]}
	}
	name, isText := values[1].(string)
	if !isText {
		return nil, &cypherfn.TypeMismatchError{Function: "uuid.v5", Expected: "String", Value: values[1]}
	}
	digest := sha1.New()
	_, _ = digest.Write(namespace.bits[:])
	_, _ = digest.Write([]byte(name))
	var u CypherUUID
	copy(u.bits[:], digest.Sum(nil))
	u.bits[6] = u.bits[6]&0x0f | 0x50 // version 5
	u.bits[8] = u.bits[8]&0x3f | 0x80 // RFC 9562 variant
	return u, nil
}
