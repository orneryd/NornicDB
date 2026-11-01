# Numeric JSON data paths

`convert.PropertyJSON` owns existing graph/property documents. It encodes and
reads graph exports/imports, backups, APOC JSON document transforms, legacy
recovery/replication, and result properties. Integer lexemes retain exact values:
int64 first, then uint64 above MaxInt64. Integers outside both domains fail.
FLOATs, including whole and subnormal values, retain a fractional/exponent token.
JSON does not carry unsigned-width tags: uint64 at or below MaxInt64 reads as the
canonical signed INTEGER with the same value.

Internal state has a separate compatibility boundary. Schema definitions at
version 0/1 and replication's legacy JSON frames use the shared
`PropertyJSON.UnmarshalLegacyState` reader, which preserves the former
`encoding/json` float64 interpretation. Schema version 2 uses exact PropertyJSON;
new replication frames keep the existing typed Gob encoding. A rewritten legacy
schema stores its already-decoded FLOAT values in version 2. DOMAIN equality
continues to compare INTEGERs and FLOATs exactly, without float64 widening.
See [the persisted-reader inventory](numeric_state_compatibility.md).

`convert.CypherInputJSON` owns external values: Cypher parameters, HTTP
request bodies, explicit fromJson conversion and load.json. It follows Neo4j
5.26: an integer lexeme inside int64 is an INTEGER; one outside int64 is
rejected with Neo4j's "Unsupported type of Number" error
(`convert.UnsupportedNumberError`); decimal and exponent lexemes are FLOATs, and
one outside the FLOAT range reads as +/-Infinity. There is no text fallback.
Input serialization preserves FLOAT lexemes before those external readers.

Out of scope in v1.4.4: MCP tool arguments and results (`pkg/mcp`) and
Heimdall's in-process tool calls keep the release decoders and are not covered
by this contract. Those decoders can still round an integer above 2^53 (for
example 9007199254740993 reads as 9007199254740992). They are deferred to the
1.5.0 MCP work.

These are distinct, zero-state domain types. Their constructors expose no number
policy argument or stdlib decoder. The generic JSON entry points are removed.
APOC source-opening helpers return readers; they cannot accept a decoder factory.
The public graph-import operation selects its persisted domain directly.

Both domain writers use the same direct encoder. It traverses borrowed values
once, appends numeric text at the leaf, and caches field metadata without
materializing proxy structs or cloning maps/lists. Typed fields and array elements
reuse compiled scalar operations. Custom JSON/text marshalers,
raw JSON, binary values and supported `,string` fields retain their standard
library handling. Unsupported `,string` options on containers are ignored while
their nested FLOATs still retain type. FLOAT text and the public string formatter
share the same append routine, including shortest float32 text and negative zero.
Private writer state cannot be constructed or escaped outside the domain owner;
the source guard has mutation fixtures for direct state, pooled state, initializer,
prepared-wrapper and encoder-construction bypasses.

Readers cache which declared types can contain interface values. Only those
fields need number normalization: typed control records, vectors and binary
payloads retain their standard decoder behavior without another object walk.
Each private decoder's concrete domain type fixes its numeric policy; there is
no per-reader policy field or exported standard decoder. Recursive type plans
cache only complete searches, so an interface reachable through a parent cannot
be mistaken for a fully typed subtree. The policy remains at the domain owner.

The authority for production consumers is [json_data_paths.json](json_data_paths.json).
Every registered consumer has one domain per data direction and an exact set of
entry-point references. HTTP and remote boundaries may read input and write
properties, or send input and receive properties; those are separate paths.
Changing a reader/writer or its domain fails `TestNumericWireOwnership`.
`TestNumericDomainPolicyMutations` checks the positive fixture and flips every
registered reference to the opposite domain, requiring the semantic diagnostic.
The legacy `cypher.JSONNumbers` forwarding API is classified as external input.

The guard scans production Go source across the repository, including all `pkg/`
and `apoc/` packages, replication and future files/packages. Raw JSON readers and
writers, method-value aliases and package initializers require individually
reviewed non-property exceptions in [numeric_wire_exceptions.json](numeric_wire_exceptions.json).
Those freeze typed binary/RawMessage envelopes, schemas, hashes, model protocols,
configuration, diagnostics and offline corpus/test tooling. They never exempt
wrong-domain owner calls. Vendored dependencies, the generated ANTLR parser and remote operator helper
overlays are outside the scan. Other generated Go source remains scanned. Mutation fixtures insert raw
`Unmarshal`/`NewDecoder` readers of any/interface/map values into storage,
replication, APOC and a new package; all must be rejected. Domain handles cannot
escape into unclassified aliases.

CDC uses the same native number/equality owners on stored values; its binary
capture format is unchanged. Bolt numeric leaves remain at `convert`: signed
int64 and IEEE float64. Native uint64 above MaxInt64 travels as an exact decimal
STRING, never a signed wrap or rounded FLOAT. Neo4j 5.26 has no unsigned
PackStream integer; this is NornicDB's extension representation, not unsigned or
Enterprise CDC parity. HTTP transaction nonfinite FLOATs retain Neo4j's text
representation; ordinary JSON rejects nonfinite values. Existing custom
serializers, raw JSON, binary data and float32 shortest-text behavior remain.

The complete boundary includes graph loading/conversion, graph export/import,
backup/restore/retention export, document transforms and JSONL parsing, WAL and
legacy recovery, replication fallback, HTTP and remote values, GraphQL
arguments and results, and the migration script's source properties and
destination parameters (MCP and Heimdall in-process tool paths excluded, see
above). Native MessagePack/Gob and declared CSV/GraphML numeric types keep
their existing protocol contracts. The PR Evidence table lists each registered
consumer and its domains; non-property exceptions retain their own reasons.

Old backups/exports may already contain `2` for a whole FLOAT originally equal to
`2.0`. That text cannot distinguish it from INTEGER `2`; it remains INTEGER when
read. No heuristic can recover the lost type. New writers preserve the token.
For example, an old writer emitted `1152921504606847000` for the whole FLOAT
`float64(1<<60)` (exactly `1152921504606846976`). Backup/export imports intentionally
restore the INTEGER token `1152921504606847000`; the original FLOAT value and kind
cannot be recovered at that import boundary. New JSON backups carry version
`2.0` and preserve FLOAT kind and negative zero. This import policy is distinct
from versioned internal-state compatibility above.
