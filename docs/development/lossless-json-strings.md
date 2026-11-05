# Exact JSON strings at graph boundaries

The canonical `internal/jsontext` codec preserves escaped unpaired UTF16 units
as exact internal string bytes and emits standard JSON `\uXXXX` escapes. Valid
Unicode strings, valid surrogate pairs and literal backslash-u spellings keep
their original meaning. A historical stored name containing the six characters
`\ud800` remains those characters; storage loading does not run Cypher source
normalization or rename existing identities.

JSON object structure and numbers belong to the standard library's token
parser; typed binding uses the supported goccy buffer API. Dynamic exact-number
fields use `NumberMap` and `NumberList`, keeping values beyond 2^53 exact.
`RawMessage` retains standard JSON rather than leaking the transient internal
string representation. Nested typed recovery views select `ProjectedInput`
explicitly; client buffers and streams enter as raw JSON.

The format remains JSON. Ordinary backspace and formfeed may be written as
`\u0008` and `\u000c` rather than `\b` and `\f`; both old and new readers see the
same values. This cosmetic spelling change does not require a storage-version
migration. Existing valid opaque RawMessage bytes are retained before enclosing
serialization, and custom marshalers run once. New surrogate values extend the
admitted domain; this does not promise exact decoding of that new domain by
older readers that replaced such units.

Integrated consumers are the transaction HTTP request/response and remote HTTP
value paths, APOC JSON conversion, native catalog/schema metadata and WAL/recovery
JSON string fields. Native entity/dictionary binary encodings, WAL framing and
snapshot binary formats are unchanged. Credential V4 migration is a separate
explicit release migration; this string codec adds no startup identity rewrite,
shadow records or storage-format version.

Standard-library error types and original input offsets are retained for typed
JSON mismatches. Error field paths use bounded read-only struct-tag metadata;
this does not bind or mutate decoded values through reflection. HTTP framing,
request-size limits and each consumer's trailing-value policy remain owned by
the original boundary.
