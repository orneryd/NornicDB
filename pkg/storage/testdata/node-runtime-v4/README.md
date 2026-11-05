# Frozen node payloads

Captured through the original native and replication codecs at pristine release
commit `0c0495b6db64453ed966923e0c8d114956ccdc50`, before the runtime Go API migration.
These fixtures are immutable compatibility evidence, not regenerated expectations.
`sha256.json` records their captured hashes. Native bodies include the existing
serialization header. The tests compare reader values and writer bytes with an
independent frozen field schema, preserving map ordering options and nil values.

Cases cover zero values, empty containers, both storage flags, query provenance,
Unicode, numeric/slice property types, timestamps, metadata and embeddings. The
oversize native body retains its original separated-embedding marker; reading it
alone does not hydrate the separately stored vectors.
