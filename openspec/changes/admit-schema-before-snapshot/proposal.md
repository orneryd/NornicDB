# Admit schema statements before their snapshot

An auto-commit schema statement must publish indexes built from the population
it validated. A concurrent data commit must not make ordinary CREATE/DROP INDEX
fail because schema admission began after the snapshot. Explicit transactions
retain their own snapshot and conflict semantics.

Native Go callers select OrdinaryBegin or AutocommitSchemaBegin. The latter
owns exclusive lifecycle admission before its snapshot and releases it at the
terminal outcome. Temporal derived keys are staged in that same transaction,
not published by a separate maintenance write. Unrelated schema changes retain
existing temporal-index completeness marks.

Badger keys, stored payloads, WAL encoding and storage version are unchanged.
Go embedding callers must migrate; there is no compatibility wrapper.
