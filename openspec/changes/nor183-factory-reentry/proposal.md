# NOR-183: allow remote factories to reenter the catalog

GetStorageWithAuth invoked foreign RemoteEngineFactory callbacks while holding the manager metadata lock. Dotted remote selection and composite root construction could deadlock when the callback read the catalog, returned an error, or closed the manager.

Resolve immutable catalog identities and local namespace views under the existing metadata lock, then construct remote engines outside it. Keep one factory call per alias per root selection, request authentication, routing, configured transaction timeout, and existing cache policy. No new revocation or global factory-sharing policy.

The existing listener/pending-listener callback regressions are retained verbatim. The rejected-factory fixture now closes its own synthetic rejected resource. A two-remote/one-local test verifies real composite results and provenance, separate credentials, and callback reentry.
