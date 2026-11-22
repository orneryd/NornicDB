# Classify the internal remote HTTP status copy

- [x] Reproduce the exact inventory failure on clean release a5b7980e89171764091a6d47d0b8a8ee3ca19c88.
- [x] Resolve the fingerprint with the checker's canonical AST formatting and SHA256.
- [x] Trace typed value, sole consumer, unchanged URL composition/authentication and redaction owners.
- [x] Parent approves one non-egress classification; no transport policy or checker changes.
- [x] Same inventory test GREEN, related controls, vet and applicable verification.
- [x] Parent reviewed exact JSON diff and authorized draft publication with scoped results.
- [ ] Publish draft and result comment; preserve evidence and clean disposable task files.

The fingerprint is the integer assignment `txResp.statusCode = resp.StatusCode`,
introduced by merged #1922. `net/http.Response.StatusCode` and the unexported
`remoteTxResponse.statusCode` are both int. The sole reader selects confirmed
2xx statement-abort handling. It is not URL/credential data or a network sink.

The existing source-owner chain remains NewRemoteEngine scheme admission,
ComposeRequestURL parsing/composition, httpTransport auth construction, and
deferred RedactClientError. Compose.Outcomes preserves multiple errors; the
redaction owner recursively processes their causes. No broader origin-policy
or runtime security guarantee is inferred from this inventory-only review.

Delegation assessment: one mechanical reviewed inventory entry; a new worker
would duplicate the completed parent/agent source audit without useful scope.
No new worker, external write, payload experiment or production edit.
