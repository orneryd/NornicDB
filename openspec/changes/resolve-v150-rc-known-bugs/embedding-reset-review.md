# Embedding reset failure propagation

CI on f5cfe9bd8 exposes a real masked stored-node read error in
ClearAllEmbeddingsForPrefix. Public regressions reproduce silent success after
malformed records and a later rejected target on the pre-correction source.

The scan is bounded by the canonical node key prefix; malformed or mismatched
in-scope bodies fail before any reset writes. Out-of-scope malformed bodies and
an unscoped bare key cannot block a namespace reset. IDs come from the actual
keys and must agree with decoded bodies. Each selected node is read and cleared
inside the existing inMutationTransaction owner, using stored visibility,
observed Before/After and StoredNodeImages. Read/staging/commit errors stop the
operation with completed per-node progress. No error skip, forwarding mutation
wrapper, owner exemption or new analyzer rule is introduced.

The old global pending-index refresh is removed: the canonical node update
already publishes each affected pending marker atomically. Hidden nodes reset
without changing suppression; other namespaces retain vectors and queues.
The operation remains per-node, not globally atomic, and is not represented as
a transactional all-or-nothing administrative reset. Concurrent conflict rules
remain authoritative. No deterministic live CAS-conflict injection is claimed.

Storage and public database integration race selections pass in 5.503 s and
2.583 s, covering reset count, partial failure, corrupt records, namespace
isolation, hidden nodes, pending-index publication and database worker state.
The GraphQL caller now propagates clear/reset/trigger errors with their causes
instead of returning success. All three failure stages reproduce against the old
resolver. The resolver package passes with race instrumentation (18.765 s);
affected HTTP admission/regeneration/live-worker controls pass (11.692 s).
The existing staged order is preserved; a failed reset after a successful clear
can leave vectors cleared and returns an error, not a completion claim.
Regeneration remains non-atomic; no all-or-nothing API promise is introduced. Full census and
generated artifacts remain CI-owned, with no clean result claimed yet.

Lexical discovery retires exactly the two reset methods from the scanner's
transitive population because they no longer route through UpdateNode's lexical
dependencies. All other 4,032 facts and dispositions are unchanged. The actual
native transaction mutation still owns constraints/publication; the lexical
manifest is not a substitute for that runtime/source proof. No extractor rule,
threshold, minimum or exemption is changed.
