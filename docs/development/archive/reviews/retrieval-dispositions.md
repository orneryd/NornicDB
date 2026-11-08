# NOR-406 duplicate advisory dispositions

Reviewed advisory [6676657090](https://github.com/orneryd/NornicDB/pull/1738#issuecomment-6676657090), candidate `b7f79b1935598c07b08629c553f725f10560c783`, release base `69115e8a2139b744ad2c7c797240b9d1f9dafcdb`. Its graph baseline was `35e3bddbf`, so every partner was inspected in current source. A successful advisory job is not an exhaustive clean result.

## Refactor targets and reuse candidates

| Candidate / partner | Current behavior and action |
| --- | --- |
| search probe `nor406StubEmbed` / Cypher `fixedQueryEmbedder.Embed` | Both return a fresh two-dimensional unit vector. Retire both bodies; compose `testfixture.FixedEmbedding2D`. The search callback is bound once at package scope, avoiding per-request method-value construction. |
| Same / nornicdb `tilingDocumentEmbedder.Embed` | Same vector ownership. Compose the same single-text capability; retain document chunking, dimensions, model and backend on the original fixture. |
| Same / nornicdb `localChunkEmbedder.Embed` | Same vector ownership. Compose the same capability without adding document-chunk support. Its separately required batch method composes `FixedBatchEmbedding2D`; the old forwarding implementation is removed. |
| Same / server `fixedSearchEmbedder.Embed` | Same vector ownership. Compose single-text and batch capabilities; preserve the existing model, backend and chunking contract. |
| search recorder `snapshot` / Heimdall `recordedQueries` | Both lock the producer mutex and return a detached slice. Remove both methods and call existing `testfixture.Snapshot` directly. |
| Same / search refresh recorder `recorded` | The indexed method has already been retired on the current release: refresh tests directly call `testfixture.Snapshot`. Reuse that current owner for the other two consumers. |
| controlled reranker `Enabled` / Bolt failure provider `Enabled` | Both return true with no state. Compose `testfixture.AlwaysEnabled` as the first field, preserving struct size. The fixtures are constructed non-nil; their different availability/error behavior remains on their respective providers. |
| search recorder `add` / server access recorder `IncrementAccess` | Both lock and append one item. Retire the private add method and call `testfixture.Append` directly. Keep `IncrementAccess` only as the method required by the production access-accumulator interface, calling that same append owner. The server's atomic drain remains lock-held: splitting it into snapshot and reset would lose concurrently recorded values. |

Fixed vector results remain independently owned on each call and each batch row. The shared embedding owner explicitly rejects canceled contexts; current callers supply request/worker contexts. Tests cover detached vectors, cancellation and deadline errors. No optional embedding capability is added to a fixture that did not already expose it.

## Existing owner proofs

Each following entry was checked against the integrated source rather than accepted from similarity alone:

| Advisory pair | Ownership |
| --- | --- |
| `runSearchRequest` / localization `SearchGraphContinuation` | Request dispatch calls the existing localized error owner. No copied localization body. |
| `runSearchRequest` / `readRetrievalContinuation` | Canonical continuation parser is called by request dispatch. |
| `runSearchRequest` / `applyRetrievalPolicyOptions` | Canonical policy application is called by request dispatch. |
| `scoreNodes` / `groupCandidatesByNode` | Exact scoring calls the grouping owner. |
| `groupCandidatesByNode` / `boundCompleteNodeMembers` | Grouping invokes the existing membership-completion owner. |
| `groupCandidatesByNode` / `scoreNodes` (extracted owner) | Grouping and scoring retain different stages; shared grouping is called directly, not reproduced. |
| `candidateNodeBoundIsComplete` / `groupCandidatesByNode` | The complete-single-member pruning condition is extracted once and reused by grouping and HNSW selection. |
| `candidateNodeBoundIsComplete` / `nodeMainVectorKey` | Membership proof calls the existing canonical main-vector-key classifier. |

## Verification limits

Integrated focused race checks passed for `internal/testfixture`, Bolt, Cypher, nornicdb, server, Heimdall and search using `-tags noui,nolocalllm`. The untagged attempt passed six packages but server linking failed because this worktree lacks the optional llama library; that attempt is not a full pass. Earlier checks on `b7f79b193` do not qualify later edits. The graph remains stale, and local graph retrieval currently fails HTTP-to-HTTPS routing; no graph mutation or checker-rule change was performed. Final checked revisions and command outcomes belong in the PR verification record.
