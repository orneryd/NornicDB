# N152-128 caller embedding metadata ownership

Direct CreateNode, BulkCreateNodes and native transaction CreateNode+Commit pass
caller metadata to the canonical V2 encoder. Its shallow Node copy shares the
EmbedMeta map, and oversized encoding adds chunk_count to that shared map.

Approved correction: in encodeNodeInTxn, clone the metadata map only when adding
an absent chunk_count. Reuse maps.Clone, already used for top-level metadata edits
by snapshotNodeVectorMetadata. Preserve nil-map allocation, preexisting counts
and inline no-copy behavior. No nested metadata/vector ownership, timestamp or
wrapper redesign. All supported writes and V1→V2 migration converge here.

The adjacent V1 writer is called only by test fixture builders; actual migration
decodes V1 and writes through encodeNodeInTxn. Parent approved preserving that
legacy fixture encoder and its existing controls. No new clone helper is needed.

Exact ticket and five code/log blocks are archived under evidence/testdata with
SHA-256 manifest. Original public RED ran before production changes: three
reported routes fail, three controls pass, and all oversized stored count/chunk
and first/last vector-bit assertions pass. See evidence/current-red.log.txt.

The current measured-encoding oracle also compares caller metadata to the old
mutating encoder. Its correction requires preserving all encoded output/error
comparisons while asserting the new encoder leaves original caller metadata
unchanged. Parent approved that precise assertion update; nil preservation and every encoding oracle comparison remain intact.


## Parent decision on worker proposal

DeepSeek source-only job 76808f81325d4fe2a029aea573289426 completed in 295.76s.
Its owner-map.md and worker-proposal.md are preserved unchanged as historical
analysis, not the final implementation contract. Parent rejected deep ownedEntries
copying here: the encoder adds one top-level count and serializes nested values
read-only. Standard maps.Clone is sufficient; storage retention/delivery copies
serve a different lifetime and do not establish a conflicting ownership policy.
The worker's original oracle/V1 questions were independently reviewed and resolved
above. Parent accepted existing sidecar/stored-image guards and requested direct
UpdateNode plus native UpdateGraphNode+Commit controls using the same oversized
fixture. Those are appended without modifying the exact original test prefix.
