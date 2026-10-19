# Vector index upgrade and rollback

The vector entry identity fix prevents one node's chunk, named or property
vector from overwriting another node's vectors. It changes derived search
indexes; the authoritative graph storage format is unchanged.

These rollback steps apply to the release/v1.4.4 storage layout. Develop or
canary builds can include independent storage migrations; this search fix
does not establish downgrade safety for those builds.

Before upgrading, stop the database cleanly and take the normal backup of its
data directory. Start the fixed version with the same data directory. Search
indexes written by 1.4.3 or an earlier 1.4.4 build are incompatible and are
rebuilt from the graph's stored embeddings. Allow time and disk space for this
rebuild. A cache entry already overwritten by a collision is recovered from
the graph; embeddings absent from authoritative graph storage cannot be
recovered by rebuilding indexes.

Derived format changes are `.vec`/`.meta` version 2 to 3, HNSW 1.2.0 to 1.3.0,
IVF-PQ bundle 2 to 3 and vector snapshot 1.0.0 to 1.1.0. Build fingerprints
also include `vkey2`. No embedding storage key prefix or storage version is
changed. This fix does not adopt the storage layout proposed in #1285.

To roll back to 1.4.3 or a build preceding this fix:

1. Stop the fixed database cleanly.
2. Move the derived `<dataDir>/search` directory outside the data directory,
   or remove it after taking a backup. For custom search persistence paths,
   move their derived caches too: BM25, vectors, HNSW, IVF/IVF-PQ and build
   settings. Keep the authoritative graph files and WAL untouched.
3. Start the older binary with the same data directory and let it rebuild
   its own search caches. Writes made after upgrading remain in graph storage.

Do not copy the new search cache files into an older binary's cache directory.
The older binary retains its original collision bug; rollback restores its
previous behavior. Re-upgrading rebuilds collision-free indexes from the same
authoritative embeddings. A database with search persistence disabled simply
rebuilds its in-memory indexes at startup.

The regression fixture under `pkg/search/testdata/vector-v143` was written by
1.4.3 itself. Upgrade verification opens that graph and its caches, recovers
the overwritten entry, writes another node and reopens. The corresponding
rollback probe runs the 1.4.3 reader after removing only the derived caches.
