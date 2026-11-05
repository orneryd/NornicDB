The store.zip fixture contains a closed Badger graph and derived BM25, vector
and HNSW indexes written by unmodified v1.4.3 production code (f2ddd27cf).
The two nodes x and x-chunk-1 contain four authoritative embeddings; the
legacy search cache loses one because the entry keys collide.

Generation: copy writer_test.go into pkg/search/ of a v1.4.3 checkout, commit,
then run TestWriteRelease143VectorFixture on TrueNAS through sig/tnrun.sh.
Decode the VECTOR143_FIXTURE base64 line to store.zip. No source or file
version was changed. Original receipt: i20261019121120-3224-ffd0, writer
commit 8a9209dae, Go 1.26.9.

TestVectorEntryUpgradeFromRelease143Store opens the actual graph and caches,
recovers all four embeddings, writes another node, reopens, and verifies that
deleting x leaves x-chunk-1 searchable.
