# NOR-71: cancel HTTP graph projection waits

The retained current reproduction is an authenticated neighborhood/path HTTP request waiting behind unrelated exclusive REVEAL work. The old physical reveal mutex ignored request cancellation. Use the existing contextRWMutex admission kernel at the physical reveal-scope owner, and the existing context-first projection entry in both graph collectors. A refused acquisition owns no lease; the collector returns cancellation before reading. Context-free callers retain their existing unbounded entry.

This fixes only the current reproduced cancellation/reveal scope. Historical label-rebuild/index-population criteria have separate prior passing evidence in Plane; this PR does not implement those paths or remove all context-free compatibility entries. Preserve healthy graph requests and normal/reveal isolation.
