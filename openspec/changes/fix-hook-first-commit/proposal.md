# Mandatory duplicate checks use the repository's existing graph

Keep the configured global hooks active. Git processes load endpoint and
credentials from ENV and use the live database. Ingestion and duplicate search
must resolve the same repository identity and database, including linked
worktrees without an origin remote. Worktree folder names must never create
separate ingestion databases. Preserve NornicDB's `nornicdbcode` override.

The commit check creates no databases and accepts no missing/empty-baseline
substitute. Missing ingestion, failed authentication, search unavailability,
incomplete continuation and duplicate findings block the commit with an explicit
mandatory warning: do not override, bypass or disable the check; fix the cause
before proceeding to prevent duplicate and divergent code from being checked in.

Handle an unborn HEAD without crashing, while still requiring its established
graph. Prove the installed public hook through a real staged divergent-helper
commit attempt against the existing NornicDB graph, and preserve HEAD, staged
content and unrelated work when it is rejected. Keep original failures as evidence.
