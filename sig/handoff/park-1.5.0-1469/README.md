# Frozen provisional source

Parked for #1469 / 1.5.0 on 2026-10-18. This is unfinished work, not a runnable or reviewed correction. Do not move the PR head or use it for an RC.

The WIP branch starts from develop PR local test commit 662402128c165cb9a40a887035b6b9d84e492857 (remote PR head fe3650049ad4c488b4001337915cdddc48b08cf6). Production work had begun in the former release worktree before the user's develop-only decision. For preservation, 18 tracked-file patches apply unchanged to the develop WIP and 8 new files are copied unchanged. No implementation is continued during the park. Two files have develop differences and remain archive-only: pkg/nornicdb/db_admin.go and pkg/nornicdb/embed_queue.go. They must be integrated preserving develop's pause/readiness/embedding-controller features when work resumes.

provisional-tracked.patch is the complete original tracked-file diff against 6feaa9835eb937c15d3e5718f30283c38d8f1406. provisional-source/ contains exact copies of every added/modified original file, with a .txt suffix; provisional-source-manifest.json records the deleted db_embedder_owner.go and hashes. transplant-manifest.json records what was applied and why the two differing files were retained only in the archive. These are preservation decisions, not resolved code conflicts or an implementation claim. The original dirty worktree and all isolated probe worktrees remain intact.

stashed-drafts/ also exports the pre-gate release and develop leaf drafts as binary-safe patches and untracked tar archives, plus rc144-r5-production.patch. They are superseded approaches retained to avoid losing work. Do not reapply them as the approved implementation.

The active package includes a temporary TrueNAS go/format exporter, not a regression test. Remove it before presenting the integrated implementation. Its two successful package-only compile/format runs do not establish that any consumer integration compiles or works.
