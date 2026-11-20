# Preserve an admitted namespace listing during Close

NOR-77 reproduces a public ListNamespaces call combining the old node counters with released edge counters while Close retires storage. The existing namespace counter lock protects map contents but does not retain their owner through retirement.

Use the existing storage work lifetime, as Size, Sync and EstimateSize already do, to retain state through the complete public read. Preserve empty/nil post-close behavior. No new lock, alternate namespace source, API error, or shutdown policy.

Validation: preserve exact failing public-read fixture; check complete listing, edge-only namespace, real stored-node/edge controls, post-close/repeated-close and affected owner retirement tests. Focused package checks only; no full suite/race/stress. Baseline and corrected source/head/logs must be explicit.
