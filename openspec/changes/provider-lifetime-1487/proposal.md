# Own embedding providers through initialization, use and shutdown (#1487)

The maintainer explicitly requests bringing the broader lifecycle correction into
#1487 rather than keeping the test-only wait or deferring the race to 1.5.0.
Use the existing DB resource owner from #1465/#1469, adapted to the current
release. DB admission must own construction, health checks, installation,
foreground snapshots and background provider calls until those calls actually
return. Server.Stop cancels and joins its initializer; DB.Close cancels and joins
all admitted work before releasing providers and storage. No detached provider
call may outlive its resource. Preserve synchronized log snapshots, restore
production-like DB embedding configuration and remove the queue-publication wait.

Preserve prior failed evidence. Reuse the existing lifecycle implementation and
tests, correct its unresolved auto-embedding detached-call review finding, and
verify the integrated source across both parsers and native/non-native builds.
Do not change the installed mandatory duplicate hooks.

See [Implementation and verification notes](design.md) for the ownership
contracts, constructor migration and recorded verification limits.
