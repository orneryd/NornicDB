# Verification memory and interruption diagnostics

`scripts/verify.sh`, direct `scripts/generate-coverage.sh` runs and the trusted `scripts/ci/make-test-shard.sh` runner source the same `scripts/ci/go-memory-policy.sh` policy. When these run under GitHub Actions (`GITHUB_ACTIONS=true`), it supplies `GOMEMLIMIT=3GiB` unless the caller already provided a nonempty value. This standard Go setting is inherited by compiler, linker and test processes. Local verification and production server defaults are unchanged. An explicit setting, including `GOMEMLIMIT=off`, is preserved.

The limit is a soft Go-managed memory target, not an RSS or native-memory cap. More frequent garbage collection can increase compilation time. Every selected test, coverage profile, timeout, retry and failure assertion remains enabled.

## Measured qualification

On an Apple M2 Max with Go 1.27.3, compiling the same covered `pkg/cypher` test package produced these sampled process-tree RSS peaks:

| Go memory setting | Peak RSS | Compilation result |
| --- | ---: | --- |
| `off` | 6.24 GiB | Pass |
| `3GiB` | 4.15 GiB | Pass |

The experiment used `go test -c -cover` with equivalent compiler boolean spellings (`-N=false` and `-N=0`) to force both package compilations without deleting the shared Go cache. Sampling was every 0.5 seconds. Compilation elapsed times were 31.0 and 43.4 seconds, respectively; another test process was active, so these times are not isolated performance qualification. The measured RSS reduction was approximately 33.5%. Linux-hosted runner behavior requires separate CI evidence.

## Interpreting incomplete runs

Release coverage and conformance jobs have reported exit 143 and an explicit runner shutdown signal without a failed test assertion or Go timeout. That proves the jobs were interrupted; it does not establish an out-of-memory root cause or a passing test result. The memory policy reduces measured compilation pressure but is not proof that every runner interruption is repaired.

Coverage generation prints each package and attempt before starting, the effective Go memory target, then its elapsed time and exit status when it finishes. Preserve those logs and the exact head/base revisions. Runner diagnostics or host telemetry are still needed to attribute a runner shutdown. A canceled or interrupted run never substitutes for a completed verification pass.
