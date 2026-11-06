# Accepted error Go contract decision

## Established failure

On release69115e8a2, the public delayed-DROP test fails: an accepted catalog operation reports an acknowledgement conflict; generic transient classification follows AcceptedCommitError.Unwrap to ErrConflict; a delayed client retry deletes the replacement database. The original PR's barrier correction makes accepted outcomes non-retryable, retaining their cause only for explicit typed diagnostics.

## Current and proposed Go API

Current AcceptedCommitError exports State, LogicalID and Err, plus Error and Unwrap. Its documentation promises errors.Is can discover the original timeout/transport cause. Retiring Unwrap changes that public diagnostic contract, so this decision is requested explicitly.

Proposed: retain the existing typed fields and meaningful accepted-outcome Error; retire Unwrap entirely and do not add the original PR's redundant Cause getter. A caller first identifies *AcceptedCommitError with errors.As, then inspects accepted.Err for diagnostics. Generic errors.Is and public retry classification stop at accepted ownership; pre-acceptance conflicts retain ordinary transient classification. Explicit calls to the removed Unwrap method require migration/rebuild. No HTTP/Bolt syntax, storage layout, keys, payload format or version is changed by this error API decision.

The alternative is retaining Unwrap only as a nil-returning compatibility method. That keeps explicit method calls compiling but changes its documented semantics and leaves a legacy entry point without useful behavior. The maintainer has asked for canonical callers without compatibility leftovers.

## Validation required

- Accepted native conflict settles once, releases locks and leaves later public writes available.
- Accepted DROP acknowledgement error cannot lead to automatic retry of a replacement generation.
- Standard and joined accepted error chains remain nonretryable across public status renderers; typed Err diagnostics preserve cause.
- Genuine pre-acceptance conflicts, cancellation and uniqueness failures preserve their own classifications.
- All diagnostic callers/tests/docs migrate to the one typed field; full affected checks and delivered-head CI gate pass.

This is a contract proposal, not a tested new-head fix. The candidate remains mid-rebase; original barriers are unqualified until integrated.
