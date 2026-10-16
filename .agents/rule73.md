# Rule 73 — change the causal approach

## Trigger

Group related failures by the shared contract or underlying cause. The affected
family is the set of implementations and consumers that must satisfy that contract
or causal invariant. Identify its shared owner and affected operations, representations
and execution paths. Keep the boundary within the user's requested outcome.

A failed correction attempt is a correction presented as ready that still fails its
required review or verification. Ordinary implementation and test failures before a
correction is presented as ready are feedback, not failed correction attempts.

- Two failed correction attempts in the same family without an intervening pass
  trigger Rule 73.
- A pass ends the active failed-attempt sequence and preserves the history.
- If materially the same failed treatment later recurs, use that history and enter
  Rule 73 instead of treating it as new work.
- Work on another family neither consumes nor resets this family's sequence.

Enter Rule 73 sooner when materially the same failed treatment is being repeated
without new evidence, changed conditions, corrected implementation, or another
concrete reason it should now succeed.

## Required course change

Rule 73 ends ordinary per-finding and per-consumer correction for that family.
Re-establish its complete current boundary and accepted corrections, identify the
shared causal owner, architecture, mechanism or method that let the failures survive,
and correct that whole causal unit through every affected consumer as one integrated
implementation. Preserve genuine protocol boundaries without duplicating the same
behavior across them.

The next correction is ready for review only when the changed causal approach is
implemented across the complete affected unit and its verification passes. Exercise
the required product outcome through the affected public paths and consumers, including
the conditions behind the recurring failure. Intermediate implementation and test
failures remain feedback within this work; they do not call for another review of an
unfinished correction.

If the corrected unit fails its required review or verification, Rule 73 remains
active. Reassess and materially change the failed causal approach before presenting
another integrated correction. Do not resume serial leaf patches.

Rule 73 prescribes no particular architecture or technical recipe. Follow the evidence,
stay within the affected scope, and finish when the integrated correction passes its
required review and verification.
