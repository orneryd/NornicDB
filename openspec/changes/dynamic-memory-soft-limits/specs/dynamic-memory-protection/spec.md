# Spec Delta

## Purpose

Keep the database available under memory pressure through cooperative, dynamically
sized application and transaction budgets without misrepresenting storage outcomes.

## ADDED Requirements

### Requirement: Explicit soft-limit contract
The system SHALL document application and transaction thresholds as cooperative
soft limits, distinguish them from Go GC targets and container hard limits, and
explain that sampling gaps, native calls and single allocations can overshoot.

#### Scenario: One allocation crosses the threshold between checks
- **WHEN** an allocation exceeds a configured threshold before the next safe checkpoint
- **THEN** the engine reports the limitation honestly and terminates eligible work at the next safe checkpoint rather than claiming an allocator-enforced ceiling or guaranteed OOM immunity.

### Requirement: Dynamic sizing without attributing queries to baseline
The system SHALL initialize and reconcile baseline and reserve estimates using
actual resource ownership and observations, without counting active transaction
memory as persistent baseline or charging overlapping measurements twice.

#### Scenario: Model loading changes the baseline
- **WHEN** a model is loaded or unloaded while transactions exist
- **THEN** the shared query budget adjusts within the application/container constraint, active query estimates remain separately owned, and unavailable headroom blocks new expensive admissions.

#### Scenario: Budget shrinks below outstanding usage
- **WHEN** updated resource usage or container constraints reduce available capacity below reservations
- **THEN** existing usage remains accounted, admission stops and the approved safe cancellation policy applies without silently erasing debt or inflating baseline with active queries.

### Requirement: Shared admission and execution policy
The system SHALL enforce caller cancellation/deadlines, approximate transaction
budgets and application pressure through one shared policy owner and existing
execution checkpoints, supplemented by pre-growth reservations where estimable.

#### Scenario: Single oversized query with no later requests
- **WHEN** one admitted query exceeds a transaction budget or selected application-pressure policy and no other requests arrive
- **THEN** its eligible execution terminates with the typed reason at a safe checkpoint and releases owned resources; enforcement does not depend on another admission request.

#### Scenario: Concurrent reservations
- **WHEN** concurrent transactions request capacity from the same finite aggregate budget
- **THEN** they cannot independently spend the same available budget and physical process growth is not fully charged to each transaction.

#### Scenario: Cancellation competes with memory pressure
- **WHEN** caller cancellation, deadline expiration and memory pressure overlap
- **THEN** the established first-failure semantics preserve the recorded cause, Done notification and transport classification without relabeling an unknown error as OOM.

### Requirement: Compatible failures and service recovery
The system SHALL reject application-pressure admissions with HTTP 503 and expose
appropriate typed errors through non-HTTP transports. Individual transaction
exhaustion SHALL use Neo.ClientError.General.TransactionOutOfMemoryError;
aggregate pool exhaustion SHALL remain distinct. Transaction-limit settings
SHALL default to zero/unlimited on the release port.

#### Scenario: Transaction exceeds its configured estimate
- **WHEN** a pre-commit transaction exceeds its memory threshold
- **THEN** Bolt/HTTP expose the verified transaction-memory error, the transaction follows existing failure/rollback/reset semantics, no pending writes become committed and a fresh session verifies the outcome.

#### Scenario: Admission rejected but cleanup must run
- **WHEN** application pressure prevents new expensive requests
- **THEN** cancellation, rollback, essential health and already admitted completion can still use reserved capacity, and expensive admissions resume only under the approved recovery policy.

### Requirement: Storage completion remains truthful under pressure
The system SHALL account for Badger/WAL/background and kernel writeback pressure
in observation/headroom, preserve configured durability and make cancellation
respect each storage route's irreversible acceptance/commit boundary.

#### Scenario: Pressure before irreversible commit
- **WHEN** a memory failure occurs before the inventoried irreversible boundary
- **THEN** the transaction can abort without committing its writes and rollback/flush cleanup has capacity to finish.

#### Scenario: Pressure during post-commit fsync
- **WHEN** memory pressure rises after irreversible commit while storage synchronization is pending
- **THEN** the engine completes/reports the actual committed and durability outcome without fabricating an OOM rollback or claiming committed writes were undone.

#### Scenario: Slow disk retains dirty pages
- **WHEN** slow flush/fsync leaves write buffers or dirty/writeback pages resident
- **THEN** pressure remains observable, new expensive work is limited and essential storage progress remains possible; fsync completion alone does not reset memory accounting.

### Requirement: Variable feature costs are estimates with observable coverage
The system SHALL distinguish observed total usage from component estimates and
SHALL cover transaction work, indexes/builds/inserts, embedding workers, LLM
inference, reranking and storage working sets without assuming fixed costs or
attributing shared process deltas to individual operations.

#### Scenario: Indexing and AI work grow while commits are rejected
- **WHEN** index construction or insertion, embedding, reranking or inference consumes memory while new HTTP commits are rejected
- **THEN** the same application policy governs new feature work and eligible active work; transaction rejection alone is not claimed to control total growth.

#### Scenario: Fsync overlaps other allocations
- **WHEN** fsync runs concurrently with writes, inference or GC
- **THEN** telemetry records scoped usage, dirty/writeback observations and timing without claiming that their delta is the exact fsync allocation or that historical peaks guarantee future capacity.

#### Scenario: HTTP commit endpoint encounters pressure
- **WHEN** a `/db/{name}/tx/commit` request is rejected before execution because of application pressure
- **THEN** it receives a structured HTTP 503 memory-pressure response promptly rather than an unbounded wait, and no transaction writes are executed.

#### Scenario: Explicit HTTP commit crosses the storage boundary
- **WHEN** `/db/{name}/tx/{id}/commit` has irreversibly accepted its writes before a pressure check
- **THEN** it follows the actual storage/transaction completion outcome instead of reporting a fictitious rollback or applying a blanket admission error after commit.

### Requirement: Budget ownership spans input through final consumption
The system SHALL apply application admission before expensive request decoding and
parser work, preserve transaction accounting across statements, and transfer result
and buffered-write charges without a gap until their actual owner releases them.

#### Scenario: Statement fails while its explicit transaction retains writes
- **WHEN** memory pressure fails a statement whose session still owns staged writes
- **THEN** cleanup is serialized with active execution and terminal operations, retained memory stays charged until released, and subsequent transaction control preserves the original failure rather than committing those writes.

#### Scenario: Slow client retains output
- **WHEN** execution finishes but result rows remain queued or partially consumed
- **THEN** result ownership retains its charge until consumption/discard/disconnect cleanup; execution completion alone does not release it.

### Requirement: Accepted write completion has protected finite capacity
The system SHALL obtain bounded completion capacity before entering storage acceptance
and SHALL preserve replayable WAL and large-commit publication/rollback contracts.

#### Scenario: Pressure after replayable WAL append
- **WHEN** a buffered commit has successfully appended its replayable WAL record but has not yet published its buffer entry
- **THEN** the accepted write completes its publication and ownership transfer without a new memory cancellation falsely reporting rollback.

#### Scenario: Completion pool is exhausted
- **WHEN** a still-abortable transaction cannot reserve completion capacity
- **THEN** it fails before storage acceptance with the appropriate pool error, without holding locks needed by flush or rollback to release capacity.

### Requirement: Async write buffers participate in application limits
The system SHALL reserve async write-buffer capacity before irreversible acceptance,
account for all live generations and their auxiliary allocations, and keep accepted
writes eligible for flush/replay under pressure. Rotation SHALL NOT reset byte usage.

#### Scenario: Producers exceed the stalled flusher's capacity
- **WHEN** incoming writes exhaust the buffer byte allowance or shared application capacity while disk progress is stalled
- **THEN** new abortable writes fail with the appropriate resource error before replayable WAL acceptance, existing accepted writes remain accounted and flush can use protected completion capacity.

#### Scenario: A generation rotates or replay fails
- **WHEN** an active buffer rotates into draining, retains readers or encounters a replay error
- **THEN** its retained allocations remain charged until actual release; neither rotation nor retry admits unbounded replacement work or discards acknowledged writes.
