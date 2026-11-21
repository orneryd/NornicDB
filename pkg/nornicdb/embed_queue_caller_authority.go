package nornicdb

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Managed embedding work has exactly two provenance classes. Service work is
// caused by managed generation itself (writes, recovery scans, provider
// retries) and holds no provenance record: the managed-generation gate decides
// it. Caller work is caused by an authorized administration operation (manual
// retry of parked failures) and holds a record that stays with the attempt until
// that attempt ends.
//
// Provenance matters because a parked failure is not otherwise eligible for
// submission: the caller operation is the cause of the provider request. The
// worker validates each caller batch before provider submission and each
// result before persistence.
var (
	// ErrEmbeddingSubmissionRefused reports caller-caused input that the
	// authorizing operation no longer admits. The batch is refused before any
	// text is sent to the provider.
	ErrEmbeddingSubmissionRefused = errors.New("embedding submission refused: the authorizing operation no longer admits this input")
	// ErrEmbeddingAuthorityRequired reports a caller-caused requeue attempted
	// without the authority that must revalidate it.
	ErrEmbeddingAuthorityRequired = errors.New("embedding caller authority is required")
)

// EmbeddingJobAuthority revalidates the caller operation that caused a
// caller-originated embedding attempt before the provider request and
// before each result is persisted.
//
// Implementations are read-only: they decide, they never mutate the queue.
// Returning an error refuses the whole batch before submission.
type EmbeddingJobAuthority interface {
	RevalidateEmbeddingJob(ctx context.Context, nodes []*storage.Node) error
}

type embeddingJobInputsKey struct{}

// EmbeddingJobInputs identifies the source fields considered before one
// caller's embedding provider submission.
type EmbeddingJobInputs struct {
	Text          storage.EmbeddingTextPolicy
	AllProperties bool
}

// EmbeddingJobInputsFromContext provides the authoritative worker input policy.
func EmbeddingJobInputsFromContext(ctx context.Context) (EmbeddingJobInputs, bool) {
	input, ok := ctx.Value(embeddingJobInputsKey{}).(EmbeddingJobInputs)
	return input, ok
}

func (ew *EmbedWorker) embeddingAuthorityContext(allProperties bool) context.Context {
	return context.WithValue(ew.ctx, embeddingJobInputsKey{}, EmbeddingJobInputs{
		Text: storage.EmbeddingTextPolicy{
			Include: ew.config.PropertiesInclude, Exclude: ew.config.PropertiesExclude,
			IncludeLabels: ew.config.IncludeLabels,
		},
		AllProperties: allProperties,
	})
}

func interruptedCallerAttempt(node *storage.Node) bool {
	return node.EmbedMeta[storage.EmbeddingCallerPendingKey] == true
}

// callerEmbeddingJob is one caller operation's provenance record. Every node
// requeued by a single operation shares the record, so the worker revalidates
// one authority once per batch instead of per node. Its presence is the
// per-node answer to "was this attempt caused by a caller?".
type callerEmbeddingJob struct {
	authority EmbeddingJobAuthority
}

type embeddingCallerKey struct{}
type embeddingCallerRequest struct{ authority EmbeddingJobAuthority }

// WithEmbeddingJobAuthority binds a manual retry to the caller whose
// authorization the worker must check before processing its queued work.
func WithEmbeddingJobAuthority(ctx context.Context, authority EmbeddingJobAuthority) context.Context {
	return context.WithValue(ctx, embeddingCallerKey{}, embeddingCallerRequest{authority: authority})
}

// recordCallerAuthority binds one requeued node to the caller operation that
// caused it. The record is dropped when its work reaches a terminal state.
// Callers hold claimMu.
func (ew *EmbedWorker) recordCallerAuthorityLocked(id string, job *callerEmbeddingJob) {
	if ew.callerJobs == nil {
		ew.callerJobs = make(map[string]*callerEmbeddingJob)
	}
	if _, recorded := ew.callerJobs[id]; !recorded {
		ew.callerJobCount.Add(1)
	}
	ew.callerJobs[id] = job
}

// dropCallerAuthorityLocked is the single writer that removes provenance.
// Callers hold claimMu.
func (ew *EmbedWorker) dropCallerAuthorityLocked(id string) {
	if _, recorded := ew.callerJobs[id]; !recorded {
		return
	}
	delete(ew.callerJobs, id)
	ew.callerJobCount.Add(-1)
}

// clearCallerAuthoritiesLocked retires every provenance record. Callers hold
// claimMu. The durable marker keeps unresolved work from returning under
// managed service authority.
func (ew *EmbedWorker) clearCallerAuthoritiesLocked() {
	if len(ew.callerJobs) == 0 {
		return
	}
	ew.callerJobs = nil
	ew.callerJobCount.Store(0)
}

// admitSubmission is the single owner of the caller-authority revalidation
// performed at the provider submission boundary. Service-originated nodes pass
// through unchanged. When the batch carries caller provenance, each caller
// operation represented in it revalidates its own nodes once; a refusal records
// the refusal on the existing terminal-failure owner and releases the claim
// without sending any text. Nodes from other operations and from managed work
// are unaffected.
func (ew *EmbedWorker) admitSubmission(nodes []*storage.Node, allProperties bool) ([]*storage.Node, bool) {
	if len(nodes) == 0 {
		return nodes, false
	}
	if ew.callerJobCount.Load() == 0 {
		anyCaller := false
		for _, node := range nodes {
			anyCaller = anyCaller || interruptedCallerAttempt(node)
		}
		if !anyCaller {
			return nodes, false
		}
	}
	groups := make(map[*callerEmbeddingJob][]*storage.Node)
	admitted := make([]*storage.Node, 0, len(nodes))
	var interrupted []*storage.Node
	ew.claimMu.Lock()
	for _, node := range nodes {
		job := ew.callerJobs[string(node.ID)]
		if job == nil {
			if interruptedCallerAttempt(node) {
				interrupted = append(interrupted, node)
			} else {
				admitted = append(admitted, node)
			}
			continue
		}
		groups[job] = append(groups[job], node)
	}
	ew.claimMu.Unlock()
	refused := len(interrupted) > 0
	for _, node := range interrupted {
		ew.refuseSubmission(node, ErrEmbeddingSubmissionRefused)
	}
	ctx := ew.embeddingAuthorityContext(allProperties)
	for job, callerNodes := range groups {
		if err := job.authority.RevalidateEmbeddingJob(ctx, callerNodes); err != nil {
			refused = true
			for _, node := range callerNodes {
				ew.refuseSubmission(node, err)
			}
			continue
		}
		admitted = append(admitted, callerNodes...)
	}
	return admitted, refused
}

func (ew *EmbedWorker) revalidateEmbeddingResult(node *storage.Node, allProperties bool) error {
	if !interruptedCallerAttempt(node) && ew.callerJobCount.Load() == 0 {
		return nil
	}
	ew.claimMu.Lock()
	job := ew.callerJobs[string(node.ID)]
	ew.claimMu.Unlock()
	if job == nil {
		if interruptedCallerAttempt(node) {
			return ErrEmbeddingSubmissionRefused
		}
		return nil
	}
	return job.authority.RevalidateEmbeddingJob(ew.embeddingAuthorityContext(allProperties), []*storage.Node{node})
}

// refuseSubmission records a refused provider submission on the terminal
// failure owner and releases the claim. It never calls the provider.
func (ew *EmbedWorker) refuseSubmission(node *storage.Node, cause error) {
	ew.recordEmbeddingRefusal(node, cause)
	ew.releaseNodeClaim(node.ID)
}

func (ew *EmbedWorker) recordEmbeddingRefusal(node *storage.Node, cause error) {
	if cause == nil {
		cause = ErrEmbeddingSubmissionRefused
	}
	if !errors.Is(cause, ErrEmbeddingSubmissionRefused) {
		cause = errors.Join(ErrEmbeddingSubmissionRefused, cause)
	}
	ew.markNodeEmbeddingFailed(ew.ctx, node.ID, cause, node)
}

// RegenerateDatabaseEmbeddingsForCaller queues existing managed vectors for
// replacement within one concrete database under the caller's authority.
func (db *DB) RegenerateDatabaseEmbeddingsForCaller(ctx context.Context, database string, authority EmbeddingJobAuthority) (int, error) {
	db.mu.RLock()
	worker := db.embedQueue
	db.mu.RUnlock()
	if worker == nil {
		return 0, localizedError(localization.NornicDBCoreAutoEmbedNotEnabled(), nil)
	}
	if authority == nil || database == "" {
		return 0, ErrEmbeddingAuthorityRequired
	}
	return worker.regenerateEmbeddingsForCaller(ctx, database, authority)
}

func (ew *EmbedWorker) regenerateEmbeddingsForCaller(ctx context.Context, database string, authority EmbeddingJobAuthority) (int, error) {
	var targets []storage.NodeID
	if err := ew.storage.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{Prefix: database + ":", WithEmbeddings: true}, func(node *storage.Node) error {
		if len(node.ChunkEmbeddings) != 0 && len(node.ChunkEmbeddings[0]) != 0 {
			targets = append(targets, node.ID)
		}
		return ctx.Err()
	}); err != nil {
		return 0, err
	}

	job := &callerEmbeddingJob{authority: authority}
	queued := 0
	for _, id := range targets {
		if err := ctx.Err(); err != nil {
			return queued, err
		}
		node, err := ew.embeddingSnapshot(id)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return queued, err
		}
		if !ew.admitsEmbedding(id) {
			continue
		}
		provider, err := ew.selectEmbeddingProvider().resolve(id)
		if err != nil {
			return queued, ew.redactFailure(id, err)
		}
		if provider == nil {
			continue
		}
		properties, consumesProperties := provider.(embed.DocumentPropertyChunkEmbedder)
		if err := authority.RevalidateEmbeddingJob(ew.embeddingAuthorityContext(consumesProperties && properties.UsesDocumentProperties()), []*storage.Node{node}); err != nil {
			continue
		}
		if err := ew.resetCallerEmbedding(node, job); errors.Is(err, storage.ErrEmbeddingSourceChanged) {
			continue
		} else if err != nil {
			return queued, err
		}
		queued++
		ew.signalTrigger()
	}
	return queued, nil
}

func (ew *EmbedWorker) resetCallerEmbedding(node *storage.Node, job *callerEmbeddingJob) error {
	embeddingutil.InvalidateManagedEmbeddings(node)
	if job != nil {
		node.EmbedMeta = map[string]any{storage.EmbeddingCallerPendingKey: true}
	}
	if err := ew.commitNodeReset(node, job); err != nil {
		return err
	}
	ew.addNodeToPendingEmbeddings(node.ID)
	return nil
}

// EmbeddingDatabaseOfNodeID resolves the managed database that owns a stored
// node identifier. A qualified identifier carries its database; anything else
// belongs to the default database, matching managed-generation admission.
func (db *DB) EmbeddingDatabaseOfNodeID(id storage.NodeID) (string, bool) {
	database, local, qualified := splitQualifiedID(string(id))
	if !qualified {
		return db.defaultDatabaseName(), true
	}
	if local == "" {
		return "", false
	}
	return database, true
}
