package storage

// EntityCommitMode retains the current producer's durability and ACK policy.
// It belongs to the transaction/command, not to individual entities.
type EntityCommitMode uint8

const (
	CommitModeExplicit EntityCommitMode = iota // explicit durability, no buffered ACK
	CommitModeImplicit                         // existing autocommit/bulk policy, buffered ACK eligible
	CommitModeDirect                           // native direct policy, no extra explicit Sync or buffered ACK
)

func (mode EntityCommitMode) valid() bool { return mode <= CommitModeDirect }
