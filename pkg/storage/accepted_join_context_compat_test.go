// Released accepted-join API compatibility for the exported accepted owner.
//
// AcceptedEntityCommitHandle.JoinContext can be used by Go transports outside this
// repository: such a transport holds the handle and joins a recorded or
// uncertain outcome with the caller's context, the outer receipt owner and the
// transport error. The interface below states that released three-argument
// method exactly, declared from outside the package the way an external
// transport declares the receiver it holds, so a missing, extra, reordered or
// variadic parameter is a compile error here instead of a silent break of the
// published Go API.
//
// The release-aware entrypoint is additive and separately named: it states the
// exact four-argument method this package's own transports use to end their
// accepted work enrolment inside the inline join.
//
// Author: NOR-288 lane (Claude), 2026-10-30.
package storage_test

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// releasedAcceptedJoin is the published accepted-join entrypoint of the
// released API: exactly three arguments and no release callback.
type releasedAcceptedJoin interface {
	JoinContext(context.Context, storage.CommitReceiptOwner, error)
}

// releasedAcceptedJoinWithRelease is the additive release-aware entrypoint: the
// same join plus the caller's own accepted work enrolment release.
type releasedAcceptedJoinWithRelease interface {
	JoinContextWithRelease(context.Context, storage.CommitReceiptOwner, error, func())
}

// The accepted owner must keep satisfying the released interface, and the
// release-aware entrypoint must keep exactly the shape this package's
// transports call.
var (
	_ releasedAcceptedJoin            = (*storage.AcceptedEntityCommitHandle)(nil)
	_ releasedAcceptedJoinWithRelease = (*storage.AcceptedEntityCommitHandle)(nil)
)
