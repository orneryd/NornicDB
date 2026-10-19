package replication

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type xhighPublicationGate struct {
	*StorageAdapter
	prepared, resumePreparation chan struct{}
	applying, resumeApply chan struct{}
}

func (s *xhighPublicationGate) PrepareCommandContext(ctx context.Context, cmd *Command) error {
	if err := s.StorageAdapter.PrepareCommandContext(ctx, cmd); err != nil { return err }
	close(s.prepared)
	<-s.resumePreparation
	return nil
}

func (s *xhighPublicationGate) ApplyCommandContext(ctx context.Context, cmd *Command) error {
	if cmd.Type == CmdCreateNode {
		close(s.applying)
		<-s.resumeApply
	}
	return s.StorageAdapter.ApplyCommandContext(ctx, cmd)
}

func TestXHighRaftPeerPublicationLockOrder(t *testing.T) {
	if os.Getenv("NORNIC_XHIGH_LOCK_CHILD") != "1" {
		command := exec.Command(os.Args[0], "-test.run=^TestXHighRaftPeerPublicationLockOrder$", "-test.v")
		command.Env = append(os.Environ(), "NORNIC_XHIGH_LOCK_CHILD=1")
		output, err := command.CombinedOutput()
		if err != nil { t.Fatalf("Raft publication/apply must progress for different databases: %v\n%s", err, output) }
		return
	}
	base := storage.NewMemoryEngine()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	target := &xhighPublicationGate{adapter, make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})}
	cfg := DefaultConfig()
	cfg.Mode, cfg.NodeID = ModeRaft, "xhigh-lock"
	cfg.Raft.Peers = []PeerConfig{{ID: "peer", Addr: "127.0.0.1:17001"}}
	cfg.Raft.CommitTimeout = 5*time.Second
	raft, err := NewRaftReplicator(cfg, target)
	require.NoError(t, err)
	raft.state, raft.currentTerm = StateLeader, 1
	data, err := encodeNodePayload(&storage.Node{ID: "peer_b:prior"})
	require.NoError(t, err)
	raft.log = append(raft.log, &RaftLogEntry{Index: 1, Term: 1, Command: &Command{Type: CmdCreateNode, Data: data}})
	raft.matchIndex["peer"] = 1
	dropData, err := encodeGob(namespaceDropCommand{Prefix: "target_a:"})
	require.NoError(t, err)
	future := &applyFuture{caller: context.Background(), cmd: &Command{Type: CmdDeleteByPrefix, Data: dropData, namespaceDrop: storage.NewNamespaceDrop(nil)}, errCh: make(chan error, 1), doneCh: make(chan struct{})}
	proposalDone := make(chan struct{})
	go func() { raft.processApply(context.Background(), future); close(proposalDone) }()
	select { case <-target.prepared: case <-time.After(time.Second): t.Fatal("DROP preparation not reached") }
	applyDone := make(chan struct{})
	go func() { raft.advanceCommitIndex(); close(applyDone) }()
	select { case <-target.applying: case <-time.After(time.Second): t.Fatal("prior B apply not reached") }
	close(target.resumePreparation)
	deadline := time.Now().Add(time.Second)
	for {
		select {
		case <-proposalDone:
			// A correct owner can append without waiting for the paused
			// physical apply. Do not require a blocked append stack.
			close(target.resumeApply)
			select { case <-applyDone: os.Exit(0); case <-time.After(time.Second): t.Fatal("peer apply did not progress") }
		default:
		}
		stack := make([]byte, 1<<20)
		n := runtime.Stack(stack, true)
		if strings.Contains(string(stack[:n]), "(*RaftReplicator).processApply.func1") { break }
		if time.Now().After(deadline) { t.Fatal("proposal did not reach log append") }
		time.Sleep(time.Millisecond)
	}
	close(target.resumeApply)
	select {
	case <-applyDone:
		select { case <-proposalDone: os.Exit(0); case <-time.After(time.Second): }
	case <-time.After(300*time.Millisecond):
	}
	stack := make([]byte, 1<<20)
	n := runtime.Stack(stack, true)
	fmt.Printf("DEADLOCK: proposal holds physical publication barrier waiting for logMu; earlier B apply holds logMu waiting for physical publication barrier\n%s", stack[:n])
	os.Exit(31)
}
