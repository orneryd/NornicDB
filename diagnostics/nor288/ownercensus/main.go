// Author: NOR-288 lane (Claude), 2026-10-30.
// Behaviour-defined owner census over the compiled module (production code,
// whole module ./...). Membership is resolved by type and value, never by a
// callee's name: the module is type-checked, built into SSA, and a VTA call
// graph (refining CHA) resolves static calls, interface calls, method values
// and calls through function values or parameters to every function they may
// reach. Four contracts:
//
//	A. DROP-state decision, outside pkg/multidb (the owner). A deleting
//	   function is the owner's DropDatabase, DropCompositeDatabase or
//	   FinishPendingDrop, or any function (adapter, method value, bound or
//	   interface wrapper, closure) that calls a deleting function with a
//	   name taken from its own inputs: a string parameter or captured
//	   variable, or a value loaded from one through fields, pointers or
//	   conversions (a wrapper that reads the name from its receiver). A name
//	   produced by a call (parsing a statement) does not bind the caller. A deleting call is a
//	   call site whose resolved callees include a deleting function, whatever
//	   the called value is named. A probe is a call that may reach the owner's
//	   availability checks (Exists, ExistsOrIsConstituent, GetStorage,
//	   GetStorageWithAuth, GetDatabase, ResolveDatabase, IsCompositeDatabase)
//	   or a function whose every call is such a probe (a thin wrapper). A
//	   decider is an if whose condition tests a probe result and whose two
//	   paths differ in reaching a deleting call, where the probe and a
//	   deleting call take the same name argument. Expected: 0.
//	B. Accepted-outcome barrier construction: a composite literal whose type
//	   is storage.AcceptedCommitError (type identity). Expected: only
//	   storage.AcceptedOutcome.
//	C. Accepted-application endpoint decision: a function that uses the field
//	   BadgerTransaction.acceptedApplication and calls the method
//	   revalidateEndpointsLocked (object identity). Expected: only that method.
//	D. Lifetime or background-work primitive on a line added by
//	   git diff <base>...HEAD (base = the layer this PR stacks on): a go
//	   statement, a sync.WaitGroup variable or field, an errgroup call, or a
//	   context.With* lifetime call (object identity). The one owner is
//	   lifecycle.WorkLifetime: a launch is allowed only where the enclosing
//	   function enrolls through WorkLifetime.Acquire (object identity); any
//	   added WaitGroup or context lifetime outside pkg/lifecycle is a finding.
//
//	E. Second closing decider: on a line the diff adds, a read of a bool field
//	   of a struct that also holds a lifecycle.WorkLifetime (lifecycle state
//	   kept beside the owner; type and field-object identity). Closing state
//	   is read from the owner (WorkLifetime.Closing, Acquire). Expected: 0.
//
// The census runs on any head: FinishPendingDrop, AcceptedOutcome and
// WorkLifetime are optional, and a control that needs an absent API is
// adapted or omitted, which the output records.
//
// Planted controls enter only as compiler overlays; a missed or misreported
// control invalidates the run.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

const nornic = "github.com/orneryd/nornicdb/"

type control struct {
	path, marker, class, source string
	report                      bool
}

var controls = []control{
	{"pkg/server/zz_nor288_a_early.go", "nor288PlantedEarlyReturnDecider", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedEarlyReturnDecider(ctx context.Context, m *multidb.DatabaseManager) error {
	if !m.Exists("planted") {
		return nil
	}
	return m.DropDatabase(ctx, "planted")
}
`, true},
	{"pkg/server/zz_nor288_a_storage.go", "nor288PlantedStorageProbeDecider", "A", `package server

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

type nor288PlantedStore struct {
	m      *multidb.DatabaseManager
	remove func(context.Context, string) error
}

func nor288NewPlantedStore(m *multidb.DatabaseManager) *nor288PlantedStore {
	return &nor288PlantedStore{m: m, remove: m.DropDatabase}
}

func (s *nor288PlantedStore) nor288PlantedStorageProbeDecider(ctx context.Context, name string) error {
	engine, err := s.m.GetStorage(name)
	if err != nil {
		return errors.New("not found")
	}
	_ = engine
	return s.remove(ctx, name)
}
`, true},
	{"pkg/server/zz_nor288_a_branch.go", "nor288PlantedBranchDecider", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedBranchDecider(ctx context.Context, m *multidb.DatabaseManager) error {
	if m.Exists("planted") {
		return m.DropDatabase(ctx, "planted")
	}
	return nil
}
`, true},
	// A positive: the deletion is reached through a differently named
	// function value; only the value, not its name, makes it a deletion.
	{"pkg/server/zz_nor288_a_value.go", "nor288PlantedValueDecider", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedValueDecider(ctx context.Context, m *multidb.DatabaseManager) error {
	purge := m.DropDatabase
	if !m.Exists("planted") {
		return nil
	}
	return purge(ctx, "planted")
}
`, true},
	// A positive: a wrapper derives the name from its receiver and deletes;
	// its caller probes that name and calls the wrapper without passing it.
	{"pkg/server/zz_nor288_a_derived.go", "nor288PlantedDerivedDecider", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

type nor288PlantedTarget struct {
	m    *multidb.DatabaseManager
	name string
}

func (t *nor288PlantedTarget) remove(ctx context.Context) error {
	return t.m.DropDatabase(ctx, t.name)
}

func nor288PlantedDerivedDecider(ctx context.Context, t *nor288PlantedTarget) error {
	if !t.m.Exists(t.name) {
		return nil
	}
	return t.remove(ctx)
}
`, true},
	{"pkg/server/zz_nor288_a_replace.go", "nor288PlantedReplaceShape", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedReplaceShape(ctx context.Context, m *multidb.DatabaseManager, replace bool) error {
	if replace {
		return m.DropDatabase(ctx, "planted")
	} else if m.Exists("planted") {
		return nil
	}
	return nil
}
`, false},
	// A negative (original control): both paths reach a deletion; the
	// fall-through deletes through a parameter named drop, which its caller
	// binds to DropDatabase.
	{"pkg/server/zz_nor288_a_owner.go", "nor288PlantedOwnerRouted", "A", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func nor288PlantedOwnerRouted(ctx context.Context, m *multidb.DatabaseManager, drop func(context.Context, string) error) error {
	if _, err := m.GetStorage("planted"); err != nil {
		return m.FinishPendingDrop(ctx, "planted")
	}
	return drop(ctx, "planted")
}

func nor288PlantedOwnerRoutedCaller(ctx context.Context, m *multidb.DatabaseManager) error {
	return nor288PlantedOwnerRouted(ctx, m, m.DropDatabase)
}
`, false},
	{"pkg/server/zz_nor288_b_literal.go", "nor288PlantedBarrierLiteral", "B", `package server

import "github.com/orneryd/nornicdb/pkg/storage"

func nor288PlantedBarrierLiteral(err error) error {
	return &storage.AcceptedCommitError{State: storage.CommitRecoveryPending, Err: err}
}
`, true},
	{"pkg/server/zz_nor288_b_decoy.go", "nor288PlantedBarrierDecoy", "B", `package server

type AcceptedCommitError struct{ Err error }

func nor288PlantedBarrierDecoy(err error) error {
	_ = &AcceptedCommitError{Err: err}
	return err
}
`, false},
	{"pkg/storage/zz_nor288_c_twin.go", "nor288PlantedEndpointTwin", "C", `package storage

func (tx *BadgerTransaction) nor288PlantedEndpointTwin() error {
	if tx.acceptedApplication {
		return nil
	}
	return tx.revalidateEndpointsLocked()
}
`, true},
	{"pkg/multidb/zz_nor288_d_lifetime.go", "nor288PlantedLifetime", "D", `package multidb

import (
	"context"
	"sync"
)

type nor288PlantedRunner struct{ wg sync.WaitGroup }

func (r *nor288PlantedRunner) nor288PlantedLifetime(fn func(context.Context)) {
	ctx, cancel := context.WithCancel(context.Background())
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer cancel()
		fn(ctx)
	}()
}
`, true},
	{"pkg/server/zz_nor288_d_owner.go", "nor288PlantedOwnerLaunch", "D", `package server

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

func nor288PlantedOwnerLaunch(w *lifecycle.WorkLifetime, fn func(context.Context)) bool {
	ctx, release, ok := w.Acquire(w.Context())
	if !ok {
		return false
	}
	go func() {
		defer release()
		fn(ctx)
	}()
	return true
}
`, false},
	// E: a second closing decider beside the engine's lifecycle owner.
	{"pkg/storage/zz_nor288_e_flag.go", "nor288PlantedClosingDecider", "E", `package storage

func (b *BadgerEngine) nor288PlantedClosingDecider() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closing
}
`, true},
	// E negative: closing state read from the owner itself.
	{"pkg/storage/zz_nor288_e_owner.go", "nor288PlantedOwnerClosing", "E", `package storage

func (b *BadgerEngine) nor288PlantedOwnerClosing() bool {
	return b.work.Closing()
}
`, false},
	// F: a write admitted by the owner's late phase (admission sealed) instead
	// of the engine's write decider; Close rejects writers before that phase.
	{"pkg/storage/zz_nor288_f_phase.go", "nor288PlantedLatePhaseWrite", "F", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedLatePhaseWrite(p *PreparedEntityCommit) error {
	if b.work.Closing() {
		return ErrStorageClosed
	}
	_, err := b.SubmitEntityCommit(context.Background(), p, nil)
	return err
}
`, true},
	// F negative: the write asks the engine's one write decider.
	{"pkg/storage/zz_nor288_f_decider.go", "nor288PlantedWriteDecider", "F", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedWriteDecider(p *PreparedEntityCommit) error {
	if err := b.ensureOpen(true); err != nil {
		return err
	}
	_, err := b.SubmitEntityCommit(context.Background(), p, nil)
	return err
}
`, false},
	// G: enrolled accepted work reaching a call with a context Close cannot
	// cancel, directly and one call away (the shape of the WAL initial submit).
	{"pkg/storage/zz_nor288_g_direct.go", "nor288PlantedEnrolledBackground", "G", `package storage

import "context"

// nor288PlantedBlockOn waits on its context: Close can end it only through ctx.
func nor288PlantedBlockOn(ctx context.Context) { <-ctx.Done() }

func (b *BadgerEngine) nor288PlantedEnrolledBackground(p *PreparedEntityCommit) bool {
	return b.RunAcceptedContinuation(func(ctx context.Context) { nor288PlantedBlockOn(context.Background()) })
}
`, true},
	{"pkg/storage/zz_nor288_g_hop.go", "nor288PlantedEnrolledHop", "G", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedEnrolledHopHelper(p *PreparedEntityCommit) {
	nor288PlantedBlockOn(context.Background())
}

func (b *BadgerEngine) nor288PlantedEnrolledHop(p *PreparedEntityCommit) bool {
	return b.RunAcceptedContinuation(func(ctx context.Context) { b.nor288PlantedEnrolledHopHelper(p) })
}
`, true},
	// G negative: enrolled work passes the owner's work context.
	{"pkg/storage/zz_nor288_g_owner.go", "nor288PlantedEnrolledOwnerContext", "G", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedEnrolledOwnerContext(p *PreparedEntityCommit) bool {
	return b.RunAcceptedContinuation(func(ctx context.Context) { _, _ = b.SubmitEntityCommit(ctx, p, nil) })
}
`, false},
	// F: a phase decider returning the sealed phase, whose caller gates a write.
	{"pkg/storage/zz_nor288_f_decider_bool.go", "nor288PlantedPhaseDecider", "F", `package storage

import "context"

func (b *BadgerEngine) nor288PlantedPhaseDecider() bool { return b.work.Closing() }

func (b *BadgerEngine) nor288PlantedPhaseDeciderCaller(p *PreparedEntityCommit) {
	if !b.nor288PlantedPhaseDecider() {
		_, _ = b.SubmitEntityCommit(context.Background(), p, nil)
	}
}
`, true},
	// F negative: a sealed-phase read that gates only a read.
	{"pkg/storage/zz_nor288_f_readonly.go", "nor288PlantedReadOnlyClosing", "F", `package storage

func (b *BadgerEngine) nor288PlantedReadOnlyClosing() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.work.Closing() {
		return 0
	}
	return len(b.acceptedApplications)
}
`, false},
	// G classification plants, each reached from enrolled work.
	{"pkg/storage/zz_nor288_g_classes.go", "nor288PlantedG", "G", `package storage

import (
	"context"
	"log/slog"
	"time"
)

func nor288PlantedLogOnlyHelper(ctx context.Context) { slog.Default().Log(ctx, slog.LevelInfo, "planted") }

func nor288PlantedLogThenWaitHelper(ctx context.Context) {
	slog.Default().Log(ctx, slog.LevelInfo, "planted")
	<-ctx.Done()
}

func (b *BadgerEngine) nor288PlantedGClasses(p *PreparedEntityCommit) bool {
	return b.RunAcceptedContinuation(func(ctx context.Context) {
		nor288PlantedGLogOnly()
		nor288PlantedGLogThenWait()
		nor288PlantedGDeadline()
		nor288PlantedGCancel()
		b.nor288PlantedGJoin()
		b.nor288PlantedGAcquire()
		nor288PlantedGFuncValue(p, b)
	})
}

func nor288PlantedGLogOnly()       { nor288PlantedLogOnlyHelper(context.Background()) }
func nor288PlantedGLogThenWait()   { nor288PlantedLogThenWaitHelper(context.Background()) }
func nor288PlantedGDeadline()      { _, cancel := context.WithTimeout(context.Background(), time.Second); cancel() }
func nor288PlantedGCancel()        { _, cancel := context.WithCancel(context.Background()); cancel() }
func (b *BadgerEngine) nor288PlantedGJoin() { _ = b.work.Shutdown(context.Background(), nil) }
func (b *BadgerEngine) nor288PlantedGAcquire() {
	if _, release, ok := b.work.Acquire(context.Background()); ok {
		release()
	}
}

func nor288PlantedGHookTarget(p *PreparedEntityCommit, b *BadgerEngine) { nor288PlantedBlockOn(context.Background()) }

var nor288PlantedGHook = nor288PlantedGHookTarget

func nor288PlantedGFuncValue(p *PreparedEntityCommit, b *BadgerEngine) { nor288PlantedGHook(p, b) }

func nor288PlantedNilGuardLogHelper(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	slog.Default().Log(ctx, slog.LevelInfo, "planted")
}

func nor288PlantedNilGuardWaitHelper(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	<-ctx.Done()
}

func nor288PlantedGNilGuardLog()  { nor288PlantedNilGuardLogHelper(context.Background()) }
func nor288PlantedGNilGuardWait() { nor288PlantedNilGuardWaitHelper(context.Background()) }

func (b *BadgerEngine) nor288PlantedGNilGuard() bool {
	return b.RunAcceptedContinuation(func(ctx context.Context) {
		nor288PlantedGNilGuardLog()
		nor288PlantedGNilGuardWait()
	})
}
`, false},
	// F after audit #31 (each reported; the twin after them is not): a write
	// two calls away; a decision returned as an error and forwarded by a
	// helper; an interface callee that writes; a flag recorded beside an owner
	// by a callee and read elsewhere; a bound method value; a closure; a
	// decision passed as an argument.
	{"pkg/storage/zz_nor288_f_hop.go", "nor288PlantedFHop", "F", `package storage

func (b *BadgerEngine) nor288PlantedFHopB(p *PreparedEntityCommit) { _ = p.startCommit(nil) }
func (b *BadgerEngine) nor288PlantedFHopA(p *PreparedEntityCommit) { b.nor288PlantedFHopB(p) }

func (b *BadgerEngine) nor288PlantedFHop(p *PreparedEntityCommit) {
	if b.work.Closing() {
		return
	}
	b.nor288PlantedFHopA(p)
}
`, true},
	{"pkg/storage/zz_nor288_f_forward.go", "nor288PlantedFCheck", "F", `package storage

func (b *BadgerEngine) nor288PlantedFCheck() error {
	if b.work.Closing() {
		return ErrStorageClosed
	}
	return nil
}

func (b *BadgerEngine) nor288PlantedFForward() error { return b.nor288PlantedFCheck() }

func (b *BadgerEngine) nor288PlantedFForwarded(p *PreparedEntityCommit) {
	if err := b.nor288PlantedFForward(); err != nil {
		return
	}
	_ = p.startCommit(nil)
}
`, true},
	{"pkg/storage/zz_nor288_f_dynamic.go", "nor288PlantedFDynamic", "F", `package storage

type nor288PlantedFWriter interface{ nor288PlantedFWrite(*PreparedEntityCommit) }
type nor288PlantedFCommitter struct{}

func (nor288PlantedFCommitter) nor288PlantedFWrite(p *PreparedEntityCommit) { _ = p.startCommit(nil) }

func (b *BadgerEngine) nor288PlantedFDynamic(w nor288PlantedFWriter, p *PreparedEntityCommit) {
	if !b.work.Closing() {
		w.nor288PlantedFWrite(p)
	}
}

func (b *BadgerEngine) nor288PlantedFDynamicCaller(p *PreparedEntityCommit) {
	b.nor288PlantedFDynamic(nor288PlantedFCommitter{}, p)
}
`, true},
	{"pkg/storage/zz_nor288_f_flag.go", "nor288PlantedFFlag", "F", `package storage

import "github.com/orneryd/nornicdb/pkg/lifecycle"

type nor288PlantedFOwner struct {
	work lifecycle.WorkLifetime
	shut bool
}

func (o *nor288PlantedFOwner) nor288PlantedFMarkShut() { o.shut = true }

func (b *BadgerEngine) nor288PlantedFFlag(o *nor288PlantedFOwner) {
	if b.work.Closing() {
		o.nor288PlantedFMarkShut()
	}
}

func nor288PlantedFFlagGate(o *nor288PlantedFOwner, p *PreparedEntityCommit) {
	if !o.shut {
		_ = p.startCommit(nil)
	}
}
`, true},
	{"pkg/storage/zz_nor288_f_method_value.go", "nor288PlantedFMethodValue", "F", `package storage

func (b *BadgerEngine) nor288PlantedFMethodValue(p *PreparedEntityCommit) {
	closing := b.work.Closing
	if !closing() {
		_ = p.startCommit(nil)
	}
}
`, true},
	{"pkg/storage/zz_nor288_f_closure.go", "nor288PlantedFClosure", "F", `package storage

func (b *BadgerEngine) nor288PlantedFRun(f func()) { f() }

func (b *BadgerEngine) nor288PlantedFClosure(p *PreparedEntityCommit) {
	b.nor288PlantedFRun(func() {
		if b.work.Closing() {
			return
		}
		_ = p.startCommit(nil)
	})
}
`, true},
	{"pkg/storage/zz_nor288_f_arg.go", "nor288PlantedFArg", "F", `package storage

func nor288PlantedFArgGate(closing bool, p *PreparedEntityCommit) {
	if !closing {
		_ = p.startCommit(nil)
	}
}

func (b *BadgerEngine) nor288PlantedFArg(p *PreparedEntityCommit) {
	nor288PlantedFArgGate(b.work.Closing(), p)
}
`, true},
	// F twin: the same two-hop shape whose helpers only read.
	{"pkg/storage/zz_nor288_f_read_hop.go", "nor288PlantedFReadHop", "F", `package storage

func (b *BadgerEngine) nor288PlantedFReadHopB() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.acceptedApplications)
}
func (b *BadgerEngine) nor288PlantedFReadHopA() int { return b.nor288PlantedFReadHopB() }

func (b *BadgerEngine) nor288PlantedFReadHop() int {
	if b.work.Closing() {
		return 0
	}
	return b.nor288PlantedFReadHopA()
}
`, false},
	// G after audit #31, reached from enrolled work; expected classes are in
	// classControls: a callback field the program assigns (followed) and one
	// it never assigns (closed); Done compared to nil, polled, and in a
	// blocking select; an interface callee that blocks and one that does not;
	// a context stored in a field then waited on, and one only checked; a
	// context returned by a helper then waited on, and one only checked; a
	// receipt wait on Background.
	{"pkg/storage/zz_nor288_g_flow.go", "nor288PlantedG", "G", `package storage

import (
	"context"
	"sync/atomic"
)

var nor288PlantedSetHook, nor288PlantedUnsetHook atomic.Pointer[func(context.Context, *nor288PlantedHookTag)]

func nor288PlantedInstallSetHook() {
	f := nor288PlantedStoredHookTarget
	nor288PlantedSetHook.Store(&f)
}
func nor288PlantedInstallUnsetHook() {
	f := nor288PlantedUnstoredHookTarget
	nor288PlantedUnsetHook.Store(&f)
}
func nor288PlantedGSetHook() {
	if h := nor288PlantedSetHook.Load(); h != nil {
		(*h)(context.Background(), nil)
	}
}
func nor288PlantedGUnsetHook() {
	if h := nor288PlantedUnsetHook.Load(); h != nil {
		(*h)(context.Background(), nil)
	}
}

type nor288PlantedHookTag struct{}

type nor288PlantedHooks struct {
	stored, unstored func(context.Context, *nor288PlantedHookTag)
}

func nor288PlantedStoredHookTarget(ctx context.Context, _ *nor288PlantedHookTag)   { <-ctx.Done() }
func nor288PlantedUnstoredHookTarget(ctx context.Context, _ *nor288PlantedHookTag) { <-ctx.Done() }

var nor288PlantedHookState = &nor288PlantedHooks{stored: nor288PlantedStoredHookTarget}

func nor288PlantedGStoredHook() { nor288PlantedHookState.stored(context.Background(), nil) }
func nor288PlantedGUnstoredHook() {
	if h := nor288PlantedHookState.unstored; h != nil {
		h(context.Background(), nil)
	}
}

func nor288PlantedDoneNil(ctx context.Context) bool { return ctx.Done() != nil }
func nor288PlantedDonePoll(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
func nor288PlantedDoneSelect(ctx context.Context, c chan struct{}) {
	select {
	case <-ctx.Done():
	case <-c:
	}
}
func nor288PlantedGDoneNil()    { _ = nor288PlantedDoneNil(context.Background()) }
func nor288PlantedGDonePoll()   { _ = nor288PlantedDonePoll(context.Background()) }
func nor288PlantedGDoneSelect() { nor288PlantedDoneSelect(context.Background(), nil) }

type nor288PlantedBlockingWaiter interface{ nor288PlantedWaitBlocking(context.Context) }
type nor288PlantedQuietWaiter interface{ nor288PlantedWaitQuiet(context.Context) }
type nor288PlantedWaiterImpl struct{}

func (nor288PlantedWaiterImpl) nor288PlantedWaitBlocking(ctx context.Context) { <-ctx.Done() }
func (nor288PlantedWaiterImpl) nor288PlantedWaitQuiet(ctx context.Context)    { _ = ctx.Err() }

func nor288PlantedGDynamicBlocking() {
	var w nor288PlantedBlockingWaiter = nor288PlantedWaiterImpl{}
	w.nor288PlantedWaitBlocking(context.Background())
}
func nor288PlantedGDynamicQuiet() {
	var w nor288PlantedQuietWaiter = nor288PlantedWaiterImpl{}
	w.nor288PlantedWaitQuiet(context.Background())
}

type nor288PlantedBlockingBox struct{ ctx context.Context }
type nor288PlantedQuietBox struct{ ctx context.Context }

func nor288PlantedBlockingBoxWait(b *nor288PlantedBlockingBox)  { <-b.ctx.Done() }
func nor288PlantedQuietBoxCheck(b *nor288PlantedQuietBox) error { return b.ctx.Err() }
func nor288PlantedGFieldBlocking() {
	nor288PlantedBlockingBoxWait(&nor288PlantedBlockingBox{ctx: context.Background()})
}
func nor288PlantedGFieldQuiet() {
	_ = nor288PlantedQuietBoxCheck(&nor288PlantedQuietBox{ctx: context.Background()})
}

func nor288PlantedBlockingBg() context.Context { return context.Background() }
func nor288PlantedQuietBg() context.Context    { return context.Background() }
func nor288PlantedGReturnedBlocking()          { <-nor288PlantedBlockingBg().Done() }
func nor288PlantedGReturnedQuiet()             { _ = nor288PlantedQuietBg().Err() }

func nor288PlantedGReceiptWait(r *CommitReceipt) { _ = r.WaitOutcome(context.Background()) }

func (b *BadgerEngine) nor288PlantedGFlow() bool {
	return b.RunAcceptedContinuation(func(ctx context.Context) {
		nor288PlantedGStoredHook()
		nor288PlantedGUnstoredHook()
		nor288PlantedGDoneNil()
		nor288PlantedGDonePoll()
		nor288PlantedGDoneSelect()
		nor288PlantedGDynamicBlocking()
		nor288PlantedGDynamicQuiet()
		nor288PlantedGFieldBlocking()
		nor288PlantedGFieldQuiet()
		nor288PlantedGReturnedBlocking()
		nor288PlantedGReturnedQuiet()
		nor288PlantedGReceiptWait(nil)
		nor288PlantedInstallSetHook()
		nor288PlantedGSetHook()
		nor288PlantedGUnsetHook()
	})
}
`, false},
	// D: parent context plus deadline (classified) and a derived context
	// without a deadline (must report), on planted (added) lines.
	{"pkg/storage/zz_nor288_d_deadline.go", "nor288PlantedDDeadline", "D", `package storage

import (
	"context"
	"time"
)

func nor288PlantedDDeadline(ctx context.Context) {
	_, cancel := context.WithTimeout(ctx, time.Second)
	cancel()
}
`, false},
	{"pkg/storage/zz_nor288_d_cancel.go", "nor288PlantedDCancel", "D", `package storage

import "context"

func nor288PlantedDCancel(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	cancel()
}
`, true},
}

type finding struct {
	Class string   `json:"class"`
	Func  string   `json:"func"`
	File  string   `json:"file"`
	Line  int      `json:"line"`
	Shape string   `json:"shape,omitempty"`
	Path  []string `json:"path,omitempty"`
	Added bool     `json:"added_line"`
}

var hunk = regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@`)

func addedLines(root, base string) map[string]map[int]bool {
	out := map[string]map[int]bool{}
	data, err := exec.Command("git", "-C", root, "diff", "-U0", "--no-color", base+"...HEAD", "--", "*.go").Output()
	if err != nil {
		panic(fmt.Sprintf("git diff: %v", err))
	}
	file := ""
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 1<<20), 1<<26)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "+++ b/") {
			file = strings.TrimPrefix(line, "+++ b/")
			out[file] = map[int]bool{}
			continue
		}
		if m := hunk.FindStringSubmatch(line); m != nil && file != "" {
			start, _ := strconv.Atoi(m[1])
			count := 1
			if m[2] != "" {
				count, _ = strconv.Atoi(m[2])
			}
			for i := 0; i < count; i++ {
				out[file][start+i] = true
			}
		}
	}
	return out
}

func method(pkg *types.Package, typeName, name string) *types.Func {
	obj := pkg.Scope().Lookup(typeName)
	if obj == nil {
		panic("missing type " + pkg.Path() + "." + typeName)
	}
	m, _, _ := types.LookupFieldOrMethod(types.NewPointer(obj.Type()), true, pkg, name)
	fn, ok := m.(*types.Func)
	if !ok {
		panic("missing method " + typeName + "." + name)
	}
	return fn
}

func field(pkg *types.Package, typeName, name string) *types.Var {
	obj := pkg.Scope().Lookup(typeName)
	f, _, _ := types.LookupFieldOrMethod(obj.Type(), true, pkg, name)
	v, ok := f.(*types.Var)
	if !ok {
		panic("missing field " + typeName + "." + name)
	}
	return v
}

func objOf(fn *ssa.Function) *types.Func {
	if fn == nil {
		return nil
	}
	o, _ := fn.Object().(*types.Func)
	return o
}

func main() {
	root, base := os.Args[1], os.Args[2]
	overlay := map[string][]byte{}
	planted := map[string]bool{}
	// Controls that need an API this head lacks are adapted or omitted.
	hasFinish := sourceHas(root, "pkg/multidb", "func (m *DatabaseManager) FinishPendingDrop(")
	hasLifetime := sourceHas(root, "pkg/lifecycle", "func (w *WorkLifetime) Acquire(")
	hasEngineOwner := sourceHas(root, "pkg/storage", "work lifecycle.WorkLifetime")
	adaptations := map[string]string{}
	var active []control
	for _, c := range controls {
		if !hasFinish && strings.Contains(c.source, "m.FinishPendingDrop(") {
			c.source = strings.ReplaceAll(c.source, "m.FinishPendingDrop(", "m.DropDatabase(")
			adaptations[c.marker] = "FinishPendingDrop absent on this head: the routed branch deletes through DropDatabase"
		}
		if (c.class == "E" || c.class == "F" || c.class == "G") && !hasEngineOwner {
			adaptations[c.marker] = "the engine has no lifecycle owner on this head: control omitted"
			continue
		}
		if !hasLifetime && strings.Contains(c.source, "lifecycle.WorkLifetime") {
			adaptations[c.marker] = "WorkLifetime absent on this head: control omitted"
			continue
		}
		active = append(active, c)
		path := filepath.Join(root, filepath.FromSlash(c.path))
		overlay[path] = []byte(c.source)
		planted[path] = true
	}
	fset := token.NewFileSet()
	cfg := &packages.Config{Mode: packages.LoadAllSyntax, Dir: root, Fset: fset, Overlay: overlay, Tests: false,
		Env: append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=1", "GOWORK=off", "GOFLAGS=-p=2"), BuildFlags: []string{"-mod=readonly", "-race", "-tags=noui,nolocalllm"}}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		panic(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		panic("package load errors")
	}
	prog, _ := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	funcs := sourceFunctions(prog)
	cg := vta.CallGraph(funcs, cha.CallGraph(prog))
	graph := buildGraph(cg, funcs, storedFields(prog))
	closeWriterless(prog, graph)

	byPath := map[string]*packages.Package{}
	packages.Visit(pkgs, nil, func(p *packages.Package) { byPath[p.PkgPath] = p })
	multidbPkg, storagePkg := byPath[nornic+"pkg/multidb"].Types, byPath[nornic+"pkg/storage"].Types
	owners := map[*types.Func]bool{}
	for _, n := range []string{"DropDatabase", "DropCompositeDatabase", "FinishPendingDrop"} {
		if fn := methodOpt(multidbPkg, "DatabaseManager", n); fn != nil {
			owners[fn] = true
		}
	}
	probeOwners := map[*types.Func]bool{}
	for _, n := range []string{"Exists", "ExistsOrIsConstituent", "GetStorage", "GetStorageWithAuth", "GetDatabase", "ResolveDatabase", "IsCompositeDatabase"} {
		probeOwners[method(multidbPkg, "DatabaseManager", n)] = true
	}
	constructor, _ := storagePkg.Scope().Lookup("AcceptedOutcome").(*types.Func)
	barrierType := storagePkg.Scope().Lookup("AcceptedCommitError").Type()
	acceptedField := field(storagePkg, "BadgerTransaction", "acceptedApplication")
	revalidate := method(storagePkg, "BadgerTransaction", "revalidateEndpointsLocked")
	var acquire, closingFn *types.Func
	var engineWork *types.Var
	if v := storagePkg.Scope().Lookup("BadgerEngine"); v != nil {
		engineWork = field(storagePkg, "BadgerEngine", "work")
	}
	writeDecider := method(storagePkg, "BadgerEngine", "ensureOpen")
	writeDeciderLocked := methodOpt(storagePkg, "BadgerEngine", "ensureOpenLocked")
	ownerClose := method(storagePkg, "BadgerEngine", "Close")
	var lifetimeType types.Type
	if lp := byPath[nornic+"pkg/lifecycle"]; lp != nil {
		acquire = methodOpt(lp.Types, "WorkLifetime", "Acquire")
		closingFn = methodOpt(lp.Types, "WorkLifetime", "Closing")
		if obj := lp.Types.Scope().Lookup("WorkLifetime"); obj != nil {
			lifetimeType = obj.Type()
		}
	}
	// E: bool fields of every struct that also holds a lifecycle owner.
	stateFields := map[*types.Var]bool{}
	if lifetimeType != nil {
		for _, p := range byPath {
			if !strings.HasPrefix(p.PkgPath, nornic) || p.Types == nil {
				continue
			}
			for _, name := range p.Types.Scope().Names() {
				tn, ok := p.Types.Scope().Lookup(name).(*types.TypeName)
				if !ok {
					continue
				}
				st, ok := tn.Type().Underlying().(*types.Struct)
				if !ok {
					continue
				}
				hosts := false
				for i := 0; i < st.NumFields(); i++ {
					t := st.Field(i).Type()
					if ptr, ok := t.(*types.Pointer); ok {
						t = ptr.Elem()
					}
					hosts = hosts || types.Identical(t, lifetimeType)
				}
				if !hosts {
					continue
				}
				for i := 0; i < st.NumFields(); i++ {
					if basic, ok := st.Field(i).Type().Underlying().(*types.Basic); ok && basic.Kind() == types.Bool {
						stateFields[st.Field(i)] = true
					}
				}
			}
		}
	}

	// Deleting functions: the owner deletions, then every caller that passes
	// a name taken from its own inputs through to one of them.
	deleting := map[*ssa.Function]bool{}
	var queue []*ssa.Function
	for fn := range graph.funcs {
		if owners[objOf(fn)] {
			deleting[fn] = true
			queue = append(queue, fn)
		}
	}
	for len(queue) > 0 {
		callee := queue[0]
		queue = queue[1:]
		for _, e := range graph.in[callee] {
			if deleting[e.caller] || !passesInputName(e.site.Common(), e.caller) {
				continue
			}
			deleting[e.caller] = true
			queue = append(queue, e.caller)
		}
	}
	// Probe set: owner probes plus thin wrappers whose every call is a probe.
	probe := map[*ssa.Function]bool{}
	for fn := range graph.funcs {
		if probeOwners[objOf(fn)] {
			probe[fn] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for fn := range graph.funcs {
			out := graph.out[fn]
			if probe[fn] || len(out) == 0 {
				continue
			}
			thin := true
			for _, e := range out {
				thin = thin && probe[e.callee]
			}
			if thin {
				probe[fn] = true
				changed = true
			}
		}
	}
	deleteSite, probeSite := map[token.Pos]bool{}, map[token.Pos]bool{}
	calleesAt := map[token.Pos][]*ssa.Function{}
	for _, edges := range graph.out {
		for _, e := range edges {
			pos := e.site.Common().Pos()
			calleesAt[pos] = append(calleesAt[pos], e.callee)
			if deleting[e.callee] {
				deleteSite[pos] = true
			}
			if probe[e.callee] {
				probeSite[pos] = true
			}
		}
	}

	added := addedLines(root, base)
	// Write-admission sites (object identity) and the non-fallback call graph.
	writeSites := map[*ssa.Function]bool{}
	for _, m := range []*types.Func{methodOpt(storagePkg, "BadgerEngine", "beginBarrierWrite"), methodOpt(storagePkg, "BadgerEngine", "beginLifecycleWrite"),
		methodOpt(storagePkg, "BadgerEngine", "beginEntityWrite"), methodOpt(storagePkg, "BadgerEngine", "submitAcceptedEntityApplication"),
		methodOpt(storagePkg, "PreparedEntityCommit", "startCommit")} {
		if m != nil {
			if f := prog.FuncValue(m); f != nil {
				writeSites[f] = true
			}
		}
	}
	rel := func(file string) string {
		r, err := filepath.Rel(root, file)
		if err != nil || strings.HasPrefix(r, "..") {
			return filepath.ToSlash(file)
		}
		return filepath.ToSlash(r)
	}
	var findings, lifetimeSites, receiptWaits []finding
	unitPackages := []string{"pkg/storage", "pkg/replication", "pkg/multidb", "pkg/qdrantgrpc", "pkg/cypher"}
	inUnitFile := func(file string) bool {
		for _, u := range unitPackages {
			if strings.HasPrefix(file, u+"/") {
				return true
			}
		}
		return false
	}
	var candidatesA []string
	record := func(class, file string, fn *ast.FuncDecl, shape string) {
		findings = append(findings, finding{Class: class, Func: funcName(fn), File: rel(file), Line: fset.Position(fn.Pos()).Line, Shape: shape})
	}
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, nornic) {
			continue
		}
		info := p.TypesInfo
		for i, file := range p.Syntax {
			path := p.CompiledGoFiles[i]
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			inOwner := p.PkgPath == nornic+"pkg/multidb"
			isPlanted := planted[path]
			onAdded := func(pos token.Pos) bool { return isPlanted || added[rel(path)][fset.Position(pos).Line] }
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				fnObj, _ := info.Defs[fn.Name].(*types.Func)
				// A
				if !inOwner && hasSite(fn.Body, probeSite) {
					candidatesA = append(candidatesA, rel(path)+":"+funcName(fn))
					if decides(fn, probeSite, deleteSite) {
						record("A", path, fn, "probe result changes whether a deletion of the same name is reached")
					}
				}
				// B
				if fnObj != constructor {
					reported := false
					ast.Inspect(fn.Body, func(x ast.Node) bool {
						if lit, ok := x.(*ast.CompositeLit); ok && !reported {
							if tv, ok := info.Types[lit]; ok && types.Identical(tv.Type, barrierType) {
								record("B", path, fn, "constructs the accepted-outcome barrier")
								reported = true
							}
						}
						return !reported
					})
				}
				// C
				if fnObj != revalidate {
					usesField, callsRevalidate := false, false
					ast.Inspect(fn.Body, func(x ast.Node) bool {
						switch n := x.(type) {
						case *ast.SelectorExpr:
							if sel := info.Selections[n]; sel != nil && sel.Obj() == acceptedField {
								usesField = true
							}
						case *ast.CallExpr:
							for _, c := range calleesAt[n.Lparen] {
								if objOf(c) == revalidate {
									callsRevalidate = true
								}
							}
						}
						return true
					})
					if usesField && callsRevalidate {
						record("C", path, fn, "uses acceptedApplication and calls endpoint revalidation")
					}
				}
				// D
				inLifecycle := p.PkgPath == nornic+"pkg/lifecycle"
				enrolls := false
				ast.Inspect(fn.Body, func(x ast.Node) bool {
					if sel, ok := x.(*ast.SelectorExpr); ok && acquire != nil && info.Uses[sel.Sel] == acquire {
						enrolls = true
					}
					return true
				})
				ast.Inspect(fn.Body, func(x ast.Node) bool {
					switch n := x.(type) {
					case *ast.GoStmt:
						if !onAdded(n.Pos()) {
							return true
						}
						site := finding{Class: "D", Func: funcName(fn), File: rel(path), Line: fset.Position(n.Pos()).Line, Shape: "go"}
						if enrolls || inLifecycle {
							site.Shape += " (enrolled in lifecycle.WorkLifetime)"
						} else {
							findings = append(findings, site)
						}
						lifetimeSites = append(lifetimeSites, site)
					case *ast.CallExpr:
						var obj types.Object
						switch f := n.Fun.(type) {
						case *ast.SelectorExpr:
							obj = info.Uses[f.Sel]
						case *ast.Ident:
							obj = info.Uses[f]
						}
						fo, ok := obj.(*types.Func)
						if !ok || fo.Pkg() == nil || !onAdded(n.Pos()) || inLifecycle {
							return true
						}
						path := fo.Pkg().Path()
						if (path == "context" && strings.HasPrefix(fo.Name(), "With") && fo.Name() != "WithValue" && fo.Name() != "WithoutCancel") || path == "golang.org/x/sync/errgroup" {
							site := finding{Class: "D", Func: funcName(fn), File: rel(p.CompiledGoFiles[i]), Line: fset.Position(n.Pos()).Line, Shape: fo.Pkg().Name() + "." + fo.Name()}
							if path == "context" && (fo.Name() == "WithTimeout" || fo.Name() == "WithDeadline") {
								site.Class, site.Shape = "D-classified", "parent context plus deadline, no new lifetime ("+fo.Name()+")"
							}
							findings = append(findings, site)
							lifetimeSites = append(lifetimeSites, site)
						}
					}
					return true
				})
			}
			// E: reads of lifecycle state kept beside the owner, on added lines.
			// The engine's one decider (ensureOpen, ensureOpenLocked) is the
			// owner's own read point and is not a second decider.
			if p.PkgPath != nornic+"pkg/lifecycle" {
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok || fn.Body == nil {
						continue
					}
					if fo, _ := info.Defs[fn.Name].(*types.Func); fo != nil && (fo == writeDecider || (writeDeciderLocked != nil && fo == writeDeciderLocked)) {
						continue
					}
					reported := false
					ast.Inspect(fn.Body, func(x ast.Node) bool {
						sel, ok := x.(*ast.SelectorExpr)
						if !ok || reported {
							return !reported
						}
						if s := info.Selections[sel]; s != nil {
							if v, ok := s.Obj().(*types.Var); ok && stateFields[v] && onAdded(sel.Pos()) {
								record("E", path, fn, "reads lifecycle state "+v.Name()+" kept beside the owner")
								reported = true
							}
						}
						return !reported
					})
				}
			}
			// D: WaitGroup variables and fields declared on added lines (type identity).
			ast.Inspect(file, func(x ast.Node) bool {
				id, ok := x.(*ast.Ident)
				if !ok {
					return true
				}
				v, ok := info.Defs[id].(*types.Var)
				if !ok || v == nil || !isWaitGroup(v.Type()) || !onAdded(id.Pos()) {
					return true
				}
				site := finding{Class: "D", Func: "WaitGroup " + id.Name, File: rel(path), Line: fset.Position(id.Pos()).Line, Shape: "sync.WaitGroup"}
				if isPlanted {
					site.Func = "nor288PlantedLifetime WaitGroup " + id.Name
				}
				if p.PkgPath == nornic+"pkg/lifecycle" {
					site.Shape += " (the lifecycle owner)"
				} else {
					findings = append(findings, site)
				}
				lifetimeSites = append(lifetimeSites, site)
				return true
			})
		}
	}
	// F (whole unit, every line), after audit #31. Sources: every call to
	// WorkLifetime.Closing in the program (closures, bound method values and
	// helpers taking the lifetime as a parameter included) whose receiver may
	// be the engine owner's field BadgerEngine.work: traced through fields,
	// parameters (every caller's argument), free variables, phis and returns;
	// a receiver that cannot be traced counts as the engine's. Excluded only:
	// the engine's one decider (ensureOpen, ensureOpenLocked), the owner's
	// Close, and pkg/lifecycle's own declared functions.
	// Membership: the decision is followed by the value flow (explicitly, and
	// implicitly at its If: a join's phis, a return or a constant flag store
	// into an existing object inside a branch region, and lifecycle state a
	// callee in the region records beside an owner); a decision gates a write
	// admission when a call in either branch region (blocks reachable from the
	// branch before the branches' join) may reach a write-admission site in
	// the call graph (any number of calls; static, interface, function-value
	// and signature-fallback edges). A function that is itself a write-admission
	// site and reads the sealed phase is a member. An escape the flow does not
	// model is a member ("unproven").
	type fVerdict struct {
		Func   string `json:"func"`
		File   string `json:"file"`
		Line   int    `json:"line"`
		Member bool   `json:"member"`
		Reason string `json:"reason"`
	}
	var fVerdicts []fVerdict
	sinkPkg := func(path string) bool {
		return path == "log/slog" || path == "log" || path == nornic+"pkg/observability" || strings.HasPrefix(path, "go.opentelemetry.io/")
	}
	env := newFlowEnv(prog, graph, fset, rel, func(f *ssa.Function) bool { return sinkPkg(pkgPathOf(f)) })
	if closingFn != nil && engineWork != nil {
		closingSSA := prog.FuncValue(closingFn)
		excluded := map[*ssa.Function]bool{}
		for _, m := range []*types.Func{writeDecider, writeDeciderLocked, ownerClose} {
			if m != nil {
				if f := prog.FuncValue(m); f != nil {
					excluded[f] = true
				}
			}
		}
		// Admission reach: every function with a call path to a write-admission site.
		admits := map[*ssa.Function]bool{}
		var aq []*ssa.Function
		for f := range writeSites {
			admits[f] = true
			aq = append(aq, f)
		}
		for len(aq) > 0 {
			f := aq[0]
			aq = aq[1:]
			for _, e := range graph.in[f] {
				if !admits[e.caller] {
					admits[e.caller] = true
					aq = append(aq, e.caller)
				}
			}
		}
		// Lifecycle state beside an owner: bool and sync/atomic fields of a
		// module struct that also holds a lifecycle.WorkLifetime.
		ownerState := map[*types.Var]bool{}
		if lifetimeType != nil {
			for _, p := range byPath {
				if !strings.HasPrefix(p.PkgPath, nornic) || p.Types == nil {
					continue
				}
				for _, name := range p.Types.Scope().Names() {
					tn, ok := p.Types.Scope().Lookup(name).(*types.TypeName)
					if !ok {
						continue
					}
					st, ok := tn.Type().Underlying().(*types.Struct)
					if !ok {
						continue
					}
					hosts := false
					for i := 0; i < st.NumFields(); i++ {
						t := st.Field(i).Type()
						if ptr, ok := t.(*types.Pointer); ok {
							t = ptr.Elem()
						}
						hosts = hosts || types.Identical(t, lifetimeType)
					}
					for i := 0; hosts && i < st.NumFields(); i++ {
						t := st.Field(i).Type()
						basic, isBasic := t.Underlying().(*types.Basic)
						named, isNamed := t.(*types.Named)
						if (isBasic && basic.Kind() == types.Bool) || (isNamed && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "sync/atomic") {
							ownerState[st.Field(i).Origin()] = true
						}
					}
				}
			}
		}
		// Owner state a function records with a constant, directly or through
		// any module function it calls.
		directFlags := func(f *ssa.Function) []*types.Var {
			var out []*types.Var
			for _, b := range f.Blocks {
				for _, instr := range b.Instrs {
					switch x := instr.(type) {
					case *ssa.Store:
						if fa, ok := x.Addr.(*ssa.FieldAddr); ok {
							if _, isConst := x.Val.(*ssa.Const); isConst {
								if v := fieldVar(fa.X, fa.Field); v != nil && ownerState[v] {
									out = append(out, v)
								}
							}
						}
					case ssa.CallInstruction:
						cc := x.Common()
						if c := cc.StaticCallee(); c != nil && pkgPathOf(c) == "sync/atomic" && len(cc.Args) > 0 {
							if fa, ok := cc.Args[0].(*ssa.FieldAddr); ok {
								if v := fieldVar(fa.X, fa.Field); v != nil && ownerState[v] {
									out = append(out, v)
								}
							}
						}
					}
				}
			}
			return out
		}
		flagMemo := map[*ssa.Function][]*types.Var{}
		calleeFlags := func(root *ssa.Function) []*types.Var {
			if got, ok := flagMemo[root]; ok {
				return got
			}
			seen := map[*ssa.Function]bool{root: true}
			queue := []*ssa.Function{root}
			var out []*types.Var
			for len(queue) > 0 {
				f := queue[0]
				queue = queue[1:]
				out = append(out, directFlags(f)...)
				for _, e := range graph.out[f] {
					if !seen[e.callee] && strings.HasPrefix(pkgPathOf(e.callee), nornic) {
						seen[e.callee] = true
						queue = append(queue, e.callee)
					}
				}
			}
			flagMemo[root] = out
			return out
		}
		idomMemo := map[*ssa.Function]map[*ssa.BasicBlock]*ssa.BasicBlock{}
		fHook := func(w *flowWalker, it flowItem, use ssa.Instruction) bool {
			ifi, ok := use.(*ssa.If)
			if !ok || it.kind != "dec" {
				return false
			}
			fn, block := ifi.Parent(), ifi.Block()
			if idomMemo[fn] == nil {
				idomMemo[fn] = postIdoms(fn)
			}
			join := idomMemo[fn][block]
			for _, succ := range block.Succs {
				region := regionOf(succ, join)
				for b := range region {
					for _, instr := range b.Instrs {
						switch x := instr.(type) {
						case ssa.CallInstruction:
							for _, c := range env.calleesAt[x] {
								if admits[c] {
									w.fail("the decision at"+strings.TrimPrefix(env.where(ifi), " at")+" controls a call to "+c.String()+", which reaches a write-admission site", nil)
									return true
								}
								for _, v := range calleeFlags(c) {
									w.pushField(v, "dec")
								}
							}
						case *ssa.Return:
							w.returnFrom(fn, "dec", -1, it.stack)
						case *ssa.Store:
							if _, isConst := x.Val.(*ssa.Const); isConst {
								root := x.Addr
								for {
									fa, ok := root.(*ssa.FieldAddr)
									if !ok {
										break
									}
									root = fa.X
								}
								if a, ok := root.(*ssa.Alloc); !(ok && region[a.Block()]) {
									w.storeTo(x.Addr, "dec", nil, x)
								}
							}
						}
					}
					for _, s := range b.Succs {
						if region[s] {
							continue
						}
						for _, instr := range s.Instrs {
							if phi, ok := instr.(*ssa.Phi); ok {
								w.push(phi, "dec", -1, it.stack)
							}
						}
					}
				}
			}
			return true
		}
		// Receiver origin: "engine", "other" (another owner's field) or "unknown".
		join := func(a, b string) string {
			switch {
			case a == "":
				return b
			case b == "":
				return a
			case a == "engine" || b == "engine":
				return "engine"
			case a == "unknown" || b == "unknown":
				return "unknown"
			}
			return "other"
		}
		var origin func(v ssa.Value, seen map[ssa.Value]bool) string
		origin = func(v ssa.Value, seen map[ssa.Value]bool) string {
			if seen[v] {
				return ""
			}
			seen[v] = true
			if len(seen) > 4096 {
				return "unknown"
			}
			switch x := v.(type) {
			case *ssa.FieldAddr:
				if fieldVar(x.X, x.Field) == engineWork.Origin() {
					return "engine"
				}
				return "other"
			case *ssa.Phi:
				out := ""
				for _, e := range x.Edges {
					out = join(out, origin(e, seen))
				}
				return out
			case *ssa.Parameter:
				fn, idx := x.Parent(), -1
				for i, p := range fn.Params {
					if p == x {
						idx = i
					}
				}
				if len(graph.in[fn]) == 0 {
					return "unknown"
				}
				out := ""
				for _, e := range graph.in[fn] {
					cc := e.site.Common()
					var arg ssa.Value
					switch {
					case cc.IsInvoke() && idx == 0:
						arg = cc.Value
					case cc.IsInvoke() && idx-1 < len(cc.Args):
						arg = cc.Args[idx-1]
					case !cc.IsInvoke() && idx < len(cc.Args):
						arg = cc.Args[idx]
					}
					if arg == nil {
						return "unknown"
					}
					out = join(out, origin(arg, seen))
				}
				return out
			case *ssa.FreeVar:
				fn, idx := x.Parent(), -1
				for i, f := range fn.FreeVars {
					if f == x {
						idx = i
					}
				}
				out := ""
				for _, mc := range env.closures[fn] {
					if idx >= 0 && idx < len(mc.Bindings) {
						out = join(out, origin(mc.Bindings[idx], seen))
					}
				}
				if out == "" {
					return "unknown"
				}
				return out
			case *ssa.Call:
				out := ""
				callees, _ := env.callees(x)
				for _, c := range callees {
					for _, b := range c.Blocks {
						if r, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return); ok && len(r.Results) == 1 {
							out = join(out, origin(r.Results[0], seen))
						}
					}
				}
				if out == "" {
					return "unknown"
				}
				return out
			}
			return "unknown"
		}
		for fn := range ssautil.AllFunctions(prog) {
			top := fn
			for top.Parent() != nil {
				top = top.Parent()
			}
			if excluded[top] || (fn.Synthetic == "" && pkgPathOf(top) == nornic+"pkg/lifecycle") {
				continue
			}
			for _, b := range fn.Blocks {
				for _, instr := range b.Instrs {
					call, ok := instr.(*ssa.Call)
					if !ok {
						continue
					}
					isClosing := call.Call.StaticCallee() == closingSSA
					for _, c := range env.calleesAt[call] {
						isClosing = isClosing || c == closingSSA
					}
					if !isClosing || len(call.Call.Args) == 0 {
						continue
					}
					if o := origin(call.Call.Args[0], map[ssa.Value]bool{}); o != "engine" && o != "unknown" {
						continue
					}
					// Attribution: the function holding the read; a bound method
					// value is attributed to every function creating it.
					holders := []*ssa.Function{fn}
					if fn.Synthetic != "" {
						holders = nil
						for _, mc := range env.closures[fn] {
							if len(mc.Bindings) == 0 {
								continue
							}
							if o := origin(mc.Bindings[0], map[ssa.Value]bool{}); o == "engine" || o == "unknown" {
								holders = append(holders, mc.Parent())
							}
						}
					}
					reason := ""
					if writeSites[top] {
						reason = "the function is itself a write-admission site and decides on the sealed phase"
					} else {
						w := newFlowWalker(env, 200000, fHook)
						w.push(call, "dec", -1, nil)
						if stop := w.run(); stop != "" {
							reason = stop
							if !strings.HasPrefix(stop, "the decision at") {
								reason = "unproven: the decision escapes the modelled flow (" + stop + ")"
							}
						}
					}
					for _, h := range holders {
						pos := fset.Position(call.Pos())
						if fn.Synthetic != "" {
							pos = fset.Position(h.Pos())
						}
						file := rel(pos.Filename)
						v := fVerdict{Func: shortFunc(h), File: file, Line: pos.Line, Member: reason != "", Reason: reason}
						if v.Reason == "" {
							v.Reason = "no decision path reaches a write-admission site (followed through values, returns, calls, fields, closures and branch regions)"
						}
						fVerdicts = append(fVerdicts, v)
						if reason != "" && (planted[pos.Filename] || inUnitFile(file)) {
							findings = append(findings, finding{Class: "F", Func: shortFunc(h), File: file, Line: pos.Line, Shape: "gates a write admission on the engine owner's sealed phase: " + reason, Added: planted[pos.Filename] || added[file][pos.Line]})
						}
					}
				}
			}
		}
	}
	// G: enrolled accepted work (functions passed to RunAcceptedContinuation,
	// or launched by go in a function that enrolls through Acquire) and every
	// module function they reach (call graph) passing context.Background or
	// context.TODO to a call on an added line: Close's cancellation cannot
	// reach that call, so Close may wait on it without bound.
	if ctxPkg := byPath["context"]; ctxPkg != nil && hasEngineOwner {
		background := map[types.Object]bool{ctxPkg.Types.Scope().Lookup("Background"): true, ctxPkg.Types.Scope().Lookup("TODO"): true}
		runCont := map[types.Object]bool{storagePkg.Scope().Lookup("RunAcceptedContinuation"): true}
		if m := methodOpt(storagePkg, "BadgerEngine", "RunAcceptedContinuation"); m != nil {
			runCont[m] = true
		}
		staticObj := func(c *ssa.CallCommon) types.Object {
			if f := c.StaticCallee(); f != nil && f.Object() != nil {
				return f.Object()
			}
			return nil
		}
		fnValue := func(v ssa.Value) *ssa.Function {
			switch x := v.(type) {
			case *ssa.MakeClosure:
				f, _ := x.Fn.(*ssa.Function)
				return f
			case *ssa.Function:
				return x
			}
			return nil
		}
		var receiptType types.Type
		if obj := storagePkg.Scope().Lookup("CommitReceipt"); obj != nil {
			receiptType = types.NewPointer(obj.Type())
		}
		reached := map[*ssa.Function]bool{}
		type step struct {
			from *ssa.Function
			kind string
			pos  token.Pos
		}
		via := map[*ssa.Function]step{}
		var work []*ssa.Function
		enqueueVia := func(f, from *ssa.Function, kind string, pos token.Pos) {
			if f != nil && !reached[f] && f.Pkg != nil && strings.HasPrefix(f.Pkg.Pkg.Path(), nornic) {
				reached[f] = true
				via[f] = step{from, kind, pos}
				work = append(work, f)
			}
		}
		pathTo := func(f *ssa.Function) []string {
			var out []string
			for n := 0; f != nil && n < 64; n++ {
				st := via[f]
				where := ""
				if p := fset.Position(st.pos); p.Filename != "" {
					where = " @ " + rel(p.Filename) + ":" + strconv.Itoa(p.Line)
				}
				out = append([]string{f.String() + " [" + st.kind + where + "]"}, out...)
				if strings.HasPrefix(st.kind, "root:") {
					break // the path starts at its root
				}
				f = st.from
			}
			return out
		}
		inUnit := func(f *ssa.Function) bool {
			if f == nil || f.Pkg == nil {
				return false
			}
			for _, u := range unitPackages {
				if f.Pkg.Pkg.Path() == nornic+u {
					return true
				}
			}
			return false
		}
		callsAcquire := func(f *ssa.Function) bool {
			for _, block := range f.Blocks {
				for _, instr := range block.Instrs {
					if c, ok := instr.(ssa.CallInstruction); ok && acquire != nil && staticObj(c.Common()) == types.Object(acquire) {
						return true
					}
				}
			}
			return false
		}
		for f := range graph.funcs {
			if !inUnit(f) {
				continue
			}
			// Enrolls with a unit owner: Acquire directly, or through a unit helper.
			enrolls := callsAcquire(f)
			for _, block := range f.Blocks {
				for _, instr := range block.Instrs {
					if c, ok := instr.(ssa.CallInstruction); ok && !enrolls {
						if h := c.Common().StaticCallee(); inUnit(h) && callsAcquire(h) {
							enrolls = true
						}
					}
				}
			}
			for _, block := range f.Blocks {
				for _, instr := range block.Instrs {
					c, ok := instr.(ssa.CallInstruction)
					if !ok {
						continue
					}
					if o := staticObj(c.Common()); o != nil && runCont[o] {
						for _, a := range c.Common().Args {
							enqueueVia(fnValue(a), f, "root: argument of RunAcceptedContinuation", c.Pos())
						}
					}
					if g, ok := instr.(*ssa.Go); ok && enrolls {
						enqueueVia(g.Call.StaticCallee(), f, "root: go in a function enrolling through Acquire", g.Pos())
					}
				}
			}
		}
		for len(work) > 0 {
			f := work[0]
			work = work[1:]
			for _, e := range graph.out[f] {
				kind := "call"
				if _, isGo := e.site.(*ssa.Go); isGo {
					kind = "go"
				}
				if graph.fallback[e.site] {
					kind = "signature fallback (" + kind + ")"
				}
				enqueueVia(e.callee, f, kind, e.site.Pos())
			}
			for _, anon := range f.AnonFuncs {
				enqueueVia(anon, f, "anonymous function of", anon.Pos())
			}
		}
		// VTA-only reach: the same roots, no signature-fallback edges.
		vtaReach := map[*ssa.Function]bool{}
		var vq []*ssa.Function
		for f, st := range via {
			if strings.HasPrefix(st.kind, "root:") {
				vtaReach[f] = true
				vq = append(vq, f)
			}
		}
		for len(vq) > 0 {
			f := vq[0]
			vq = vq[1:]
			next := append([]*ssa.Function{}, f.AnonFuncs...)
			for _, e := range graph.out[f] {
				if !graph.fallback[e.site] {
					next = append(next, e.callee)
				}
			}
			for _, c := range next {
				if c != nil && !vtaReach[c] && c.Pkg != nil && strings.HasPrefix(c.Pkg.Pkg.Path(), nornic) {
					vtaReach[c] = true
					vq = append(vq, c)
				}
			}
		}
		// G sources (after audit #31): every context.Background()/TODO() call
		// inside a reached function declared in a unit package, whatever its
		// first use (argument, store, return). Its value is followed by the
		// shared flow; terminal uses: non-blocking are a sink call, Value, Err,
		// Deadline, a nil or identity comparison, ctx.Done() compared to nil or
		// polled by a select with a default, a deadline parent (WithTimeout,
		// WithDeadline), AfterFunc/Cause, and the owner's own Shutdown join;
		// WithValue/WithoutCancel pass the same lifetime on. Members: a receive
		// or blocking select on ctx.Done(), the parent of WithCancel or of a
		// WorkLifetime acquire (a lifetime the owner's Close cannot end), and
		// every unmodelled use. Receipt waits are not exempt (Nornic2,
		// 2026-10-30): work that outlives the request takes its owner's
		// lifetime, so a Background() receipt wait is a member.
		acquireFns := map[types.Object]bool{}
		if lp := byPath[nornic+"pkg/lifecycle"]; lp != nil {
			for _, n := range []string{"Acquire", "acquire"} {
				if m := methodOpt(lp.Types, "WorkLifetime", n); m != nil {
					acquireFns[m] = true
				}
			}
		}
		var shutdownFn types.Object
		if lp := byPath[nornic+"pkg/lifecycle"]; lp != nil {
			if m := methodOpt(lp.Types, "WorkLifetime", "Shutdown"); m != nil {
				shutdownFn = m
			}
		}
		gHook := func(w *flowWalker, it flowItem, use ssa.Instruction) bool {
			switch it.kind {
			case "ctx":
				if _, ok := use.(*ssa.BinOp); ok {
					return true // identity or nil comparison
				}
				c, ok := use.(ssa.CallInstruction)
				if !ok {
					return false
				}
				cc := c.Common()
				if cc.IsInvoke() && cc.Value == it.v {
					switch cc.Method.Name() {
					case "Done":
						if call, ok := c.(*ssa.Call); ok {
							w.push(call, "done", -1, it.stack)
						}
					case "Err", "Value", "Deadline":
					default:
						w.fail("ctx."+cc.Method.Name()+"()", use)
					}
					return true
				}
				callee := cc.StaticCallee()
				if callee == nil {
					return false
				}
				passed := false
				for _, a := range cc.Args {
					passed = passed || a == it.v
				}
				if !passed {
					return false
				}
				if pkgPathOf(callee) == "context" {
					switch callee.Name() {
					case "WithTimeout", "WithDeadline", "WithTimeoutCause", "WithDeadlineCause", "AfterFunc", "Cause":
						return true
					case "WithValue", "WithoutCancel":
						if call, ok := c.(*ssa.Call); ok {
							w.push(call, "ctx", -1, it.stack)
						}
						return true
					case "WithCancel", "WithCancelCause":
						w.fail("parent of a cancellable lifetime (context."+callee.Name()+") that the owner's Close cannot end", use)
						return true
					}
					return false
				}
				if o := callee.Object(); o != nil && acquireFns[o] {
					w.fail("parent of enrolled work (WorkLifetime."+o.Name()+") that the owner's Close cannot cancel", use)
					return true
				}
				if o := callee.Object(); o != nil && o == shutdownFn {
					return true // the owner's own Shutdown join
				}
				return false
			case "done":
				switch u := use.(type) {
				case *ssa.UnOp:
					if u.Op == token.ARROW {
						w.fail("receive from ctx.Done()", use)
						return true
					}
				case *ssa.Select:
					if u.Blocking {
						w.fail("blocking select on ctx.Done()", use)
					}
					return true
				case *ssa.BinOp:
					return true // nil comparison
				}
			}
			return false
		}
		seen := map[string]bool{}
		for f := range reached {
			for _, block := range f.Blocks {
				for _, instr := range block.Instrs {
					c, ok := instr.(ssa.CallInstruction)
					if !ok {
						continue
					}
					// Receipt waits are listed with their path (informational).
					if callee := c.Common().StaticCallee(); callee != nil && callee.Signature.Recv() != nil && receiptType != nil && types.Identical(callee.Signature.Recv().Type(), receiptType) {
						if pos := fset.Position(c.Pos()); pos.Filename != "" && !planted[pos.Filename] {
							receiptWaits = append(receiptWaits, finding{Class: "G-wait", Func: shortFunc(f) + " -> " + callee.Name(), File: rel(pos.Filename), Line: pos.Line, Path: pathTo(f), Added: added[rel(pos.Filename)][pos.Line]})
						}
					}
					call, ok := instr.(*ssa.Call)
					if !ok {
						continue
					}
					o := staticObj(call.Common())
					if o == nil || !background[o] {
						continue
					}
					pos := fset.Position(call.Pos())
					if pos.Filename == "" || !(planted[pos.Filename] || inUnitFile(rel(pos.Filename))) {
						continue
					}
					key := rel(pos.Filename) + ":" + strconv.Itoa(pos.Line)
					if seen[key] {
						continue
					}
					seen[key] = true
					g := finding{Class: "G", Func: shortFunc(f), File: rel(pos.Filename), Line: pos.Line, Shape: "context." + o.Name() + " in enrolled work", Path: pathTo(f), Added: added[rel(pos.Filename)][pos.Line]}
					w := newFlowWalker(env, 100000, gHook)
					w.push(call, "ctx", -1, nil)
					if why := w.run(); why == "" {
						g.Class, g.Shape = "G-classified", "every use of the context, followed through values, calls, returns, fields and closures, is non-blocking (sinks, Value/Err/Deadline, nil checks, polled Done, deadline parent, owner Shutdown)"
					} else if !vtaReach[f] {
						g.Class, g.Shape = "G-closed", "reached only through the signature fallback; VTA alone (no fallback edges) finds no path from any root"
					} else {
						g.Shape += "; blocking or unknown use: " + why
					}
					findings = append(findings, g)
				}
			}
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Class != findings[j].Class {
			return findings[i].Class < findings[j].Class
		}
		return findings[i].File+findings[i].Func < findings[j].File+findings[j].Func
	})
	sort.Strings(candidatesA)
	verdicts, valid := map[string]string{}, true
	classControls := []struct{ marker, class string }{
		{"nor288PlantedGLogOnly", "G-classified"}, {"nor288PlantedGLogThenWait", "G"},
		{"nor288PlantedGDeadline", "G-classified"}, {"nor288PlantedGCancel", "G"},
		{"nor288PlantedGJoin", "G-classified"}, {"nor288PlantedGAcquire", "G"},
		{"nor288PlantedGHook", "G"},
		{"nor288PlantedGNilGuardLog", "G-classified"}, {"nor288PlantedGNilGuardWait", "G"},
		{"nor288PlantedGStoredHook", "G"}, {"nor288PlantedGUnstoredHook", "G-classified"},
		{"nor288PlantedGDoneNil", "G-classified"}, {"nor288PlantedGDonePoll", "G-classified"}, {"nor288PlantedGDoneSelect", "G"},
		{"nor288PlantedGDynamicBlocking", "G"}, {"nor288PlantedGDynamicQuiet", "G-classified"},
		{"nor288PlantedGFieldBlocking", "G"}, {"nor288PlantedGFieldQuiet", "G-classified"},
		{"nor288PlantedBlockingBg", "G"}, {"nor288PlantedQuietBg", "G-classified"},
		{"nor288PlantedGReceiptWait", "G"},
		{"nor288PlantedGSetHook", "G"}, {"nor288PlantedGUnsetHook", "G-classified"},
	}
	for _, cc := range classControls {
		found := false
		for _, f := range findings {
			found = found || (f.Class == cc.class && strings.Contains(f.Func, cc.marker))
		}
		valid = valid && found
		verdicts["Gclass:"+cc.marker] = fmt.Sprintf("expected class=%s found=%v pass=%v", cc.class, found, found)
	}
	closeInternal := true
	for _, f := range findings {
		if f.Class == "F" && strings.HasSuffix(f.Func, "(BadgerEngine).Close") {
			closeInternal = false
		}
	}
	valid = valid && closeInternal
	verdicts["F:close-internal (BadgerEngine).Close"] = fmt.Sprintf("expected reported=false pass=%v", closeInternal)
	for _, c := range active {
		if c.marker == "nor288PlantedG" {
			continue
		}
		found := false
		for _, f := range findings {
			found = found || (f.Class == c.class && strings.Contains(f.Func, c.marker))
		}
		ok := found == c.report
		valid = valid && ok
		verdicts[c.class+":"+c.marker] = fmt.Sprintf("expected reported=%v found=%v pass=%v", c.report, found, ok)
	}
	beyond := map[string][]finding{"A": {}, "B": {}, "C": {}, "D": {}, "E": {}, "F": {}, "G": {}}
	for _, f := range findings {
		if !strings.Contains(f.Func, "nor288Planted") {
			beyond[f.Class] = append(beyond[f.Class], f)
		}
	}
	counts := map[string]int{}
	for k, v := range beyond {
		counts[k] = len(v)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"author":                    "NOR-288 lane (Claude),30October2026",
		"method":                    "packages.Load whole module ./... (production, overlays), SSA, VTA call graph refining CHA; membership by object/type identity and call-graph reachability, never by callee name; diff scope git diff " + base + "...HEAD; planted controls via compiler overlay only; no product execution",
		"callgraph_nodes":           len(cg.Nodes),
		"call_sites":                len(graph.sites),
		"sites_via_signature":       graph.fallbackSites,
		"sites_closed_unassigned":   graph.closedSites,
		"sites_closed_writerless":   graph.writerlessSites,
		"deleting_functions":        len(deleting),
		"probe_functions":           len(probe),
		"controls":                  verdicts,
		"control_adaptations":       adaptations,
		"valid":                     valid,
		"beyond_owner_count":        counts,
		"beyond_owner":              beyond,
		"receipt_waits":             receiptWaits,
		"F_verdicts":                fVerdicts,
		"scope_FG":                  "unit scope of reassessment §7 (packages storage, replication, multidb, qdrantgrpc, cypher; unit-owner roots), every line; added_line marks this diff; G-classified and G-closed are computed",
		"findings":                  findings,
		"probe_candidates_A":        candidatesA,
		"lifetime_sites_introduced": lifetimeSites,
	}); err != nil {
		panic(err)
	}
	if !valid {
		fmt.Fprintln(os.Stderr, "NOR288_OWNER_CONTROLS_FAILED")
		os.Exit(3)
	}
}

type edge struct {
	caller, callee *ssa.Function
	site           ssa.CallInstruction
}

type graphIndex struct {
	funcs         map[*ssa.Function]bool
	out, in       map[*ssa.Function][]edge
	sites         map[ssa.CallInstruction]bool
	fallback      map[ssa.CallInstruction]bool
	fallbackSites int
	// closed: a call through a function-valued field the analysed program
	// never assigns (only tests set it): it calls nothing in production.
	closed      map[ssa.CallInstruction]bool
	closedSites int
	// writerlessSites: closed by closeWriterless (every writer unreachable).
	writerlessSites int
}

// buildGraph indexes the VTA call graph by function. A dynamic call (through
// a function value) inside an analysed function that VTA leaves without any
// callee is resolved conservatively to every analysed function value with an
// identical signature (plain functions, closures, bound-method and interface
// wrappers): an unresolved call never counts as reaching nothing. The one
// exception is computed (decision 3, after audit #31): a call through a load
// of a struct field that no store in the analysed program assigns is closed.
func buildGraph(cg *callgraph.Graph, funcs map[*ssa.Function]bool, stored map[*types.Var]bool) *graphIndex {
	g := &graphIndex{funcs: map[*ssa.Function]bool{}, out: map[*ssa.Function][]edge{}, in: map[*ssa.Function][]edge{}, sites: map[ssa.CallInstruction]bool{}, fallback: map[ssa.CallInstruction]bool{}, closed: map[ssa.CallInstruction]bool{}}
	add := func(e edge) {
		g.funcs[e.caller], g.funcs[e.callee] = true, true
		g.out[e.caller] = append(g.out[e.caller], e)
		g.in[e.callee] = append(g.in[e.callee], e)
		g.sites[e.site] = true
	}
	for _, node := range cg.Nodes {
		if node.Func != nil {
			g.funcs[node.Func] = true
		}
		for _, e := range node.Out {
			if e.Site != nil && e.Caller.Func != nil && e.Callee.Func != nil {
				add(edge{e.Caller.Func, e.Callee.Func, e.Site})
			}
		}
	}
	for f := range funcs {
		g.funcs[f] = true
	}
	bySignature := map[string][]*ssa.Function{}
	for f := range g.funcs {
		if f.Signature.Recv() == nil {
			key := signatureKey(f.Signature)
			bySignature[key] = append(bySignature[key], f)
		}
	}
	for f := range funcs {
		for _, block := range f.Blocks {
			for _, instr := range block.Instrs {
				site, ok := instr.(ssa.CallInstruction)
				if !ok || g.sites[site] {
					continue
				}
				common := site.Common()
				if common.IsInvoke() || common.StaticCallee() != nil {
					continue
				}
				if _, builtin := common.Value.(*ssa.Builtin); builtin {
					continue
				}
				sig, ok := common.Value.Type().Underlying().(*types.Signature)
				if !ok {
					continue
				}
				if fv := fieldLoadOf(common.Value); fv != nil && !stored[fv] {
					g.closed[site] = true
					g.closedSites++
					continue
				}
				targets := bySignature[signatureKey(sig)]
				if len(targets) > 0 {
					g.fallbackSites++
				}
				for _, callee := range targets {
					add(edge{f, callee, site})
					g.fallback[site] = true
				}
			}
		}
	}
	return g
}

// signatureKey identifies a function type by its parameter and result types
// only: a field or parameter declared without names has the same type as a
// function or bound method value whose signature carries names.
func signatureKey(sig *types.Signature) string {
	var b strings.Builder
	b.WriteString("func(")
	for i := 0; i < sig.Params().Len(); i++ {
		b.WriteString(types.TypeString(sig.Params().At(i).Type(), nil) + ",")
	}
	if sig.Variadic() {
		b.WriteString("...")
	}
	b.WriteString(")(")
	for i := 0; i < sig.Results().Len(); i++ {
		b.WriteString(types.TypeString(sig.Results().At(i).Type(), nil) + ",")
	}
	b.WriteString(")")
	return b.String()
}

// sourceFunctions is every function of the program's source: package-level
// functions, every method of every named type (value and pointer receivers),
// their anonymous functions, plus the reachability set, so that dead code is
// covered too.
func sourceFunctions(prog *ssa.Program) map[*ssa.Function]bool {
	funcs := ssautil.AllFunctions(prog)
	var add func(f *ssa.Function)
	add = func(f *ssa.Function) {
		if f == nil {
			return
		}
		funcs[f] = true
		for _, anon := range f.AnonFuncs {
			if !funcs[anon] {
				add(anon)
			}
		}
	}
	for _, pkg := range prog.AllPackages() {
		for _, member := range pkg.Members {
			switch m := member.(type) {
			case *ssa.Function:
				add(m)
			case *ssa.Type:
				named, ok := m.Type().(*types.Named)
				if !ok || named.TypeParams().Len() > 0 {
					continue
				}
				for _, t := range []types.Type{named, types.NewPointer(named)} {
					mset := prog.MethodSets.MethodSet(t)
					for i := 0; i < mset.Len(); i++ {
						add(methodValue(prog, mset.At(i)))
					}
				}
			}
		}
	}
	return funcs
}

// methodValue builds a method's function; a selection SSA cannot build
// (abstract, generic) yields nil.
func methodValue(prog *ssa.Program, sel *types.Selection) (fn *ssa.Function) {
	defer func() {
		if recover() != nil {
			fn = nil
		}
	}()
	return prog.MethodValue(sel)
}

// passesInputName reports whether a call in f passes a string argument taken
// from f's own inputs: a parameter or captured variable, or a value loaded
// from one through field, pointer, index or conversion steps.
func passesInputName(call *ssa.CallCommon, f *ssa.Function) bool {
	if f == nil {
		return false
	}
	own := map[ssa.Value]bool{}
	for _, p := range f.Params {
		own[p] = true
	}
	for _, v := range f.FreeVars {
		own[v] = true
	}
	var fromInput func(v ssa.Value, depth int) bool
	fromInput = func(v ssa.Value, depth int) bool {
		if depth > 8 || v == nil {
			return false
		}
		if own[v] {
			return true
		}
		switch x := v.(type) {
		case *ssa.UnOp:
			return fromInput(x.X, depth+1)
		case *ssa.FieldAddr:
			return fromInput(x.X, depth+1)
		case *ssa.Field:
			return fromInput(x.X, depth+1)
		case *ssa.IndexAddr:
			return fromInput(x.X, depth+1)
		case *ssa.Index:
			return fromInput(x.X, depth+1)
		case *ssa.ChangeType:
			return fromInput(x.X, depth+1)
		case *ssa.Convert:
			return fromInput(x.X, depth+1)
		case *ssa.Phi:
			for _, edge := range x.Edges {
				if fromInput(edge, depth+1) {
					return true
				}
			}
		}
		return false
	}
	for _, arg := range call.Args {
		if basic, ok := arg.Type().Underlying().(*types.Basic); ok && basic.Info()&types.IsString != 0 && fromInput(arg, 0) {
			return true
		}
	}
	return false
}

func isWaitGroup(t types.Type) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == "sync" && n.Obj().Name() == "WaitGroup"
}

// sourceHas reports whether a production file of dir under root contains text.
func sourceHas(root, dir, text string) bool {
	files, _ := filepath.Glob(filepath.Join(root, filepath.FromSlash(dir), "*.go"))
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		if data, err := os.ReadFile(f); err == nil && bytes.Contains(data, []byte(text)) {
			return true
		}
	}
	return false
}

func methodOpt(pkg *types.Package, typeName, name string) *types.Func {
	obj := pkg.Scope().Lookup(typeName)
	if obj == nil {
		return nil
	}
	m, _, _ := types.LookupFieldOrMethod(types.NewPointer(obj.Type()), true, pkg, name)
	fn, _ := m.(*types.Func)
	return fn
}

func funcName(d *ast.FuncDecl) string {
	if d.Recv != nil && len(d.Recv.List) > 0 {
		t := d.Recv.List[0].Type
		if s, ok := t.(*ast.StarExpr); ok {
			t = s.X
		}
		if id, ok := t.(*ast.Ident); ok {
			return "(" + id.Name + ")." + d.Name.Name
		}
	}
	return d.Name.Name
}

// sitesIn returns the call expressions under n whose call sites are in set.
func sitesIn(n ast.Node, set map[token.Pos]bool) []*ast.CallExpr {
	var out []*ast.CallExpr
	if n == nil {
		return nil
	}
	ast.Inspect(n, func(x ast.Node) bool {
		if call, ok := x.(*ast.CallExpr); ok && set[call.Lparen] {
			out = append(out, call)
		}
		return true
	})
	return out
}

func hasSite(n ast.Node, set map[token.Pos]bool) bool { return len(sitesIn(n, set)) > 0 }

// rootIdent is the leftmost variable of a selector/index/star chain.
func rootIdent(e ast.Expr) string {
	for {
		switch x := e.(type) {
		case *ast.SelectorExpr:
			e = x.X
		case *ast.IndexExpr:
			e = x.X
		case *ast.StarExpr:
			e = x.X
		case *ast.ParenExpr:
			e = x.X
		case *ast.Ident:
			return x.Name
		default:
			return ""
		}
	}
}

// argKeys names what calls take: each argument expression and its root
// variable; with receivers set, also the root of each call's receiver (a
// wrapper that reads the name from its receiver).
func argKeys(calls []*ast.CallExpr, receivers bool) map[string]bool {
	out := map[string]bool{}
	for _, c := range calls {
		for _, a := range c.Args {
			out[types.ExprString(a)] = true
			if r := rootIdent(a); r != "" && r != "ctx" {
				out["root:"+r] = true
			}
		}
		if sel, ok := c.Fun.(*ast.SelectorExpr); ok && receivers {
			if r := rootIdent(sel.X); r != "" {
				out["root:"+r] = true
			}
		}
	}
	return out
}

func returns(b ast.Stmt) bool {
	switch s := b.(type) {
	case *ast.BlockStmt:
		return len(s.List) > 0 && returns(s.List[len(s.List)-1])
	case *ast.ReturnStmt:
		return true
	case *ast.IfStmt:
		return s.Else != nil && returns(s.Body) && returns(s.Else)
	}
	return false
}

// decides reports whether a probe-guarded if in fn changes whether a deletion
// is reached, for a deletion that takes the probe's name argument.
func decides(fn *ast.FuncDecl, probeSite, deleteSite map[token.Pos]bool) bool {
	type env map[string]map[string]bool // name -> probe argument keys of its latest assignment
	clone := func(e env) env {
		out := make(env, len(e))
		for k, v := range e {
			out[k] = v
		}
		return out
	}
	assign := func(stmt ast.Stmt, e env) {
		as, ok := stmt.(*ast.AssignStmt)
		if !ok {
			return
		}
		var keys map[string]bool
		if probes := sitesIn(as, probeSite); len(probes) > 0 {
			keys = argKeys(probes, false)
		}
		for _, lhs := range as.Lhs {
			if id, ok := lhs.(*ast.Ident); ok && id.Name != "_" {
				e[id.Name] = keys
			}
		}
	}
	guardKeys := func(ifs *ast.IfStmt, e env) map[string]bool {
		keys := argKeys(sitesIn(ifs.Cond, probeSite), false)
		scope := clone(e)
		if ifs.Init != nil {
			for k := range argKeys(sitesIn(ifs.Init, probeSite), false) {
				keys[k] = true
			}
			assign(ifs.Init, scope)
		}
		ast.Inspect(ifs.Cond, func(x ast.Node) bool {
			if id, ok := x.(*ast.Ident); ok {
				for k := range scope[id.Name] {
					keys[k] = true
				}
			}
			return true
		})
		return keys
	}
	branchDrops := func(branch ast.Stmt, after bool) bool {
		if branch == nil {
			return after
		}
		return hasSite(branch, deleteSite) || (!returns(branch) && after)
	}
	deleteKeys := argKeys(sitesIn(fn.Body, deleteSite), true)
	found := false
	var walk func(list []ast.Stmt, e env)
	walk = func(list []ast.Stmt, e env) {
		for i, stmt := range list {
			after := false
			for _, later := range list[i+1:] {
				after = after || hasSite(later, deleteSite)
			}
			for link := stmt; link != nil && !found; {
				ifs, ok := link.(*ast.IfStmt)
				if !ok {
					break
				}
				shared := false
				for k := range guardKeys(ifs, e) {
					shared = shared || deleteKeys[k]
				}
				if shared && branchDrops(ifs.Body, after) != branchDrops(ifs.Else, after) {
					found = true
				}
				link = ifs.Else
			}
			inner := clone(e)
			if ifs, ok := stmt.(*ast.IfStmt); ok && ifs.Init != nil {
				assign(ifs.Init, inner)
			}
			ast.Inspect(stmt, func(x ast.Node) bool {
				switch b := x.(type) {
				case *ast.BlockStmt:
					walk(b.List, clone(inner))
					return false
				case *ast.CaseClause:
					walk(b.Body, clone(inner))
					return false
				case *ast.FuncLit:
					walk(b.Body.List, env{})
					return false
				}
				return true
			})
			assign(stmt, e)
		}
	}
	walk(fn.Body.List, env{})
	return found
}

// ---------------------------------------------------------------------------
// Author: NOR-288 lane (Claude), 2026-10-30, after audit #31. Value flow
// shared by F (decisions on the engine owner's sealed phase) and G (contexts
// in enrolled work), and the field-resolved callbacks of the call graph.
//
// A tracked value is followed through every SSA use: conversions, phis, tuple
// extraction (by result index), comparisons (decisions only), calls (into the
// matching parameter of every resolved callee, keeping the call site, so a
// return goes back to that site; a value arising inside a function returns to
// every caller), closures (free variables), and stores: a local cell's loads,
// a struct field's loads anywhere in the program (sync/atomic loads of the
// field included), a global's loads. Recursion folds the call stack back to
// the repeated site, and a stack deeper than flowDepth drops its oldest site
// (that return then goes to every caller: wider, never narrower). A use
// outside this model, a call resolved only by signature, a callee without a
// body, or exceeding the budget is reported, never assumed harmless.

const flowDepth = 8

// fieldVar is the struct field selected by a FieldAddr or Field on x.
func fieldVar(x ssa.Value, idx int) *types.Var {
	t := x.Type().Underlying()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem().Underlying()
	}
	st, ok := t.(*types.Struct)
	if !ok || idx < 0 || idx >= st.NumFields() {
		return nil
	}
	return st.Field(idx).Origin()
}

// fieldLoadOf is the field v loads, or nil.
func fieldLoadOf(v ssa.Value) *types.Var {
	switch x := v.(type) {
	case *ssa.UnOp:
		if fa, ok := x.X.(*ssa.FieldAddr); ok && x.Op == token.MUL {
			return fieldVar(fa.X, fa.Field)
		}
	case *ssa.Field:
		return fieldVar(x.X, x.Field)
	}
	return nil
}

func pkgPathOf(fn *ssa.Function) string {
	if fn == nil {
		return ""
	}
	if fn.Pkg != nil {
		return fn.Pkg.Pkg.Path()
	}
	if o := fn.Object(); o != nil && o.Pkg() != nil {
		return o.Pkg().Path()
	}
	if o := fn.Origin(); o != nil && o != fn {
		return pkgPathOf(o)
	}
	return ""
}

// storedFields is every struct field the analysed program assigns: a store
// through its address (composite literals included), or a store, map update
// or send of a whole struct value containing it. A function-valued field
// outside this set is never set by the program (only by tests, not loaded).
// Assumption, stated: no field is set through reflect or unsafe.
func storedFields(prog *ssa.Program) map[*types.Var]bool {
	out := map[*types.Var]bool{}
	var markAll func(t types.Type, depth int)
	markAll = func(t types.Type, depth int) {
		st, ok := t.Underlying().(*types.Struct)
		if !ok || depth > 8 {
			return
		}
		for i := 0; i < st.NumFields(); i++ {
			out[st.Field(i).Origin()] = true
			markAll(st.Field(i).Type(), depth+1)
		}
	}
	for fn := range ssautil.AllFunctions(prog) {
		for _, b := range fn.Blocks {
			for _, instr := range b.Instrs {
				switch s := instr.(type) {
				case *ssa.Store:
					if fa, ok := s.Addr.(*ssa.FieldAddr); ok {
						if v := fieldVar(fa.X, fa.Field); v != nil {
							out[v] = true
						}
					}
					markAll(s.Val.Type(), 0)
				case *ssa.MapUpdate:
					markAll(s.Value.Type(), 0)
				case *ssa.Send:
					markAll(s.X.Type(), 0)
				}
			}
		}
	}
	return out
}

type flowEnv struct {
	graph      *graphIndex
	fset       *token.FileSet
	rel        func(string) string
	sink       func(*ssa.Function) bool
	fieldLoads map[*types.Var][]ssa.Value
	globals    map[*ssa.Global][]ssa.Value
	calleesAt  map[ssa.CallInstruction][]*ssa.Function
	closures   map[*ssa.Function][]*ssa.MakeClosure
}

func newFlowEnv(prog *ssa.Program, graph *graphIndex, fset *token.FileSet, rel func(string) string, sink func(*ssa.Function) bool) *flowEnv {
	env := &flowEnv{graph: graph, fset: fset, rel: rel, sink: sink, fieldLoads: map[*types.Var][]ssa.Value{}, globals: map[*ssa.Global][]ssa.Value{},
		calleesAt: map[ssa.CallInstruction][]*ssa.Function{}, closures: map[*ssa.Function][]*ssa.MakeClosure{}}
	for _, edges := range graph.out {
		for _, e := range edges {
			env.calleesAt[e.site] = append(env.calleesAt[e.site], e.callee)
		}
	}
	for fn := range ssautil.AllFunctions(prog) {
		for _, b := range fn.Blocks {
			for _, instr := range b.Instrs {
				switch x := instr.(type) {
				case *ssa.UnOp:
					if v := fieldLoadOf(x); v != nil {
						env.fieldLoads[v] = append(env.fieldLoads[v], x)
					} else if g, ok := x.X.(*ssa.Global); ok && x.Op == token.MUL {
						env.globals[g] = append(env.globals[g], x)
					}
				case *ssa.Field:
					if v := fieldLoadOf(x); v != nil {
						env.fieldLoads[v] = append(env.fieldLoads[v], x)
					}
				case *ssa.Call:
					// A sync/atomic load of a field reads that field.
					if c := x.Call.StaticCallee(); c != nil && pkgPathOf(c) == "sync/atomic" && strings.HasPrefix(c.Name(), "Load") && len(x.Call.Args) > 0 {
						if fa, ok := x.Call.Args[0].(*ssa.FieldAddr); ok {
							if v := fieldVar(fa.X, fa.Field); v != nil {
								env.fieldLoads[v] = append(env.fieldLoads[v], x)
							}
						}
					}
				case *ssa.MakeClosure:
					if f, ok := x.Fn.(*ssa.Function); ok {
						env.closures[f] = append(env.closures[f], x)
					}
				}
			}
		}
	}
	return env
}

func (env *flowEnv) where(i ssa.Instruction) string {
	fn := ""
	if i.Parent() != nil {
		fn = i.Parent().String()
	}
	pos := env.fset.Position(i.Pos())
	if pos.Filename == "" && i.Block() != nil && i.Parent() != nil {
		pos = env.fset.Position(i.Parent().Pos())
	}
	if pos.Filename == "" {
		return " in " + fn
	}
	return " at " + env.rel(pos.Filename) + ":" + strconv.Itoa(pos.Line) + " in " + fn
}

// callees resolves a call site. A load of a function-valued field the program
// never assigns calls nothing; a call resolved only by signature is reported.
func (env *flowEnv) callees(site ssa.CallInstruction) ([]*ssa.Function, string) {
	if env.graph.closed[site] {
		return nil, ""
	}
	if env.graph.fallback[site] {
		return nil, "a call resolved only by signature (an unresolved function value)"
	}
	if out := env.calleesAt[site]; len(out) > 0 {
		return out, ""
	}
	if c := site.Common().StaticCallee(); c != nil {
		return []*ssa.Function{c}, ""
	}
	return nil, ""
}

type flowItem struct {
	v     ssa.Value
	kind  string // "ctx", "done", "dec"; "&"+kind for a cell holding one
	idx   int    // tuple result index, -1 for the whole value
	stack []*ssa.Call
}

type flowWalker struct {
	env    *flowEnv
	seen   map[string]bool
	fields map[string]bool
	queue  []flowItem
	budget int
	hook   func(*flowWalker, flowItem, ssa.Instruction) bool
	stop   string
	// Provenance: each tracked value's first predecessor, so a reported use
	// names the functions its value passed through.
	parent map[ssa.Value]ssa.Value
	cur    ssa.Value
}

// trail names the functions the current value passed through, newest first.
func (w *flowWalker) trail() string {
	var names []string
	for v, n := w.cur, 0; v != nil && n < 64; v, n = w.parent[v], n+1 {
		name := ""
		if i, ok := v.(ssa.Instruction); ok && i.Parent() != nil {
			name = i.Parent().String()
		} else if p, ok := v.(*ssa.Parameter); ok {
			name = p.Parent().String()
		} else if f, ok := v.(*ssa.FreeVar); ok {
			name = f.Parent().String()
		}
		if name != "" && (len(names) == 0 || names[len(names)-1] != name) {
			names = append(names, name)
		}
	}
	if len(names) > 16 {
		names = append(names[:16], "...")
	}
	return strings.Join(names, " <- ")
}

func newFlowWalker(env *flowEnv, budget int, hook func(*flowWalker, flowItem, ssa.Instruction) bool) *flowWalker {
	return &flowWalker{env: env, seen: map[string]bool{}, fields: map[string]bool{}, budget: budget, hook: hook, parent: map[ssa.Value]ssa.Value{}}
}

func (w *flowWalker) push(v ssa.Value, kind string, idx int, stack []*ssa.Call) {
	if v == nil || w.stop != "" {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%p|%s|%d", v, kind, idx)
	for _, s := range stack {
		fmt.Fprintf(&b, "|%p", s)
	}
	if k := b.String(); !w.seen[k] {
		w.seen[k] = true
		if _, known := w.parent[v]; !known && v != w.cur {
			w.parent[v] = w.cur
		}
		w.queue = append(w.queue, flowItem{v, kind, idx, stack})
	}
}

func (w *flowWalker) fail(reason string, at ssa.Instruction) {
	if w.stop == "" {
		w.stop = reason
		if at != nil {
			w.stop += w.env.where(at)
		}
		if t := w.trail(); t != "" {
			w.stop += "; value path: " + t
		}
	}
}

func (w *flowWalker) pushField(v *types.Var, kind string) {
	key := fmt.Sprintf("%p|%s", v, kind)
	if w.fields[key] {
		return
	}
	w.fields[key] = true
	for _, l := range w.env.fieldLoads[v] {
		w.push(l, kind, -1, nil)
	}
}

func (w *flowWalker) run() string {
	for len(w.queue) > 0 && w.stop == "" {
		if w.budget--; w.budget < 0 {
			w.stop = "the value flow exceeds the analysis budget (unproven)"
			break
		}
		it := w.queue[0]
		w.queue = w.queue[1:]
		w.cur = it.v
		refs := it.v.Referrers()
		if refs == nil {
			continue
		}
		for _, use := range *refs {
			if w.stop != "" {
				break
			}
			if w.hook != nil && w.hook(w, it, use) {
				continue
			}
			w.generic(it, use)
		}
	}
	return w.stop
}

func (w *flowWalker) generic(it flowItem, use ssa.Instruction) {
	inner, isCell := strings.CutPrefix(it.kind, "&")
	switch u := use.(type) {
	case *ssa.DebugRef, *ssa.If, *ssa.Panic:
	case *ssa.Phi:
		w.push(u, it.kind, it.idx, it.stack)
	case *ssa.Extract:
		if it.idx < 0 || it.idx == u.Index {
			w.push(u, it.kind, -1, it.stack)
		}
	case *ssa.ChangeType, *ssa.Convert, *ssa.ChangeInterface, *ssa.MakeInterface, *ssa.TypeAssert, *ssa.MultiConvert, *ssa.SliceToArrayPointer:
		w.push(u.(ssa.Value), it.kind, -1, it.stack)
	case *ssa.UnOp:
		switch {
		case isCell && u.Op == token.MUL:
			w.push(u, inner, -1, it.stack)
		case it.kind == "dec" && u.Op == token.NOT:
			w.push(u, it.kind, -1, it.stack)
		default:
			w.fail("unmodelled "+u.Op.String()+" of the value", use)
		}
	case *ssa.BinOp:
		if it.kind == "dec" {
			w.push(u, it.kind, -1, it.stack)
		}
	case *ssa.Store:
		if u.Val == it.v {
			w.storeTo(u.Addr, it.kind, it.stack, use)
		}
	case ssa.CallInstruction:
		w.call(it, u)
	case *ssa.Return:
		for i, r := range u.Results {
			if r == it.v {
				idx := -1
				if len(u.Results) > 1 {
					idx = i
				}
				w.returnFrom(u.Parent(), it.kind, idx, it.stack)
			}
		}
	case *ssa.MakeClosure:
		if fn, ok := u.Fn.(*ssa.Function); ok {
			for i, b := range u.Bindings {
				if b == it.v && i < len(fn.FreeVars) {
					w.push(fn.FreeVars[i], it.kind, -1, nil)
				}
			}
		}
	case *ssa.Send:
		w.fail("sent on a channel", use)
	case *ssa.MapUpdate:
		w.fail("stored in a map", use)
	default:
		w.fail(fmt.Sprintf("unmodelled %T use", use), use)
	}
}

func (w *flowWalker) storeTo(addr ssa.Value, kind string, stack []*ssa.Call, at ssa.Instruction) {
	switch a := addr.(type) {
	case *ssa.Alloc:
		w.push(a, "&"+kind, -1, stack)
	case *ssa.FieldAddr:
		if v := fieldVar(a.X, a.Field); v != nil {
			w.pushField(v, kind)
			return
		}
		w.fail("stored into an unresolved field", at)
	case *ssa.Global:
		for _, l := range w.env.globals[a] {
			w.push(l, kind, -1, nil)
		}
	case *ssa.FreeVar:
		fn, idx := a.Parent(), -1
		for i, f := range fn.FreeVars {
			if f == a {
				idx = i
			}
		}
		for _, mc := range w.env.closures[fn] {
			if idx >= 0 && idx < len(mc.Bindings) {
				w.storeTo(mc.Bindings[idx], kind, nil, at)
			}
		}
	case *ssa.IndexAddr:
		w.fail("stored in a slice or array element", at)
	default:
		w.fail(fmt.Sprintf("stored through %T", addr), at)
	}
}

func (w *flowWalker) returnFrom(fn *ssa.Function, kind string, idx int, stack []*ssa.Call) {
	if len(stack) > 0 {
		w.push(stack[len(stack)-1], kind, idx, stack[:len(stack)-1])
		return
	}
	for _, e := range w.env.graph.in[fn] {
		if c, ok := e.site.(*ssa.Call); ok {
			w.push(c, kind, idx, nil)
		}
	}
}

func (w *flowWalker) call(it flowItem, u ssa.CallInstruction) {
	cc := u.Common()
	if !cc.IsInvoke() && cc.Value == it.v {
		w.fail("called as a function value", u)
		return
	}
	if b, ok := cc.Value.(*ssa.Builtin); ok {
		switch b.Name() {
		case "len", "cap", "print", "println":
		default:
			w.fail("passed to builtin "+b.Name(), u)
		}
		return
	}
	// A sync/atomic store, swap or compare-and-swap into a field records the
	// value in that field.
	if c := cc.StaticCallee(); c != nil && pkgPathOf(c) == "sync/atomic" && len(cc.Args) > 1 && cc.Args[0] != it.v {
		if n := c.Name(); strings.HasPrefix(n, "Store") || strings.HasPrefix(n, "Swap") || strings.HasPrefix(n, "CompareAndSwap") {
			if fa, ok := cc.Args[0].(*ssa.FieldAddr); ok {
				if v := fieldVar(fa.X, fa.Field); v != nil {
					w.pushField(v, it.kind)
					return
				}
			}
		}
	}
	callees, why := w.env.callees(u)
	if why != "" {
		w.fail("passed to "+why, u)
		return
	}
	var stack []*ssa.Call
	if c, ok := u.(*ssa.Call); ok {
		stack = append([]*ssa.Call{}, it.stack...)
		for i, s := range stack {
			if s == c { // recursion: the repeated site is the same frame
				stack = stack[:i]
				break
			}
		}
		if len(stack) >= flowDepth {
			stack = stack[1:]
		}
		stack = append(stack, c)
	}
	var positions []int
	if cc.IsInvoke() && cc.Value == it.v {
		positions = append(positions, 0)
	}
	for i, a := range cc.Args {
		if a == it.v {
			if cc.IsInvoke() {
				positions = append(positions, i+1)
			} else {
				positions = append(positions, i)
			}
		}
	}
	for _, callee := range callees {
		if w.env.sink(callee) {
			continue
		}
		if len(callee.Blocks) == 0 {
			w.fail("passed to "+callee.String()+", whose body is not available", u)
			return
		}
		for _, p := range positions {
			if p >= len(callee.Params) {
				w.fail("passed beyond the parameters of "+callee.String(), u)
				return
			}
			w.push(callee.Params[p], it.kind, -1, stack)
		}
	}
}

// postIdoms is each block's immediate post-dominator; nil means the exit (or
// a block that never reaches the exit, whose region is then unbounded).
func postIdoms(fn *ssa.Function) map[*ssa.BasicBlock]*ssa.BasicBlock {
	n := len(fn.Blocks)
	exit := n
	// Reverse CFG rooted at a virtual exit: its successors are the blocks
	// without successors; a block's reverse successors are its predecessors.
	rsucc := func(i int) []int {
		var out []int
		if i == exit {
			for _, b := range fn.Blocks {
				if len(b.Succs) == 0 {
					out = append(out, b.Index)
				}
			}
			return out
		}
		for _, p := range fn.Blocks[i].Preds {
			out = append(out, p.Index)
		}
		return out
	}
	rpred := func(i int) []int {
		if i == exit {
			return nil
		}
		var out []int
		if len(fn.Blocks[i].Succs) == 0 {
			out = append(out, exit)
		}
		for _, s := range fn.Blocks[i].Succs {
			out = append(out, s.Index)
		}
		return out
	}
	order := make([]int, n+1)
	for i := range order {
		order[i] = -1
	}
	var post []int
	visited := make([]bool, n+1)
	var dfs func(int)
	dfs = func(i int) {
		visited[i] = true
		for _, s := range rsucc(i) {
			if !visited[s] {
				dfs(s)
			}
		}
		order[i] = len(post)
		post = append(post, i)
	}
	dfs(exit)
	idom := make([]int, n+1)
	for i := range idom {
		idom[i] = -1
	}
	idom[exit] = exit
	intersect := func(a, b int) int {
		for a != b {
			for order[a] < order[b] {
				a = idom[a]
			}
			for order[b] < order[a] {
				b = idom[b]
			}
		}
		return a
	}
	for changed := true; changed; {
		changed = false
		for k := len(post) - 1; k >= 0; k-- {
			i := post[k]
			if i == exit {
				continue
			}
			nd := -1
			for _, p := range rpred(i) {
				if order[p] < 0 || idom[p] < 0 {
					continue
				}
				if nd < 0 {
					nd = p
				} else {
					nd = intersect(p, nd)
				}
			}
			if nd >= 0 && idom[i] != nd {
				idom[i] = nd
				changed = true
			}
		}
	}
	out := map[*ssa.BasicBlock]*ssa.BasicBlock{}
	for _, b := range fn.Blocks {
		if d := idom[b.Index]; d >= 0 && d != exit {
			out[b] = fn.Blocks[d]
		}
	}
	return out
}

// regionOf is the blocks reachable from start before the join stop (nil: no
// join, every reachable block).
func regionOf(start, stop *ssa.BasicBlock) map[*ssa.BasicBlock]bool {
	seen := map[*ssa.BasicBlock]bool{}
	stack := []*ssa.BasicBlock{start}
	for len(stack) > 0 {
		b := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if b == nil || b == stop || seen[b] {
			continue
		}
		seen[b] = true
		stack = append(stack, b.Succs...)
	}
	return seen
}

// shortFunc names a function relative to its package, without pointer stars.
func shortFunc(fn *ssa.Function) string {
	s := fn.String()
	if fn.Pkg != nil {
		s = fn.RelString(fn.Pkg.Pkg)
	}
	return strings.ReplaceAll(s, "(*", "(")
}

// closeWriterless (decision 3, second rule, after the 483d census) closes a
// call resolved only by signature whose function value is read from a
// container (a struct field, a global, or a sync/atomic value held in one)
// when no writer of that container is reachable: no function storing into it
// (directly, by a whole-struct store, by a sync/atomic store/swap, or through
// a pointer of its type the census cannot attribute) has a caller through a
// resolved call, and none is a program entry (main, init). Such a container is
// set only by tests, as the unique-lock wait hook. Stated assumption: VTA
// resolves every call that can reach a writer; exported functions nothing in
// the module calls are not invoked in production.
func closeWriterless(prog *ssa.Program, g *graphIndex) {
	type container struct {
		f  *types.Var
		gl *ssa.Global
	}
	writers := map[container][]*ssa.Function{}
	typeWriters := map[string][]*ssa.Function{}
	addr := func(a ssa.Value) (container, bool) {
		switch x := a.(type) {
		case *ssa.FieldAddr:
			if v := fieldVar(x.X, x.Field); v != nil {
				return container{f: v}, true
			}
		case *ssa.Global:
			return container{gl: x}, true
		}
		return container{}, false
	}
	elem := func(a ssa.Value) string {
		if p, ok := a.Type().Underlying().(*types.Pointer); ok {
			return types.TypeString(p.Elem(), nil)
		}
		return types.TypeString(a.Type(), nil)
	}
	var markStruct func(t types.Type, fn *ssa.Function, depth int)
	markStruct = func(t types.Type, fn *ssa.Function, depth int) {
		st, ok := t.Underlying().(*types.Struct)
		if !ok || depth > 8 {
			return
		}
		for i := 0; i < st.NumFields(); i++ {
			k := container{f: st.Field(i).Origin()}
			writers[k] = append(writers[k], fn)
			markStruct(st.Field(i).Type(), fn, depth+1)
		}
	}
	write := func(a ssa.Value, fn *ssa.Function) {
		if k, ok := addr(a); ok {
			writers[k] = append(writers[k], fn)
			return
		}
		typeWriters[elem(a)] = append(typeWriters[elem(a)], fn)
	}
	for fn := range ssautil.AllFunctions(prog) {
		for _, b := range fn.Blocks {
			for _, instr := range b.Instrs {
				switch x := instr.(type) {
				case *ssa.Store:
					write(x.Addr, fn)
					markStruct(x.Val.Type(), fn, 0)
				case *ssa.MapUpdate:
					markStruct(x.Value.Type(), fn, 0)
				case *ssa.Send:
					markStruct(x.X.Type(), fn, 0)
				case ssa.CallInstruction:
					cc := x.Common()
					if c := cc.StaticCallee(); c != nil && pkgPathOf(c) == "sync/atomic" && len(cc.Args) > 1 {
						if n := c.Name(); strings.HasPrefix(n, "Store") || strings.HasPrefix(n, "Swap") || strings.HasPrefix(n, "CompareAndSwap") {
							write(cc.Args[0], fn)
						}
					}
				}
			}
		}
	}
	reachable := func(fn *ssa.Function) bool {
		if n := fn.Name(); n == "main" || n == "init" || strings.HasPrefix(n, "init#") {
			return true
		}
		for _, e := range g.in[fn] {
			if !g.fallback[e.site] {
				return true
			}
		}
		return false
	}
	source := func(v ssa.Value) (container, string, bool) {
		if f, ok := v.(*ssa.Field); ok {
			if fv := fieldVar(f.X, f.Field); fv != nil {
				return container{f: fv}, types.TypeString(f.Type(), nil), true
			}
		}
		u, ok := v.(*ssa.UnOp)
		if !ok || u.Op != token.MUL {
			return container{}, "", false
		}
		switch x := u.X.(type) {
		case *ssa.FieldAddr, *ssa.Global:
			k, ok := addr(x)
			return k, elem(x), ok
		case *ssa.Call:
			if c := x.Call.StaticCallee(); c != nil && pkgPathOf(c) == "sync/atomic" && strings.HasPrefix(c.Name(), "Load") && len(x.Call.Args) > 0 {
				k, ok := addr(x.Call.Args[0])
				return k, elem(x.Call.Args[0]), ok
			}
		}
		return container{}, "", false
	}
	closing := map[ssa.CallInstruction]bool{}
	for site := range g.fallback {
		k, t, ok := source(site.Common().Value)
		if !ok {
			continue
		}
		open := false
		for _, w := range append(append([]*ssa.Function{}, writers[k]...), typeWriters[t]...) {
			open = open || reachable(w)
		}
		if !open {
			closing[site] = true
		}
	}
	if len(closing) == 0 {
		return
	}
	keep := func(edges []edge) []edge {
		out := edges[:0:0]
		for _, e := range edges {
			if !closing[e.site] {
				out = append(out, e)
			}
		}
		return out
	}
	for f := range g.out {
		g.out[f] = keep(g.out[f])
	}
	for f := range g.in {
		g.in[f] = keep(g.in[f])
	}
	for site := range closing {
		delete(g.fallback, site)
		delete(g.sites, site)
		g.closed[site] = true
		g.writerlessSites++
		g.fallbackSites--
	}
}
