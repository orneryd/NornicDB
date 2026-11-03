package workcensus

import (
	"go/types"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

// The select-receive fixtures reproduce, generically, the shape of an ingress
// work queue whose element carries an admitted context: a channel held in a
// private field of a private receiver, created at package initialization and
// recreated per generation, copied into a local cell and captured by the
// consumer closure, with one send under an accepted guard and a two-state
// blocking select that also watched a stop signal.
//
// The question at every use is the same: is the element field the constructor's
// checked success result, and not an unchosen receive slot or a zero element
// from a closed channel? The projection may answer that only from the
// selected-state edge of the exact select plus a closed channel population.
// Nothing here names a production function, package, field or type.

const selectReceiveBase = `package fixture

import "context"

type selectDelivery struct {
	event   *int
	release func()
}

type selectWork struct {
	selectDelivery
	ctx context.Context
}

type selectDispatcher struct {
	running bool
	events  chan selectWork
}

var liveSelectDispatcher = &selectDispatcher{events: make(chan selectWork, 8)}

func (d *selectDispatcher) start() {
	if d.running {
		return
	}
	d.running = true
	d.events = make(chan selectWork, 8)
	events := d.events
	stopped := make(chan struct{})
	go func() {
		for {
			select {
			case work := <-events:
				deliverSelectWork(work)
			case <-stopped:
				return
			}
		}
	}()
}

func (d *selectDispatcher) drain() {
	events := d.events
	for {
		select {
		case work := <-events:
			if work.release != nil {
				work.release()
			}
		default:
			return
		}
	}
}

func deliverSelectWork(work selectWork) {
	consumeSelectContext(work.ctx)
}

func consumeSelectContext(ctx context.Context) {
	_ = ctx.Err()
}

func drainedSelectWork() {
	events := liveSelectDispatcher.events
	work := <-events
	consumeDrainedSelectWork(work)
}

func drainedSelectWorkOk() {
	events := liveSelectDispatcher.events
	work, ok := <-events
	if !ok {
		return
	}
	consumeDrainedSelectWork(work)
}

func consumeDrainedSelectWork(work selectWork) {
	_ = work.ctx.Err()
}

type selectWorkKey struct{}

func acquireSelectWork(ctx context.Context) (context.Context, bool) {
	return context.WithValue(ctx, selectWorkKey{}, 1), true
}

func EmitSelectWork(event *int) {
	d := liveSelectDispatcher
	if !d.running {
		return
	}
	ctx, accepted := acquireSelectWork(context.Background())
	if !accepted {
		return
	}
	select {
	case d.events <- selectWork{selectDelivery: selectDelivery{event: event}, ctx: ctx}:
	default:
	}
}
`

// selectReceiveProducers are the positive shapes that must resolve: several
// receive states of one exact element type with every producer carrying an
// admitted context, and a context field nested inside another inline struct.
var selectReceiveProducers = []struct {
	name    string
	elem    string
	consume string
	// count is the number of consumer calls the fixture must answer for.
	count int
	plant string
}{
	{
		name:    "multiple_receive_states",
		elem:    "pairWork",
		consume: "consumePairWork",
		count:   2,
		plant: `
type pairWork struct {
	ctx context.Context
}

var firstPairWork = make(chan pairWork, 4)
var secondPairWork = make(chan pairWork, 4)

func emitFirstPairWork() {
	ctx, accepted := acquireSelectWork(context.Background())
	if !accepted {
		return
	}
	firstPairWork <- pairWork{ctx: ctx}
}

func emitSecondPairWork() {
	ctx, accepted := acquireSelectWork(context.Background())
	if !accepted {
		return
	}
	secondPairWork <- pairWork{ctx: ctx}
}

func receivePairWork() {
	stop := make(chan struct{})
	select {
	case work := <-firstPairWork:
		consumePairWork(work)
	case work := <-secondPairWork:
		consumePairWork(work)
	case <-stop:
	}
}

func consumePairWork(work pairWork) {
	_ = work.ctx.Err()
}
`,
	},
	{
		name:    "nested_inline_field",
		elem:    "nestedWork",
		consume: "consumeNestedWork",
		count:   1,
		plant: `
type nestedInner struct {
	ctx context.Context
}

type nestedWork struct {
	inner nestedInner
	label string
}

var nestedWorkChannel = make(chan nestedWork, 4)

func emitNestedWork() {
	ctx, accepted := acquireSelectWork(context.Background())
	if !accepted {
		return
	}
	nestedWorkChannel <- nestedWork{inner: nestedInner{ctx: ctx}, label: "nested"}
}

func receiveNestedWork() {
	for {
		select {
		case work := <-nestedWorkChannel:
			consumeNestedWork(work)
		default:
			return
		}
	}
}

func consumeNestedWork(work nestedWork) {
	_ = work.inner.ctx.Err()
}
`,
	},
}

// selectReceiveNegatives are the source shapes that must keep their unknown or
// zero answer even though each carries the same positive control. Every plant
// declares its own element type, so its channel population is a separate family
// and cannot be confused with the control's.
var selectReceiveNegatives = []struct {
	name    string
	elem    string
	plant   string
	consume string
	// unknown is true when the population (not a producer) fails to close, so
	// the projection must keep its unknown answer; false when the population is
	// closed and an enumerated producer's own zero field must stay a witness.
	unknown bool
	witness string
}{
	{
		name: "closed_channel",
		elem: "closedWork",
		plant: `
type closedWork struct {
	ctx context.Context
}

var closedWorkChannel = make(chan closedWork, 4)

func emitClosedWork(ctx context.Context) {
	closedWorkChannel <- closedWork{ctx: ctx}
}

func closeClosedWork() {
	close(closedWorkChannel)
}

func receiveClosedWork() {
	for {
		select {
		case work := <-closedWorkChannel:
			consumeClosedWork(work)
		default:
			return
		}
	}
}

func consumeClosedWork(work closedWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeClosedWork",
		unknown: true,
		witness: "a channel passed to a call whose body is not loaded source",
	},
	{
		name: "unknown_external_channel",
		elem: "externalWork",
		plant: `
type externalWork struct {
	ctx context.Context
}

var externalWorkChannel func() chan externalWork

func receiveExternalWork() {
	ch := externalWorkChannel()
	select {
	case work := <-ch:
		consumeExternalWork(work)
	default:
	}
}

func consumeExternalWork(work externalWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeExternalWork",
		unknown: true,
		witness: "an unmodelled definition",
	},
	{
		name: "channel_parameter",
		elem: "paramWork",
		plant: `
type paramWork struct {
	ctx context.Context
}

func receiveParamWork(ch chan paramWork) {
	select {
	case work := <-ch:
		consumeParamWork(work)
	default:
	}
}

func emitParamWork(ch chan paramWork, ctx context.Context) {
	ch <- paramWork{ctx: ctx}
}

func consumeParamWork(work paramWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeParamWork",
		unknown: true,
		witness: "a channel value held by a parameter",
	},
	{
		name: "interface_escape",
		elem: "escapeWork",
		plant: `
type escapeWork struct {
	ctx context.Context
}

var escapeWorkChannel = make(chan escapeWork, 4)

var escapeWorkSink any

func emitEscapeWork(ctx context.Context) {
	escapeWorkChannel <- escapeWork{ctx: ctx}
}

func aliasEscapeWork() {
	escapeWorkSink = escapeWorkChannel
}

func receiveEscapeWork() {
	for {
		select {
		case work := <-escapeWorkChannel:
			consumeEscapeWork(work)
		default:
			return
		}
	}
}

func consumeEscapeWork(work escapeWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeEscapeWork",
		unknown: true,
		witness: "a channel value converted to or from an interface",
	},
	{
		name: "foreign_callee_escape",
		elem: "tapWork",
		plant: `
type tapWork struct {
	ctx context.Context
}

var tapWorkChannel = make(chan tapWork, 4)

var externalWorkTap func(chan tapWork)

func emitTapWork(ctx context.Context) {
	tapWorkChannel <- tapWork{ctx: ctx}
}

func aliasTapWork() {
	externalWorkTap(tapWorkChannel)
}

func receiveTapWork() {
	for {
		select {
		case work := <-tapWorkChannel:
			consumeTapWork(work)
		default:
			return
		}
	}
}

func consumeTapWork(work tapWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeTapWork",
		unknown: true,
		witness: "a channel passed to a call whose body is not loaded source",
	},
	{
		name: "reassigned_from_unknown_source",
		elem: "reassignWork",
		plant: `
type reassignWork struct {
	ctx context.Context
}

var reassignWorkChannel = make(chan reassignWork, 4)

var externalReassign func() chan reassignWork

func emitReassignWork(ctx context.Context) {
	reassignWorkChannel <- reassignWork{ctx: ctx}
}

func reassignWorkQueue() {
	reassignWorkChannel = externalReassign()
}

func receiveReassignWork() {
	for {
		select {
		case work := <-reassignWorkChannel:
			consumeReassignWork(work)
		default:
			return
		}
	}
}

func consumeReassignWork(work reassignWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeReassignWork",
		unknown: true,
		witness: "an unmodelled definition",
	},
	{
		name: "no_producer",
		elem: "deadWork",
		plant: `
type deadWork struct {
	ctx context.Context
}

var deadWorkChannel = make(chan deadWork, 4)

func receiveDeadWork() {
	for {
		select {
		case work := <-deadWorkChannel:
			consumeDeadWork(work)
		default:
			return
		}
	}
}

func consumeDeadWork(work deadWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeDeadWork",
		unknown: true,
		witness: "no enumerated send populates a channel of",
	},
	{
		name: "unselected_slot",
		elem: "unselectedWork",
		plant: `
type unselectedWork struct {
	ctx context.Context
}

var unselectedWorkChannel = make(chan unselectedWork, 4)

func emitUnselectedWork(ctx context.Context) {
	unselectedWorkChannel <- unselectedWork{ctx: ctx}
}

func receiveUnselectedWork() {
	var held unselectedWork
	select {
	case held = <-unselectedWorkChannel:
	default:
	}
	consumeUnselectedWork(held)
}

func consumeUnselectedWork(work unselectedWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeUnselectedWork",
		unknown: true,
		witness: "may not be the chosen case at this use",
	},
	{
		name: "zero_send_in_family",
		elem: "mixedWork",
		plant: `
type mixedWork struct {
	ctx context.Context
}

var mixedWorkGood = make(chan mixedWork, 4)
var mixedWorkZero = make(chan mixedWork, 4)

func emitMixedWork(ctx context.Context) {
	mixedWorkGood <- mixedWork{ctx: ctx}
}

func emitZeroMixedWork() {
	mixedWorkZero <- mixedWork{}
}

func receiveMixedWork() {
	for {
		select {
		case work := <-mixedWorkGood:
			consumeMixedWork(work)
		default:
			return
		}
	}
}

func consumeMixedWork(work mixedWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeMixedWork",
		unknown: false,
		witness: "zero field ctx of census.fixture.mixedWork",
	},
	{
		name: "nil_send_in_family",
		elem: "nilWork",
		plant: `
type nilWork struct {
	ctx context.Context
}

var nilWorkGood = make(chan nilWork, 4)
var nilWorkNil = make(chan nilWork, 4)

func emitNilWork(ctx context.Context) {
	nilWorkGood <- nilWork{ctx: ctx}
}

func emitNilMixedWork() {
	nilWorkNil <- nilWork{ctx: nil}
}

func receiveNilWork() {
	for {
		select {
		case work := <-nilWorkGood:
			consumeNilWork(work)
		default:
			return
		}
	}
}

func consumeNilWork(work nilWork) {
	_ = work.ctx.Err()
}
`,
		consume: "consumeNilWork",
		unknown: false,
		witness: "nil context.Context",
	},
}

func loadSelectReceive(t *testing.T, name, fixture string) *Program {
	t.Helper()
	return loadEscapeProgram(t, name, selectReceiveBase+fixture)
}

// selectContextPaths are the inline index paths of every context-typed leaf of
// the fixture element, from the engine's own inline enumeration.
func selectContextPaths(t *testing.T, p *Program, elem string) [][]int {
	t.Helper()
	paths := InlineFieldPaths(namedType(t, p, elem), isContext)
	if len(paths) == 0 {
		t.Fatalf("%s has no inline context field", elem)
	}
	return paths
}

// isContext is the leaf predicate the census itself uses for context fields.
func isContext(t types.Type) bool {
	return types.Unalias(t).String() == "context.Context"
}

// selectReceiveProjections are the projections of the element's context fields
// at the fixture's own consumer calls, which is the shape a census attributes
// to the receive case.
func selectReceiveProjections(t *testing.T, p *Program, elem, consume string) []FieldProjection {
	t.Helper()
	element := namedType(t, p, elem)
	paths := selectContextPaths(t, p, elem)
	var projections []FieldProjection
	for _, fn := range p.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" || fn.Blocks == nil {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				call, ok := instruction.(ssa.CallInstruction)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != consume {
					continue
				}
				for _, argument := range call.Common().Args {
					if argument == nil || argument.Type() == nil || !types.Identical(argument.Type(), element) {
						continue
					}
					for _, path := range paths {
						projections = append(projections, p.ProjectFieldAt(argument, path, call))
					}
				}
			}
		}
	}
	if len(projections) == 0 {
		t.Fatalf("fixture consumer %s of %s was not located", consume, elem)
	}
	return projections
}

// selectReceiveProjection is the single context-field projection of a fixture
// element with one context leaf.
func selectReceiveProjection(t *testing.T, p *Program, elem, consume string) FieldProjection {
	t.Helper()
	projections := selectReceiveProjections(t, p, elem, consume)
	if len(projections) != 1 {
		t.Fatalf("%s has %d context-field projections, want 1", elem, len(projections))
	}
	return projections[0]
}

// TestSelectReceiveProjectionFollowsClosedChannelPopulation is the product
// outcome: with one channel family of an exact private element type, every
// creation enumerated and one send under an accepted guard, the receive case's
// context field is the sent value's own field - proven non-nil by the existing
// inline projection and its result/guard rules, with no new acceptance path.
func TestSelectReceiveProjectionFollowsClosedChannelPopulation(t *testing.T) {
	p := loadSelectReceive(t, "select_receive_base.go", "")
	projection := selectReceiveProjection(t, p, "selectWork", "deliverSelectWork")
	t.Logf("receive case witness=%v why=%v values=%v unknown=%v", projection.Witness, projection.Why, projection.Values, projection.Unknown)
	if projection.Unknown {
		t.Errorf("the closed channel family was not resolved: %v", projection.Witness)
	}
	if projection.Witness != nil {
		t.Errorf("the receive case kept a nil witness: %v", projection.Witness)
	}
	if len(projection.Values) == 0 {
		t.Error("the receive case retained no sent value, so no provenance was projected")
	}
	if why := strings.Join(projection.Why, " "); !strings.Contains(why, "selected receive case with a closed channel population") {
		t.Errorf("the non-nil verdict did not record the selected-case and channel-population proof: %v", projection.Why)
	}
	// The downstream read of the by-value element follows the same case.
	witness, why := selectConsumeWitness(t, p, "deliverSelectWork")
	t.Logf("downstream read witness=%v why=%v", witness, why)
	if witness != nil {
		t.Errorf("the consumer's read of the element context kept a nil witness: %v", witness)
	}
}

// selectConsumeWitness is the nil verdict at the fixture consumer's own read of
// the element's context field, the site a census attributes to the receive.
func selectConsumeWitness(t *testing.T, p *Program, function string) (witness, why []string) {
	t.Helper()
	for _, fn := range p.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" || fn.Name() != function || fn.Blocks == nil {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				call, ok := instruction.(ssa.CallInstruction)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "consumeSelectContext" {
					continue
				}
				for _, argument := range call.Common().Args {
					if argument.Type().String() == "context.Context" {
						return p.MayBeNilWhy(argument, call)
					}
				}
			}
		}
	}
	t.Fatalf("fixture consumer %s read was not located", function)
	return nil, nil
}

// TestSelectReceiveProjectionKnownProducers covers the closed shapes the
// population accepts beyond one case: several receive states of one exact
// element type, each with its own admitted producer, and a context field nested
// inside another inline struct. Each non-nil verdict must rest on the sent
// value's own source-proven result, not on a convention.
func TestSelectReceiveProjectionKnownProducers(t *testing.T) {
	for _, producer := range selectReceiveProducers {
		t.Run(producer.name, func(t *testing.T) {
			p := loadSelectReceive(t, "select_receive_"+producer.name+".go", producer.plant)
			projections := selectReceiveProjections(t, p, producer.elem, producer.consume)
			if len(projections) != producer.count {
				t.Fatalf("%s has %d projections, want %d", producer.elem, len(projections), producer.count)
			}
			for index, projection := range projections {
				t.Logf("%s case %d witness=%v why=%v values=%v", producer.elem, index, projection.Witness, projection.Why, projection.Values)
				if projection.Unknown || projection.Witness != nil {
					t.Errorf("%s case %d kept an unproved answer: unknown=%v witness=%v", producer.elem, index, projection.Unknown, projection.Witness)
				}
				if len(projection.Values) == 0 {
					t.Errorf("%s case %d retained no sent value", producer.elem, index)
				}
				why := strings.Join(projection.Why, " ")
				if !strings.Contains(why, "selected receive case with a closed channel population") {
					t.Errorf("%s case %d did not record the selected-case proof: %v", producer.elem, index, projection.Why)
				}
				if !strings.Contains(why, "context-package result context.WithValue") {
					t.Errorf("%s case %d did not validate the sent field through the existing result proof: %v", producer.elem, index, projection.Why)
				}
			}
		})
	}
}

// TestSelectReceiveProjectionNegatives keeps every unproven population visible.
// Each plant carries the same positive control, so a plant cannot pass by
// answering unknown everywhere, and each plant's own answer must keep the exact
// reason it was not accepted.
func TestSelectReceiveProjectionNegatives(t *testing.T) {
	for _, plant := range selectReceiveNegatives {
		t.Run(plant.name, func(t *testing.T) {
			p := loadSelectReceive(t, "select_receive_"+plant.name+".go", plant.plant)
			control := selectReceiveProjection(t, p, "selectWork", "deliverSelectWork")
			if control.Unknown || control.Witness != nil {
				t.Fatalf("positive control lost its proof: unknown=%v witness=%v", control.Unknown, control.Witness)
			}
			projection := selectReceiveProjection(t, p, plant.elem, plant.consume)
			t.Logf("%s unknown=%v witness=%v why=%v", plant.elem, projection.Unknown, projection.Witness, projection.Why)
			reason := strings.Join(projection.Witness, " ")
			if plant.unknown && !projection.Unknown {
				t.Errorf("%s: an unproven channel population was resolved", plant.name)
			}
			if !plant.unknown && projection.Unknown {
				t.Errorf("%s: a closed population with a zero producer stayed unknown instead of witnessed: %v", plant.name, projection.Witness)
			}
			if !strings.Contains(reason, plant.witness) {
				t.Errorf("%s: the answer did not keep its reason %q: %v", plant.name, plant.witness, projection.Witness)
			}
		})
	}
}

// TestSelectReceiveProjectionKeepsUnmodelledRepresentations keeps the adjacent
// representations unknown: a plain receive operator, a comma-ok receive and a
// select index component are not receive value slots, so the same closed
// population must not project a field through them.
func TestSelectReceiveProjectionKeepsUnmodelledRepresentations(t *testing.T) {
	p := loadSelectReceive(t, "select_receive_representations.go", "")
	element := namedType(t, p, "selectWork")
	paths := selectContextPaths(t, p, "selectWork")
	receives, indexes := 0, 0
	for _, fn := range p.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" || fn.Blocks == nil {
			continue
		}
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				switch instruction := instruction.(type) {
				case *ssa.UnOp:
					if instruction.Op.String() != "<-" || !types.Identical(instruction.Type(), element) {
						continue
					}
					receives++
					for _, path := range paths {
						projection := p.ProjectFieldAt(instruction, path, instruction)
						t.Logf("plain receive %s unknown=%v witness=%v", fn.String(), projection.Unknown, projection.Witness)
						if projection.Witness == nil && !projection.Unknown {
							t.Errorf("%s: a plain receive of %s was projected", fn.String(), element)
						}
					}
				case *ssa.Extract:
					if instruction.Index != 0 {
						continue
					}
					if _, ok := instruction.Tuple.(*ssa.Select); !ok {
						continue
					}
					indexes++
					for _, path := range paths {
						projection := p.ProjectFieldAt(instruction, path, instruction)
						t.Logf("select index %s unknown=%v witness=%v", fn.String(), projection.Unknown, projection.Witness)
						if projection.Witness == nil && !projection.Unknown {
							t.Errorf("%s: a select index component was projected", fn.String())
						}
					}
				}
			}
		}
	}
	if receives == 0 || indexes == 0 {
		t.Fatalf("fixture did not compile the guarded shapes: receives=%d indexes=%d", receives, indexes)
	}
}

func TestSelectReceiveRepeatedSendStateRetainsEveryProducer(t *testing.T) {
	p := loadSelectReceive(t, "select_repeated_send.go", `
type repeatedSendWork struct { ctx context.Context }
var repeatedSendChannel = make(chan repeatedSendWork, 4)
func emitRepeatedSendWork() {
 ch := repeatedSendChannel
 ctx := context.WithValue(context.Background(), selectWorkKey{}, 1)
 select {
 case ch <- repeatedSendWork{ctx:ctx}:
 case ch <- repeatedSendWork{}:
 default:
 }
}
func receiveRepeatedSendWork() {
 stop := make(chan struct{})
 select {
 case work := <-repeatedSendChannel: consumeRepeatedSendWork(work)
 case <-stop:
 }
}
func consumeRepeatedSendWork(work repeatedSendWork) { _ = work.ctx.Err() }
`)
	projection := selectReceiveProjection(t, p, "repeatedSendWork", "consumeRepeatedSendWork")
	if len(projection.Witness) == 0 {
		t.Fatalf("second send on the same SSA channel operand disappeared: values=%v why=%v", projection.Values, projection.Why)
	}
}

// Private fields seal ordinary field nameability, not reflection/unsafe or
// retained closures. The complete carrier must stay in enumerated source.
func TestSelectReceivePrivateCarrierEscapesRemainUnknown(t *testing.T) {
	cases := []struct{ name, imports, source string }{
		{"recursive_carrier_interface", "", `type recursiveCarrier struct { next *recursiveCarrier; dispatcher *selectDispatcher };func escapeCarrier(){_ = interface{}(&recursiveCarrier{dispatcher:liveSelectDispatcher})}`},
		{"exported_parameter", "", `type ExportedHolder struct { events chan selectWork };func ReadCarrier(h *ExportedHolder){_ = h.events}`},
		{"unknown_factory", "", `var factory func() *selectDispatcher;func escapeCarrier(){d:=factory();_ = d.running}`},
		{"interface", "", `func escapeCarrier(){ _ = interface{}(liveSelectDispatcher) }`},
		{"foreign", `;"sync/atomic"`, `var foreignCarrier atomic.Pointer[selectDispatcher]
func escapeCarrier(){ foreignCarrier.Store(liveSelectDispatcher) }`},
		{"dynamic", "", `var opaqueCarrier func(*selectDispatcher)
func escapeCarrier(){ opaqueCarrier(liveSelectDispatcher) }`},
		{"unsafe", `;"unsafe"`, `func escapeCarrier(){ _ = unsafe.Pointer(liveSelectDispatcher) }`},
		{"return", "", `func escapeCarrier() *selectDispatcher {return liveSelectDispatcher}`},
		{"exported_global", "", `var ExportedCarrier = liveSelectDispatcher`},
		{"returned_closure", "", `func escapeCarrier() func(){ d:=liveSelectDispatcher; return func(){_ = d.running} }`},
		{"callback_closure", "", `var opaqueCallback func(func())
func escapeCarrier(){d:=liveSelectDispatcher;opaqueCallback(func(){_ = d.running})}`},
		{"reflection_close", `;"reflect";"unsafe"`, `func escapeCarrier(){field:=reflect.ValueOf(liveSelectDispatcher).Elem().FieldByName("events");reflect.NewAt(field.Type(),unsafe.Pointer(field.UnsafeAddr())).Elem().Close()}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := strings.Replace(selectReceiveBase, `import "context"`, `import ("context"`+tc.imports+`)`, 1) + tc.source
			p := loadEscapeProgram(t, "carrier_"+tc.name+".go", source)
			projection := selectReceiveProjection(t, p, "selectWork", "deliverSelectWork")
			if !projection.Unknown || len(projection.Witness) == 0 {
				t.Fatalf("escaped private channel carrier received a false closed-population proof: %+v", projection)
			}
		})
	}
}

func TestSelectReceivePrivateCarrierLocalCaptureRemainsProven(t *testing.T) {
	p := loadSelectReceive(t, "carrier_local.go", `func localCarrierUse(){d:=liveSelectDispatcher;func(){_ = d.running}();go func(){_ = d.running}()}`)
	projection := selectReceiveProjection(t, p, "selectWork", "deliverSelectWork")
	if projection.Unknown || len(projection.Witness) != 0 {
		t.Fatalf("enumerated local carrier capture lost its producer proof: %+v", projection)
	}
}

func TestChannelExposureCyclicContainmentKeepsReachableTarget(t *testing.T) {
	element := types.Typ[types.Int]
	a := types.NewNamed(types.NewTypeName(0, nil, "a", nil), nil, nil)
	b := types.NewNamed(types.NewTypeName(0, nil, "b", nil), nil, nil)
	a.SetUnderlying(types.NewStruct([]*types.Var{types.NewField(0, nil, "next", types.NewPointer(b), false), types.NewField(0, nil, "hidden", types.NewChan(types.SendRecv, element), false)}, nil))
	b.SetUnderlying(types.NewStruct([]*types.Var{types.NewField(0, nil, "back", types.NewPointer(a), false)}, nil))
	containment := &channelExposure{elem: element, privateFields: true, memo: map[types.Type]bool{}, active: map[types.Type]bool{}}
	for _, root := range []types.Type{a, b, a, b} {
		if !containment.exposes(root) {
			t.Fatalf("cycle lost reachable hidden channel from %v", root)
		}
	}
	names := &channelExposure{elem: element, memo: map[types.Type]bool{}, active: map[types.Type]bool{}}
	if names.exposes(a) || names.exposes(b) {
		t.Fatal("private containment became ordinary nameability")
	}
}
