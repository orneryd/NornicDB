// Author: Claude, NOR-180 single render pass, 2026-10-30.
package errors

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// statusScan is the one walk of an error tree. It finds the first terminal
// outcome carrier, depth first and through custom As; for rendering it also
// finds the localized primary (as errors.As finds it) and every status
// diagnostic in order. carrierOnly stops at discovery, for callers that need
// only the terminal outcome (lookupTerminalView, responseTerminalView).
// compose walks the whole forest instead and lists, in order, every carrier
// and every enclosing primary for collectTerminalView to merge.
type statusScan struct {
	carrierOnly bool
	compose     bool
	carrier     bool
	found       terminalOutcomeCarrier
	localized   *localization.LocalizedError
	diagnostics [4]statusDiagnostic
	overflow    []statusDiagnostic
	count       int
	items       []scanItem
}

// scanItem is one entry of a composed forest: a terminal outcome carrier, or
// a separate enclosing primary failure next to the carriers.
type scanItem struct {
	carrier terminalOutcomeCarrier
	primary error
}

// walk visits current depth first and reports whether its subtree holds a
// terminal outcome carrier (Unwrap []error and Unwrap error are followed; a
// custom As is consulted only on a node with no Unwrap).
func (s *statusScan) walk(current error) bool {
	if current == nil || s.carrier && !s.compose {
		return false
	}
	if carrier, ok := current.(terminalOutcomeCarrier); ok {
		s.foundCarrier(carrier)
		return true
	}
	if !s.carrierOnly && !s.compose && s.localized == nil {
		if localized, ok := current.(*localization.LocalizedError); ok {
			s.localized = localized
		} else if as, ok := current.(interface{ As(any) bool }); ok {
			var target *localization.LocalizedError
			if as.As(&target) {
				s.localized = target
			}
		}
	}
	if diagnostic, ok := current.(statusDiagnostic); ok && !s.carrierOnly && !s.compose {
		if s.count < len(s.diagnostics) {
			s.diagnostics[s.count] = diagnostic
		} else {
			s.overflow = append(s.overflow, diagnostic)
		}
		s.count++
	}
	switch wrapped := current.(type) {
	case interface{ Unwrap() []error }:
		held := false
		for _, child := range wrapped.Unwrap() {
			if s.walk(child) {
				held = true
			}
		}
		return held
	case interface{ Unwrap() error }:
		mark := len(s.items)
		if s.walk(wrapped.Unwrap()) {
			return true
		}
		// No carrier below: in a composed forest the whole wrapper is one
		// enclosing primary, unless its own As supplies a carrier.
		s.items = s.items[:mark]
		if s.compose {
			s.composeEnclosing(current)
		}
		return false
	case interface{ As(any) bool }:
		var carrier terminalOutcomeCarrier
		if wrapped.As(&carrier) && carrier != nil {
			s.foundCarrier(carrier)
			return true
		}
	}
	if s.compose {
		s.items = append(s.items, scanItem{primary: current})
	}
	return false
}

// foundCarrier records a carrier: the first one found is the outcome's
// carrier; a composed forest lists every one.
func (s *statusScan) foundCarrier(carrier terminalOutcomeCarrier) {
	if !s.carrier {
		s.carrier, s.found = true, carrier
	}
	if s.compose {
		s.items = append(s.items, scanItem{carrier: carrier})
	}
}

// composeEnclosing lists a single-wrapper with no carrier below it: its own
// As may still supply a carrier for composition (not for discovery);
// otherwise it is one enclosing primary.
func (s *statusScan) composeEnclosing(current error) {
	if wrapped, ok := current.(interface{ As(any) bool }); ok {
		var carrier terminalOutcomeCarrier
		if wrapped.As(&carrier) && carrier != nil {
			s.items = append(s.items, scanItem{carrier: carrier})
			return
		}
	}
	s.items = append(s.items, scanItem{primary: current})
}

// appendDiagnostics appends each diagnostic the message does not already
// contain, rendered with render when it is set.
func (s *statusScan) appendDiagnostics(message string, render func(localization.Message) string) string {
	for i := 0; i < s.count; i++ {
		var diagnostic statusDiagnostic
		if i < len(s.diagnostics) {
			diagnostic = s.diagnostics[i]
		} else {
			diagnostic = s.overflow[i-len(s.diagnostics)]
		}
		descriptor := diagnostic.StatusDiagnostic()
		text := descriptor.Fallback
		if render != nil {
			text = render(descriptor)
		}
		// Some presenters already kept the full joined message; append only
		// diagnostics missing from that message.
		if !strings.Contains(message, text) {
			message += "\n" + text
		}
	}
	return message
}

// terminalView is the response view of the terminal outcome the walk found in
// err: err's own view when err is the carrier, otherwise the composition of
// the whole forest (collectTerminalView). Nil when the walk found none.
func (s *statusScan) terminalView(err error) *terminalView {
	if !s.carrier {
		return nil
	}
	if carrier, ok := err.(terminalOutcomeCarrier); ok {
		return carrier.terminalOutcomeView()
	}
	view := collectTerminalView(err)
	return &view
}

// statusRendering is what a renderer asks of the one render pass: render
// localizes, classify supplies a protocol's own status for errors it knows,
// and diagnosticsOnto, when set, asks for that caller-supplied message with
// err's diagnostics (or err's terminal outcome) instead of a status.
type statusRendering struct {
	render          func(localization.Message) string
	classify        func(error) (string, string, bool)
	diagnosticsOnto *string
}

// renderStatus is the one render pass that turns an error into its status,
// message and diagnostics; every status renderer goes through it. It walks the
// error tree once. A terminal outcome renders from its frozen view. Otherwise
// classify, when set and ok, supplies a protocol's own status and message for
// err, which are kept as they are unless the localized primary has that
// status; else the primary is classified by neo4jStatusPlain. A localized
// primary with the status is rendered with render, and so are its diagnostics.
func renderStatus(err error, how statusRendering) (code, message string, classified bool) {
	var scan statusScan
	scan.walk(err)
	if scan.carrier {
		view := scan.terminalView(err)
		return view.code, renderTerminalView(view, how.render), true
	}
	if how.diagnosticsOnto != nil {
		return "", scan.appendDiagnostics(*how.diagnosticsOnto, how.render), false
	}
	own := false
	if how.classify != nil {
		code, message, own = how.classify(err)
		classified = own
	}
	if !own {
		code, message, classified = neo4jStatusPlain(err)
	}
	var diagnosticRender func(localization.Message) string
	if how.render != nil && scan.localized != nil && scan.localized.Code == code {
		message, diagnosticRender = how.render(scan.localized.Message), how.render
	} else if own {
		return code, message, classified
	}
	return code, scan.appendDiagnostics(message, diagnosticRender), classified
}
