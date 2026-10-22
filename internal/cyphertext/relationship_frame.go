package cyphertext

// ConnectionFrame owns the written relationship boundary. It is immutable;
// only its owner can construct a frame or turn it into a compiled type plan.
type ConnectionFrame struct {
	detailOpen, detailClose, end, outgoingAt int
	incoming, outgoing, bracketed, complete  bool
}

func (frame ConnectionFrame) DetailBounds() (int, int) { return frame.detailOpen, frame.detailClose }
func (frame ConnectionFrame) End() int                 { return frame.end }
func (frame ConnectionFrame) OutgoingAt() int          { return frame.outgoingAt }
func (frame ConnectionFrame) Incoming() bool           { return frame.incoming }
func (frame ConnectionFrame) Outgoing() bool           { return frame.outgoing }
func (frame ConnectionFrame) Bracketed() bool          { return frame.bracketed }
func (frame ConnectionFrame) Complete() bool           { return frame.complete }
func relationshipDelimiterEnd(text string, open int, opener, closer rune, bounds map[int]int) int {
	if bounds != nil {
		if end, ok := bounds[open]; ok {
			return end
		}
		return -1
	}
	return FindMatchingDelimiter(text, open, opener, closer)
}

func ReadConnectionFrame(text string, start int, bounds map[int]int) (frame ConnectionFrame, ok bool) {
	return readConnectionFrame(text, start, bounds, nil)
}

// A protocol can consume its admitted detail through canonical token phases
// as the frame is read, avoiding a delimiter walk followed by a token rewalk.
// The connection grammar and all resulting frame facts remain owned here.
func readConnectionFrame(text string, start int, bounds map[int]int, detail func(string, int) int) (frame ConnectionFrame, ok bool) {
	prefix, ok := ReadConnectionPrefix(text, start)
	if !ok {
		return prefix.frame, false
	}
	close := -1
	if open := prefix.DetailOpen(); open >= 0 {
		if detail != nil {
			close = detail(text, open)
		} else {
			close = relationshipDelimiterEnd(text, open, '[', ']', bounds)
		}
		if close < 0 {
			return prefix.frame, false
		}
	}
	return prefix.Complete(close)
}

// ConnectionPrefix is the connection owner's suspended structural phase.
// A lexical walk that is already visiting a detail carries this value until
// it encounters that detail's closing bracket, rather than reading ahead
// through the same quoted/nested source and then traversing it again.
type ConnectionPrefix struct {
	text     string
	frame    ConnectionFrame
	at       int
	leftDash bool
}

func (prefix ConnectionPrefix) DetailOpen() int { return prefix.frame.detailOpen }

func ReadConnectionPrefix(text string, start int) (prefix ConnectionPrefix, ok bool) {
	prefix.text = text
	prefix.frame.detailOpen, prefix.frame.detailClose, prefix.frame.outgoingAt = -1, -1, -1
	at := GapEnd(text, start)
	if at < len(text) && text[at] == '<' {
		prefix.frame.incoming = true
		at++
	}
	prefix.leftDash = at < len(text) && text[at] == '-'
	if prefix.frame.incoming && !prefix.leftDash {
		return prefix, false
	}
	if prefix.leftDash {
		at = GapEnd(text, at+1)
	}
	if at < len(text) && text[at] == '[' {
		prefix.frame.bracketed, prefix.frame.detailOpen = true, at
	} else if !prefix.leftDash {
		return prefix, false
	}
	prefix.at = at
	return prefix, true
}

// Complete consumes the closing position supplied by the ongoing lexical
// walk (or by ReadConnectionFrame's delimiter phase). Prefix and completion
// own the same arrow grammar for all consumers.
func (prefix ConnectionPrefix) Complete(detailClose int) (frame ConnectionFrame, ok bool) {
	frame, text, at := prefix.frame, prefix.text, prefix.at
	if frame.bracketed {
		if detailClose <= frame.detailOpen || detailClose >= len(text) || text[detailClose] != ']' {
			return frame, false
		}
		frame.detailClose = detailClose
		at = GapEnd(text, detailClose+1)
	}
	rightDash := at < len(text) && text[at] == '-'
	if rightDash {
		dash := at
		at = GapEnd(text, at+1)
		if at < len(text) && text[at] == '>' {
			frame.outgoing, frame.outgoingAt = true, dash
			at = GapEnd(text, at+1)
		}
	}
	frame.complete, frame.end = prefix.leftDash && rightDash, at
	return frame, true
}
