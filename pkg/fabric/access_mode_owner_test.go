package fabric

import "testing"

// Without an access-mode owner every statement counts as a write, so no
// write runs without its claim; a participant is named by its database,
// whichever name selected it.
func TestAccessModeWithoutOwnerCountsAsWrite(t *testing.T) {
	withAccessMode(t, nil)
	if !decideAccessMode("MATCH (n) RETURN n", false) || !fragmentMayWrite("RETURN 1") {
		t.Fatal("a statement without an owner must count as a write")
	}
	if got, want := LocalParticipant("one"), participantKeyFromLocation(&LocationLocal{DBName: "one"}); got != want {
		t.Fatalf("LocalParticipant: %q, want %q", got, want)
	}
	remote := &LocationRemote{URI: "bolt://far:7687", DBName: "far"}
	if got, want := ParticipantOf(remote), participantKeyFromLocation(remote); got != want {
		t.Fatalf("ParticipantOf: %q, want %q", got, want)
	}
}
